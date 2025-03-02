import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SqlParserSpec extends AnyWordSpec with Matchers {
  import SqlParser._

  "SqlParser" should {

    def verifyResult[E, R](result: Either[E, R]): Unit =
      result match {
        case Right(_) => succeed
        case Left(errStr) =>
          System.err.println(errStr)
          fail()
    }

    "parse basic SELECT statements" when {
      "using named columns" in {
        val input = "SELECT id, name FROM users"
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.projection match {
          case ExpressionProjection(exprs) =>
            exprs.map(_._1) should contain theSameElementsInOrderAs List(
              ColumnRef("id"),
              ColumnRef("name")
            )
          case _ => fail("Expected ExpressionProjection")
        }
        stmt.from shouldBe TableName("users")
        stmt.where shouldBe None
      }

      "using star projection" in {
        val input = "SELECT * FROM users"
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.projection shouldBe StarProjection
        stmt.from shouldBe TableName("users")
        stmt.where shouldBe None
      }

      "using whitespace variations" in {
        val input = """SELECT  id,
                              name,
                              age
                       FROM   users
                    """
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.projection match {
          case ExpressionProjection(exprs) =>
            exprs.map(_._1) should contain theSameElementsInOrderAs List(
              ColumnRef("id"),
              ColumnRef("name"),
              ColumnRef("age")
            )
          case _ => fail("Expected ExpressionProjection")
        }
      }

      "using case insensitive keywords" in {
        val inputs = List(
          "SELECT id FROM users",
          "select id from users",
          "Select id From Users"
        )
        val expected = ColumnRef("id")

        inputs.foreach { input =>
          val result = SqlParser.parse(input)
          verifyResult(result)

          val stmt = result.right.get
          stmt.projection match {
            case ExpressionProjection(exprs) =>
              exprs.head._1 shouldBe expected
            case _ => fail("Expected ExpressionProjection")
          }
        }
      }
    }

    "parse column aliases" when {
      "using AS keyword" in {
        val input = "SELECT id AS user_id, name AS user_name FROM users"
        val result = SqlParser.parse(input)

        verifyResult(result)
        val stmt = result.right.get
        stmt.projection match {
          case ExpressionProjection(exprs) =>
            exprs should contain theSameElementsInOrderAs List(
              (ColumnRef("id"), Some("user_id")),
              (ColumnRef("name"), Some("user_name"))
            )
          case _ => fail("Expected ExpressionProjection")
        }
      }
    }

    "parse WHERE conditions" when {
      "using simple conditions" in {
        val input = "SELECT * FROM users WHERE age > 18"
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.where shouldBe Some(
          GreaterThan(
            ColumnRef("age"),
            NumberLit(18)
          )
        )
      }

      "using AND conditions" in {
        val input = "SELECT * FROM users WHERE status = 'active' AND age > 18"
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.where shouldBe Some(
          And(
            Equals(ColumnRef("status"), StringLit("active")),
            GreaterThan(ColumnRef("age"), NumberLit(18))
          )
        )
      }

      "using OR conditions" in {
        val input = "SELECT * FROM users WHERE status = 'active' OR status = 'pending'"
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.where shouldBe Some(
          Or(
            Equals(ColumnRef("status"), StringLit("active")),
            Equals(ColumnRef("status"), StringLit("pending"))
          )
        )
      }

      "using NOT conditions" in {
        val input = "SELECT * FROM users WHERE NOT status = 'inactive'"
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.where shouldBe Some(
          Not(Equals(ColumnRef("status"), StringLit("inactive")))
        )
      }

      "using complex nested conditions" in {
        val input = "Select * FROM users WHERE ( status = 'active' AND age > 18) OR role = 'admin'"
        val result = SqlParser.parse(input)
        verifyResult(result)

        result.isRight shouldBe true
        val stmt = result.right.get
        stmt.where shouldBe Some(
          Or(
            And(
              Equals(ColumnRef("status"), StringLit("active")),
              GreaterThan(ColumnRef("age"), NumberLit(18))
            ),
            Equals(ColumnRef("role"), StringLit("admin"))
          )
        )
      }
    }

    "parse WITH (CTE) statements" when {
      "using single CTE" in {
        val input =
          """WITH temp AS (SELECT * FROM users)
            |SELECT * FROM temp
            |""".stripMargin
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.withClause shouldBe defined
        val withClause = stmt.withClause.get
        withClause.ctes should have length 1

        val cte = withClause.ctes.head
        cte.name shouldBe "temp"
        cte.query.projection shouldBe StarProjection
        cte.query.from shouldBe TableName("users")
      }

      "using multiple CTEs" in {
        val input = """WITH active_users AS (SELECT * FROM users WHERE status = 'active'),
                            premium_users AS (SELECT * FROM active_users WHERE subscription = 'premium')
                       SELECT * FROM premium_users
                    """
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.withClause shouldBe defined
        val withClause = stmt.withClause.get
        withClause.ctes should have length 2

        val Seq(activeUsers, premiumUsers) = withClause.ctes

        activeUsers.name shouldBe "active_users"
        activeUsers.query.where shouldBe Some(
          Equals(ColumnRef("status"), StringLit("active"))
        )

        premiumUsers.name shouldBe "premium_users"
        premiumUsers.query.where shouldBe Some(
          Equals(ColumnRef("subscription"), StringLit("premium"))
        )
      }
    }

    "parse special conditions" when {
      "using IS NULL" in {
        val input = "SELECT * FROM users WHERE last_login IS NULL"
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.where shouldBe Some(IsNull(ColumnRef("last_login")))
      }

      "using REGEX match" in {
        val input = "SELECT * FROM users WHERE name REGEX '^[A-Z].*$'"
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.where shouldBe Some(
          RegexMatch(
            ColumnRef("name"),
            StringLit("^[A-Z].*$")
          )
        )
      }

      "using IN with list" in {
        val input = "SELECT * FROM users WHERE status IN ('active', 'pending')"
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.where match {
          case Some(In(ColumnRef("status"), Left(values))) =>
            values should contain theSameElementsInOrderAs List(
              StringLit("active"),
              StringLit("pending")
            )
          case _ => fail("Expected IN condition with list")
        }
      }

      "using IN with subquery" in {
        val input = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.where match {
          case Some(In(ColumnRef("id"), Right(SubqueryExpr(subquery)))) =>
            subquery.from shouldBe TableName("orders")
          case _ => fail("Expected IN condition with subquery")
        }
      }
    }

    "parse GROUP BY statements" when {
      "using a single column" in {
        val input = "SELECT id, count(*) FROM users Group By id"
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.groupBy shouldBe defined
        val groupByCols = stmt.groupBy.get
        groupByCols should have size 1
        groupByCols.head shouldBe ColumnRef("id")
      }

      "using multiple columns" in {
        val input = "SELECT department, role, avg(salary) FROM employees GROUP BY department, role"
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.groupBy shouldBe defined
        val groupByCols = stmt.groupBy.get
        groupByCols should have size 2
        groupByCols(0) shouldBe ColumnRef("department")
        groupByCols(1) shouldBe ColumnRef("role")
      }

      "with case-insensitive GROUP BY keywords" in {
        val input = "SELECT department, count(*) FROM employees group by department"
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.groupBy shouldBe defined
        val groupByCols = stmt.groupBy.get
        groupByCols should have size 1
        groupByCols.head shouldBe ColumnRef("department")
      }

      "with whitespace variations" in {
        val input = """SELECT department, count(*)
                      |FROM   employees
                      |GROUP  BY    department""".stripMargin
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.groupBy shouldBe defined
        val groupByCols = stmt.groupBy.get
        groupByCols should have size 1
        groupByCols.head shouldBe ColumnRef("department")
      }

      "with expressions in GROUP BY" in {
        val input = "SELECT year, sum(revenue) FROM sales GROUP BY year + 1"
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.groupBy shouldBe defined
        val groupByCols = stmt.groupBy.get
        groupByCols should have size 1
        groupByCols.head shouldBe a[Add]
        val addExpr = groupByCols.head.asInstanceOf[Add]
        addExpr.left shouldBe ColumnRef("year")
        addExpr.right shouldBe NumberLit(1)
      }

      "with WHERE clause and GROUP BY" in {
        val input = "SELECT region, SUM(sales) FROM transactions WHERE year = 2023 GROUP BY region"
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.where shouldBe defined
        stmt.where.get shouldBe Equals(ColumnRef("year"), NumberLit(2023))

        stmt.groupBy shouldBe defined
        val groupByCols = stmt.groupBy.get
        groupByCols should have size 1
        groupByCols.head shouldBe ColumnRef("region")
      }

      "with GROUP BY in subquery" in {
        val input = """WITH dept_stats AS (
                      |  SELECT department, COUNT(*) as employee_count
                      |  FROM employees
                      |  GROUP BY department
                      |)
                      |SELECT * FROM dept_stats""".stripMargin
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.withClause shouldBe defined
        val cte = stmt.withClause.get.ctes.head

        cte.query.groupBy shouldBe defined
        val groupByCols = cte.query.groupBy.get
        groupByCols should have size 1
        groupByCols.head shouldBe ColumnRef("department")
      }

      "with no GROUP BY clause" in {
        val input = "SELECT id, name FROM users"
        val result = SqlParser.parse(input)
        verifyResult(result)

        val stmt = result.right.get
        stmt.groupBy shouldBe None
      }

      "failing with incomplete GROUP BY" in {
        val input = "SELECT dept, COUNT(*) FROM employees GROUP BY"
        SqlParser.parse(input) shouldBe a[Left[_, _]]
      }
    }

    "fail to parse" when {
      "missing columns" in {
        val input = "SELECT FROM users"
        SqlParser.parse(input) shouldBe a[Left[_, _]]
      }

      "missing FROM clause" in {
        val input = "SELECT id"
        SqlParser.parse(input) shouldBe a[Left[_, _]]
      }

      "missing table name" in {
        val input = "SELECT id FROM"
        SqlParser.parse(input) shouldBe a[Left[_, _]]
      }

      "missing WHERE condition" in {
        val input = "SELECT id FROM users WHERE"
        SqlParser.parse(input) shouldBe a[Left[_, _]]
      }

      "incomplete WITH clause" in {
        val input = "WITH temp AS SELECT * FROM users"
        SqlParser.parse(input) shouldBe a[Left[_, _]]
      }

      "unterminated string literal" in {
        val input = "SELECT * FROM users WHERE name = 'unterminated"
        SqlParser.parse(input) shouldBe a[Left[_, _]]
      }
    }
  }
}