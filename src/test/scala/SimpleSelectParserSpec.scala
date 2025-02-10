import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SimpleSelectParserSpec extends AnyWordSpec with Matchers {
  import SimpleSelectParser._

  "SimpleSelectParser" should {
    "parse basic SELECT statement with named columns" in {
      val input = "SELECT id, name FROM users"
      val expected = SelectStatement(ColumnProjection(List("id", "name")), "users", None)
      SimpleSelectParser.parse(input) shouldBe Right(expected)
    }

    "parse SELECT statement with single column" in {
      val input = "SELECT id FROM products"
      val expected = SelectStatement(ColumnProjection(List("id")), "products", None)
      SimpleSelectParser.parse(input) shouldBe Right(expected)
    }

    "parse SELECT with star projection" in {
      val input = "SELECT * FROM users"
      val expected = SelectStatement(StarProjection, "users", None)
      SimpleSelectParser.parse(input) shouldBe Right(expected)
    }

    "parse SELECT with whitespace variations" in {
      val input = """
        SELECT  id,
                name,
                age
        FROM    users
      """
      val expected = SelectStatement(ColumnProjection(List("id", "name", "age")), "users", None)
      SimpleSelectParser.parse(input) shouldBe Right(expected)
    }

    "parse SELECT with comments" in {
      val input = """
        -- This is a comment
        SELECT id, /* inline comment */ name FROM users
        /* multi-line
           comment */
      """
      val expected = SelectStatement(ColumnProjection(List("id", "name")), "users", None)
      SimpleSelectParser.parse(input) shouldBe Right(expected)
    }

    "parse SELECT with case insensitive keywords" in {
      val inputs = List(
        "SELECT id FROM users",
        "select id from users",
        "Select id From Users"
      )
      val expected = SelectStatement(ColumnProjection(List("id")), "users", None)

      inputs.foreach { input =>
        SimpleSelectParser.parse(input) match {
          case Right(selectStmt) =>
            selectStmt match {
              case SelectStatement(ColumnProjection(cols), table, where) =>
                cols.map(_.toLowerCase) shouldBe expected.projection.asInstanceOf[ColumnProjection].columns
                table.toLowerCase shouldBe expected.tableName
                where shouldBe None
              case _ => fail("Unexpected projection type")
            }
          case Left(_) => fail("Failed to parse")
        }
      }
    }

    // New test cases for WHERE clause
    "parse SELECT with WHERE clause" in {
      val input = "SELECT id, name FROM users WHERE status = 'active'"
      val expected = SelectStatement(
        ColumnProjection(List("id", "name")),
        "users",
        Some(Equals("status", "active"))
      )
      SimpleSelectParser.parse(input) shouldBe Right(expected)
    }

    "parse SELECT * with WHERE clause" in {
      val input = "SELECT * FROM users WHERE status = 'active'"
      val expected = SelectStatement(
        StarProjection,
        "users",
        Some(Equals("status", "active"))
      )
      SimpleSelectParser.parse(input) shouldBe Right(expected)
    }

    "parse SELECT with WHERE clause and whitespace" in {
      val input = """
        SELECT id, name
        FROM users
        WHERE status = 'active'
      """
      val expected = SelectStatement(
        ColumnProjection(List("id", "name")),
        "users",
        Some(Equals("status", "active"))
      )
      SimpleSelectParser.parse(input) shouldBe Right(expected)
    }

    "parse SELECT with WHERE clause and comments" in {
      val input = """
        SELECT id, name FROM users
        -- Check only active users
        WHERE status = 'active' /* Active users only */
      """
      val expected = SelectStatement(
        ColumnProjection(List("id", "name")),
        "users",
        Some(Equals("status", "active"))
      )
      SimpleSelectParser.parse(input) shouldBe Right(expected)
    }

    "parse SELECT with case insensitive WHERE clause" in {
      val inputs = List(
        "SELECT id FROM users WHERE status = 'active'",
        "select id from users where status = 'active'",
        "Select id From Users WHERE status = 'active'"
      )
      val expected = SelectStatement(
        ColumnProjection(List("id")),
        "users",
        Some(Equals("status", "active"))
      )

      inputs.foreach { input =>
        SimpleSelectParser.parse(input) match {
          case Right(SelectStatement(ColumnProjection(cols), table, Some(Equals(col, value)))) =>
            cols.map(_.toLowerCase) shouldBe expected.projection.asInstanceOf[ColumnProjection].columns
            table.toLowerCase shouldBe expected.tableName
            col shouldBe "status"
            value shouldBe "active"
          case _ => fail("Failed to parse or unexpected structure")
        }
      }
    }

    "parse complex WHERE conditions" when {
      "using AND" in {
        val input = "SELECT id FROM users WHERE status = 'active' AND role = 'admin'"
        val expected = SelectStatement(
          ColumnProjection(List("id")),
          "users",
          Some(And(
            Equals("status", "active"),
            Equals("role", "admin")
          ))
        )
        SimpleSelectParser.parse(input) shouldBe Right(expected)
      }

      "using OR" in {
        val input = "SELECT id FROM users WHERE status = 'active' OR status = 'pending'"
        val expected = SelectStatement(
          ColumnProjection(List("id")),
          "users",
          Some(Or(
            Equals("status", "active"),
            Equals("status", "pending")
          ))
        )
        SimpleSelectParser.parse(input) shouldBe Right(expected)
      }

      "using NOT" in {
        val input = "SELECT id FROM users WHERE NOT status = 'inactive'"
        val expected = SelectStatement(
          ColumnProjection(List("id")),
          "users",
          Some(Not(Equals("status", "inactive")))
        )
        SimpleSelectParser.parse(input) shouldBe Right(expected)
      }

      "using IS NULL" in {
        val input = "SELECT id FROM users WHERE last_login IS NULL"
        val expected = SelectStatement(
          ColumnProjection(List("id")),
          "users",
          Some(IsNull("last_login"))
        )
        SimpleSelectParser.parse(input) shouldBe Right(expected)
      }

      "using IN clause" in {
        val input = "SELECT id FROM users WHERE role IN ('admin', 'moderator')"
        val expected = SelectStatement(
          ColumnProjection(List("id")),
          "users",
          Some(In("role", List("admin", "moderator")))
        )
        SimpleSelectParser.parse(input) shouldBe Right(expected)
      }

      "using parentheses for grouping" in {
        val input = "SELECT id FROM users WHERE (status = 'active' AND role = 'admin') OR role = 'superadmin'"
        val expected = SelectStatement(
          ColumnProjection(List("id")),
          "users",
          Some(Or(
            And(
              Equals("status", "active"),
              Equals("role", "admin")
            ),
            Equals("role", "superadmin")
          ))
        )
        SimpleSelectParser.parse(input) shouldBe Right(expected)
      }

    }

    "fail for invalid syntax" when {
      "missing columns" in {
        val input = "SELECT FROM users"
        SimpleSelectParser.parse(input) shouldBe a[Left[_, _]]
      }

      "missing table name" in {
        val input = "SELECT id FROM"
        SimpleSelectParser.parse(input) shouldBe a[Left[_, _]]
      }

      "missing FROM clause" in {
        val input = "SELECT id"
        SimpleSelectParser.parse(input) shouldBe a[Left[_, _]]
      }

      "invalid column separator" in {
        val input = "SELECT id; name FROM users"
        SimpleSelectParser.parse(input) shouldBe a[Left[_, _]]
      }

      "incomplete WHERE clause" in {
        val input = "SELECT id FROM users WHERE"
        SimpleSelectParser.parse(input) shouldBe a[Left[_, _]]
      }

      "missing value in WHERE clause" in {
        val input = "SELECT id FROM users WHERE status ="
        SimpleSelectParser.parse(input) shouldBe a[Left[_, _]]
      }

      "missing equals in WHERE clause" in {
        val input = "SELECT id FROM users WHERE status 'active'"
        SimpleSelectParser.parse(input) shouldBe a[Left[_, _]]
      }

      "unclosed string in WHERE clause" in {
        val input = "SELECT id FROM users WHERE status = 'active"
        SimpleSelectParser.parse(input) shouldBe a[Left[_, _]]
      }
    }
  }
}