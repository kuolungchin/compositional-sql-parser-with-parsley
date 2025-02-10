import parsley.Parsley
import parsley.combinator.{option, sepBy1}
import parsley.expr.chain
import parsley.token.Lexer
import parsley.token.descriptions.text.{EscapeDesc, TextDesc}
import parsley.token.descriptions.{LexicalDesc, NameDesc, SpaceDesc, SymbolDesc}
import parsley.token.predicate.Basic

object SqlParser {
  // AST for expressions
  sealed trait Expression
  case class ColumnRef(name: String) extends Expression
  case class NumberLit(value: Double) extends Expression
  case class StringLit(value: String) extends Expression
  case class SubqueryExpr(select: SelectStatement) extends Expression

  sealed trait BinaryOp extends Expression {
    def left: Expression
    def right: Expression
  }

  // Arithmetic
  case class Add(left: Expression, right: Expression) extends BinaryOp
  case class Subtract(left: Expression, right: Expression) extends BinaryOp
  case class Multiply(left: Expression, right: Expression) extends BinaryOp
  case class Divide(left: Expression, right: Expression) extends BinaryOp

  // Comparisons
  case class Equals(left: Expression, right: Expression) extends BinaryOp
  case class NotEquals(left: Expression, right: Expression) extends BinaryOp
  case class GreaterThan(left: Expression, right: Expression) extends BinaryOp
  case class GreaterThanEquals(left: Expression, right: Expression) extends BinaryOp
  case class LessThan(left: Expression, right: Expression) extends BinaryOp
  case class LessThanEquals(left: Expression, right: Expression) extends BinaryOp

  // Logical
  case class And(left: Expression, right: Expression) extends BinaryOp
  case class Or(left: Expression, right: Expression) extends BinaryOp
  case class Not(expr: Expression) extends Expression

  // Special conditions
  case class IsNull(expr: Expression) extends Expression
  case class IsTrue(expr: Expression) extends Expression
  case class IsFalse(expr: Expression) extends Expression
  case class RegexMatch(expr: Expression, pattern: StringLit) extends Expression
  case class In(expr: Expression, values: Either[List[Expression], SubqueryExpr]) extends Expression

  // CTE support
  case class CTE(name: String, query: SelectStatement)
  case class WithClause(ctes: List[CTE])

  // Extended table references for FROM clause
  sealed trait TableRef
  case class TableName(name: String) extends TableRef
  case class SubqueryTable(select: SelectStatement, alias: Option[String]) extends TableRef

  // Main AST nodes
  sealed trait Projection
  case object StarProjection extends Projection
  case class ExpressionProjection(expressions: List[(Expression, Option[String])]) extends Projection

  case class SelectStatement(
                              withClause: Option[WithClause],
                              projection: Projection,
                              from: TableRef,
                              where: Option[Expression]
                            )

  private val lexicalDesc = LexicalDesc.plain.copy(
    nameDesc = NameDesc.plain.copy(
      identifierStart = Basic(c => c.isLetter || c == '$' || c == '_'),
      identifierLetter = Basic(c => c.isLetterOrDigit || c == '$' || c == '_' || c == '.')),
    symbolDesc = SymbolDesc.plain.copy(
      hardKeywords =
        Set("WITH", "SELECT", "FROM", "AS", "WHERE", "GROUP BY", "HAVING") ++
          Set("CROSS", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "JOIN", "ON", "UNION") ++
          Set("OR", "AND", "NOT", "IN", "IS", "REGEX", "NULL") ++
          Set("TRUE", "FALSE"),
      hardOperators = Set("=", "<>", ">", ">=", "<", "<=", "*", "/", "+", "-"),
      caseSensitive = false),
    textDesc = TextDesc.plain.copy(
      escapeSequences = EscapeDesc.plain.copy(
        escBegin = '\\',
        literals = Set('\\', '\'', '\b', '\n', '\r', '\t', '\u0000')),
      stringEnds = Set("'")),
    spaceDesc = SpaceDesc.plain.copy(
      space = Basic(c => c.isWhitespace || c == '\n' || c == '\r'),
      commentLine = "--",
      commentStart = "/*",
      commentEnd = "*/")
  )

  private val lexer = new Lexer(lexicalDesc)

  private val IDENTIFIER = lexer.lexeme.names.identifier
  private val NUMBER = lexer.lexeme.signedCombined.number
  private val STRING = lexer.lexeme.string.fullUtf16

  // Keywords
  private val WITH = lexer.lexeme.symbol("WITH")
  private val SELECT = lexer.lexeme.symbol("SELECT")
  private val FROM = lexer.lexeme.symbol("FROM")
  private val WHERE = lexer.lexeme.symbol("WHERE")
  private val AS = lexer.lexeme.symbol("AS")
  private val AND = lexer.lexeme.symbol("AND")
  private val OR = lexer.lexeme.symbol("OR")
  private val NOT = lexer.lexeme.symbol("NOT")
  private val IN = lexer.lexeme.symbol("IN")
  private val IS = lexer.lexeme.symbol("IS")
  private val NULL = lexer.lexeme.symbol("NULL")
  private val TRUE = lexer.lexeme.symbol("TRUE")
  private val FALSE = lexer.lexeme.symbol("FALSE")
  private val REGEX = lexer.lexeme.symbol("REGEX")

  // Operators
  private val EQUALS = lexer.lexeme.symbol("=")
  private val NOT_EQUALS = lexer.lexeme.symbol("<>")
  private val GREATER_THAN = lexer.lexeme.symbol(">")
  private val GREATER_EQUALS = lexer.lexeme.symbol(">=")
  private val LESS_THAN = lexer.lexeme.symbol("<")
  private val LESS_EQUALS = lexer.lexeme.symbol("<=")
  private val PLUS = lexer.lexeme.symbol("+")
  private val MINUS = lexer.lexeme.symbol("-")
  private val MULTIPLY = lexer.lexeme.symbol("*")
  private val DIVIDE = lexer.lexeme.symbol("/")
  private val LPAREN = lexer.lexeme.symbol("(")
  private val RPAREN = lexer.lexeme.symbol(")")
  private val COMMA = lexer.lexeme.symbol(",")

  // Base parsers
  private lazy val numberLit: Parsley[Expression] =
    NUMBER.map(n => NumberLit(n.fold(_.toDouble, d => d.toDouble)))

  private lazy val stringLit: Parsley[Expression] =
    STRING.map(StringLit)

  private lazy val columnRef: Parsley[Expression] =
    IDENTIFIER.map(ColumnRef)

  // Expression parsers with precedence hierarchy
  private lazy val expr: Parsley[Expression] = orExpr

  private lazy val orExpr: Parsley[Expression] =
    chain.left1(andExpr, OR.map(_ => (l: Expression, r: Expression) => Or(l, r)))

  private lazy val andExpr: Parsley[Expression] =
    chain.left1(notExpr, AND.map(_ => (l: Expression, r: Expression) => And(l, r)))

  private lazy val notExpr: Parsley[Expression] =
    (NOT *> inExpr).map(Not) | inExpr

  private lazy val inExpr: Parsley[Expression] = {
    def inList: Parsley[Either[List[Expression], SubqueryExpr]] =
      LPAREN *> (selectStmt.map(stmt => Right(SubqueryExpr(stmt))) |
          sepBy1(expr, COMMA).map(Left(_))) <* RPAREN

    isTrueExpr.flatMap { expr =>
      (IN *> inList.map(values => In(expr, values))) | Parsley.pure(expr)
    }
  }

  private lazy val isTrueExpr: Parsley[Expression] =
    isNullExpr.flatMap { expr =>
      (IS *> ((TRUE.map(_ => IsTrue(expr))) |
        (FALSE.map(_ => IsFalse(expr))))) |
        Parsley.pure(expr)
    }

  private lazy val isNullExpr: Parsley[Expression] =
    regexExpr.flatMap { expr =>
      (IS *> NULL).map(_ => IsNull(expr)) | Parsley.pure(expr)
    }

  private lazy val regexExpr: Parsley[Expression] =
    comparisonExpr.flatMap { expr =>
      (REGEX *> stringLit).map {
        case StringLit(pattern) => RegexMatch(expr, StringLit(pattern))
        case _ => throw new IllegalStateException("Expected string literal")
      } | Parsley.pure(expr)
    }

  private lazy val comparisonExpr: Parsley[Expression] =
    chain.left1(
      addExpr,
      EQUALS.map(_ => (l: Expression, r: Expression) => Equals(l, r)) |
        NOT_EQUALS.map(_ => (l: Expression, r: Expression) => NotEquals(l, r)) |
        GREATER_THAN.map(_ => (l: Expression, r: Expression) => GreaterThan(l, r)) |
        GREATER_EQUALS.map(_ => (l: Expression, r: Expression) => GreaterThanEquals(l, r)) |
        LESS_THAN.map(_ => (l: Expression, r: Expression) => LessThan(l, r)) |
        LESS_EQUALS.map(_ => (l: Expression, r: Expression) => LessThanEquals(l, r))
    )

  private lazy val addExpr: Parsley[Expression] =
    chain.left1(
      mulExpr,
      PLUS.map(_ => (l: Expression, r: Expression) => Add(l, r)) |
        MINUS.map(_ => (l: Expression, r: Expression) => Subtract(l, r))
    )

  private lazy val mulExpr: Parsley[Expression] =
    chain.left1(
      atomExpr,
      MULTIPLY.map(_ => (l: Expression, r: Expression) => Multiply(l, r)) |
        DIVIDE.map(_ => (l: Expression, r: Expression) => Divide(l, r))
    )

  //  private lazy val atomExpr: Parsley[Expression] =  //  WRONG.
  //    numberLit | stringLit | subqueryExpr | columnRef |
  //      LPAREN *> expr <* RPAREN
  private lazy val atomExpr: Parsley[Expression] =  // FIXED
    numberLit | stringLit | columnRef | LPAREN *> expr <* RPAREN | subqueryExpr

  private lazy val subqueryExpr: Parsley[Expression] =
    (LPAREN *> selectStmt <* RPAREN).map(SubqueryExpr)

  //CTE parser
  //  WITH
  //  t1 AS (SELECT * FROM table1),
  //  t2 AS (SELECT * FROM table2),
  //  t3 AS (SELECT * FROM t1 JOIN t2)
  //  SELECT * FROM t3
  private lazy val cte: Parsley[CTE] = for {
    name <- IDENTIFIER
    _ <- AS
    _ <- LPAREN
    query <- selectStmt
    _ <- RPAREN
  } yield CTE(name, query)

  private lazy val withClause: Parsley[WithClause] =  for {
    _ <- WITH
    ctes <-  sepBy1(cte, COMMA)
  } yield WithClause(ctes)

  // Projection parsers
  private lazy val projection: Parsley[Projection] = {
    val star = MULTIPLY.map(_ => StarProjection)

    val exprAlias = expr.flatMap { e =>
      (AS *> IDENTIFIER).map(alias => (e, Some(alias)))
        .|(Parsley.pure((e, None)))
    }

    val exprList = sepBy1(exprAlias, COMMA).map(ExpressionProjection)

    star | exprList
  }

  // Table reference parsers
  private lazy val tableRef: Parsley[TableRef] = {
    val simpleTable = IDENTIFIER.map(TableName)

    val subquery = for {
      select <- LPAREN *> selectStmt <* RPAREN
      alias <- option(AS *> IDENTIFIER)
    } yield SubqueryTable(select, alias)

    subquery | simpleTable
  }


  private lazy val selectStmt: Parsley[SelectStatement] = for {
    withOpt <- option(withClause)
    _       <- SELECT
    proj    <- projection
    _       <- FROM
    table   <- tableRef
    where   <- option(WHERE *> expr)
  } yield SelectStatement(withOpt, proj, table, where)

  def parse(input: String): Either[String, SelectStatement] =
    selectStmt.parse(input).toEither
}