import parsley.Parsley
import parsley.combinator.sepBy1
import parsley.expr.chain
import parsley.token.Lexer
import parsley.token.descriptions.text.{EscapeDesc, TextDesc}
import parsley.token.descriptions.{LexicalDesc, NameDesc, SpaceDesc, SymbolDesc}
import parsley.token.predicate.Basic

object SimpleSelectParser {
  // AST for projections
  sealed trait Projection
  case object StarProjection extends Projection
  case class ColumnProjection(columns: List[String]) extends Projection

  // AST for WHERE conditions
  sealed trait Condition
  case class Equals(column: String, value: String) extends Condition
  case class And(left: Condition, right: Condition) extends Condition
  case class Or(left: Condition, right: Condition) extends Condition
  case class Not(condition: Condition) extends Condition
  case class IsNull(column: String) extends Condition
  case class In(column: String, values: List[String]) extends Condition

  case class SelectStatement(projection: Projection, tableName: String, whereClause: Option[Condition])

  private val lexicalDesc = LexicalDesc.plain.copy(
    nameDesc = NameDesc.plain.copy(
      identifierStart = Basic(c => c.isLetter || c == '_'),
      identifierLetter = Basic(c => c.isLetterOrDigit || c == '_'),
    ),
    symbolDesc = SymbolDesc.plain.copy(
      hardKeywords = Set("SELECT", "FROM", "WHERE", "OR", "AND", "NOT", "IN", "IS", "NULL"),
      hardOperators = Set(",", "*", "=", "(", ")"),
      caseSensitive = false
    ),
    textDesc = TextDesc.plain.copy(
      escapeSequences = EscapeDesc.plain.copy(
        escBegin = '\\',
        literals = Set('\\', '\'', '\b', '\n', '\r', '\t', '\u0000')
      ),
      stringEnds = Set("'")
    ),
    spaceDesc = SpaceDesc.plain.copy(
      space = Basic(c => c.isWhitespace || c == '\n' || c == '\r'),
      commentLine = "--",
      commentStart = "/*",
      commentEnd = "*/"
    )
  )

  private val lexer = new Lexer(lexicalDesc)
  import lexer.lexeme.symbol.implicits.implicitSymbol

  private val IDENTIFIER = lexer.lexeme.names.identifier
  private val STRING = lexer.lexeme.string.fullUtf16  // For string literals in WHERE
  private val SELECT = lexer.lexeme.symbol("SELECT")
  private val FROM = lexer.lexeme.symbol("FROM")
  private val WHERE = lexer.lexeme.symbol("WHERE")
  private val STAR = lexer.lexeme.symbol("*")
  private val EQUALS = lexer.lexeme.symbol("=")
  private val AND: Parsley[Unit] = lexer.lexeme.symbol("AND")  // Parsley is a Monad, which has map, flatMap
  private val OR = lexer.lexeme.symbol("OR")
  private val NOT = lexer.lexeme.symbol("NOT")
  private val IN = lexer.lexeme.symbol("IN")
  private val IS = lexer.lexeme.symbol("IS")
  private val NULL = lexer.lexeme.symbol("NULL")
  private val LPAREN = lexer.lexeme.symbol("(")
  private val RPAREN = lexer.lexeme.symbol(")")

  // Parser for star projection
  private val starProjection: Parsley[Projection] =
    STAR.map(_ => StarProjection)

  // Parser for column list projection
  private val columnListProjection: Parsley[Projection] =
    sepBy1(IDENTIFIER, ",").map(ColumnProjection)

  // Combined projection parser
  private val projection: Parsley[Projection] =
    starProjection | columnListProjection

  // Parser for WHERE condition
  private val inList: Parsley[List[String]] =
    LPAREN *> sepBy1(STRING, ",") <* RPAREN

  private lazy val atom: Parsley[Condition] = {
    def equalsParser(col: String): Parsley[Condition] =
      EQUALS *> STRING.map(value => Equals(col, value))

    def isNullParser(col: String): Parsley[Condition] =
      IS *> NULL *> Parsley.pure(IsNull(col))

    def inParser(col: String): Parsley[Condition] =
      IN *> inList.map(values => In(col, values))

    def columnCondition: Parsley[Condition] =
      IDENTIFIER.flatMap(col =>
        equalsParser(col) | isNullParser(col) | inParser(col)
      )

    def parenthesizedCondition: Parsley[Condition] =
      LPAREN *> condition <* RPAREN

    columnCondition | parenthesizedCondition
  }

  private lazy val notFactor: Parsley[Condition] =
    (NOT *> atom).map(Not) | atom

  private lazy val andFactor: Parsley[Condition] =
    chain.left1(notFactor, AND.map(_ => (l: Condition, r: Condition) => And(l, r)))

  private lazy val condition: Parsley[Condition] =
    chain.left1(andFactor, OR.map(_ => (l: Condition, r: Condition) => Or(l, r)))

  // Parser for optional WHERE clause
  private val whereClause: Parsley[Option[Condition]] =
    (WHERE *> condition).map(Some(_)) | Parsley.pure(None)

  val selectStatement: Parsley[SelectStatement] = lexer.fully(
    for {
      _     <- SELECT
      proj  <- projection
      _     <- FROM
      table <- IDENTIFIER
      where <- whereClause
    } yield SelectStatement(proj, table, where)
  )

  def parse(input: String): Either[String, SelectStatement] =
    selectStatement.parse(input).toEither
}
