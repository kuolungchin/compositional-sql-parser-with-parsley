import parsley.expr.chain
import parsley.token.Lexer
import parsley.token.descriptions._
import parsley.{Failure, Parsley, Success}

object SimpleExprParser extends App {
  // Lexer setup
  private val lexicalDesc = LexicalDesc.plain.copy(
    symbolDesc = SymbolDesc.plain.copy(
      hardOperators = Set("+")
    )
  )
  private val lexer = new Lexer(lexicalDesc)

  // Parser for numbers
  private val NUMBER =  lexer.lexeme.signedCombined.number
    .map(either =>  either.fold(
      intValue => intValue.toInt,  // handle integer
      doubleValue => doubleValue.toInt // handle double
    ))

  private val PLUS = lexer.lexeme.symbol("+")

  val expr: Parsley[Int] =
    chain.left1(NUMBER, PLUS.map(_ => (l: Int, r: Int) => l + r))

  def parse(input: String): Either[String, Int] =
    expr.parse(input) match {
      case Success(result) => Right(result)
      case Failure(err) => Left(err)
    }

  // Test the parser
  val input = "1 + 2 + 3"
  SimpleExprParser.parse(input) match {
    case Right(num) => println(num)
    case Left(msg) => println(msg)
  }

  // When parsing "1 + 2 + 3":
  // Parse 1
  // Parse +, get function (l,r) => l + r
  // Parse 2
  // Apply function: 1 + 2 = 3
  // Parse +, get function (l,r) => l + r
  // Parse 3
  // Apply function: 3 + 3 = 6

  // Under the hood, there are left associative and curried
}

