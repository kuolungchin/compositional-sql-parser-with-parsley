import parsley.Parsley.{atomic, many, pure, some}
import parsley.character.{char, digit, letter, string}
import parsley.combinator.{option, sepBy1}
import parsley.{Failure, Parsley, Result, Success}


object ParsleyOperatorPlayground extends App {
  def showResult[E, R](message: String, result: Result[E, R]) = result match {
    case Success(value) => println(s"Successfully parsing ${message}: ${value}")
    case Failure(_) => println(s"Example ***${message}*** failed")
  }

  // NOTE: Using ~> and <~ for keeping one side
  def quotedString: Parsley[String] = {
    val quote = char('"')
    val content = many(letter | digit | char(' ')).map(_.mkString)
    atomic(quote ~> content <~ quote) // Parse quote, then content, then quote, but only keep content
  }
  val result1 = quotedString.parse("\"hello 99 world\"")
  showResult("quoted string", result1)

  // NOTE: Using <~> for keeping both parts
  def numberPair: Parsley[(Int, Int)] = {
    val number = some(digit).map(_.mkString.toInt)
    number <~> (char(',') ~> number)  // Parse number, comma, number and keep both numbers as tuple
                                      //  see: def <~>[B](q: =>Parsley[B]): Parsley[(A, B)]
                                      // If both sides <~> of are successful then tuple: (x, y)
  }
  val result2 = numberPair.parse("123,456")
  showResult("number pair", result2)

  // NOTE: Using <*> for function application
  case class Person(name: String, age: Int)
  def person: Parsley[Person] = {
    val name = string("name:") ~> quotedString
    val age = string(",age:") ~> option(digit).map(_.mkString.toInt)
    pure(Person.curried) <*> name <*> age  // Using <*> with pure to lift the function into Parsley context
  }
  val result3 = person.parse("""name:"John",age:30""")
  showResult("Person", result3)


  // NOTE: Using </> for default values
  def optionalAge: Parsley[Int] = {
    some(digit).map(_.mkString.toInt) </> 0  // Using: Changed many to some for non-empty sequence
  }
  showResult("Age \"\" as 0", optionalAge.parse(""))
  showResult("Age 25", optionalAge.parse("25"))

  // NOTE: Using | for alternatives
  def command: Parsley[String] = {
    string("help") | string("exit") | string("version")
  }
  showResult("Command alternatives - help", command.parse("help"))
  showResult("Command alternatives - version", command.parse("version"))
  showResult("Command alternatives - exit", command.parse("exit"))
  showResult("Command alternatives - invalid", command.parse("invalid"))

  // NOTE: Using | for alternatives
  def listParser: Parsley[List[String]] = {
    val item = char('"') ~> many(letter | digit | char(' ')).map(_.mkString) <~ char('"')
    char('[') ~> sepBy1(item, string(", ")) <~ char(']')
  }
  showResult("List", listParser.parse("""["apple", "banana", "cherry"]"""))
}