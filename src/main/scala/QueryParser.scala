import org.apache.arrow.dataset.source.Dataset
import scala.util.parsing.combinator.RegexParsers
/*
object QueryParser extends RegexParsers {
  def apply(input: String)(using loaded_datasets : Map[String, Dataset]): Atom = parseAll(relation, input) match {
    case Success(result, _) => result
    case failure: NoSuccess => scala.sys.error(failure.msg)
  }

  private def body: Parser[List[Term]] = rep1sep(wordConstant | floatConstant | numberConstant | variable, ", ") ^^ (c => c)
  private def wordConstant: Parser[Term]   = """[a-z][a-zA-Z]*""".r ^^ (c => Constant[String](c))
  private def floatConstant: Parser[Term] = """-?\d+\.\d*[1-9]|\.\d*[1-9]|\d+\.\d*[1-9]""".r ^^ (c => Constant[Float](c.toFloat))
  private def numberConstant: Parser[Term] = """(0|[1-9]\d*)""".r ^^ (c => Constant[Int](c.toInt))
  private def variable: Parser[Term] = """[A-Z][a-zA-Z]*""".r ^^ (c => Variable(c))
  private def relation(using loaded_datasets : Map[String, Dataset]): Parser[Atom] =
    ("""[a-z][a-zA-Z]+""".r ~ "(" ~ body ~ ")") ^^ {
      case head_name ~ sl ~ b ~ sr =>
        loaded_datasets.get(head_name) match
          case Some(ds) => new Atom(b, ds)
          case None => throw new Exception(f"dataset \"$head_name\" not found")
    }
}*/
import scala.util.parsing.combinator.*

class QueryParser(loaded_datasets : Map[String, Dataset]) extends RegexParsers {
  private def atomParser: Parser[Atom] = relationNameParser ~ "(" ~ termParser ~ ")" ^^ {
    case head_name ~ _ ~ b ~ _ =>
      new Atom(head_name, b, loaded_datasets.get(head_name))
  }
  private def queryParser: Parser[ConjunctiveQuery] = headParser ~ bodyParser ^^ {
    case head ~ body => ConjunctiveQuery(head, body)
  }
  private def relationNameParser: Parser[String] = """[a-zA-Z]+""".r
  private def termParser: Parser[List[Term]] = rep1sep( parseTermFloat | parseTermInt | parseTermString | parseVariable, ",")
  private def parseTermInt: Parser[Term] = """0|(-?[1-9]\d*)\b""".r ^^ (c => Constant[Int](c.toInt))
  private def parseTermString: Parser[Term] = parseTermStringUnquoted | parseTermStringQuotedSingle | parseTermStringQuotedDouble
  private def parseTermStringUnquoted: Parser[Term] = """[a-z]\S*\b""".r ^^ (c => Constant[String](c))
  private def parseTermStringQuotedSingle: Parser[Term] = "'" ~> """[a-z][^\n\t']*\b""".r <~ "'"  ^^ (c => Constant[String](c))
  private def parseTermStringQuotedDouble: Parser[Term] = '"' ~> """[a-z][^\n\t"]*\b""".r <~ '"'  ^^ (c => Constant[String](c))
  private def parseTermFloat: Parser[Term] = """-?\d+\.\d*\b""".r ^^ (c => Constant[Float](c.toFloat))
  private def parseVariable: Parser[Variable] = """[A-Z][a-zA-Z]*""".r ^^ (c => Variable(c))
  private def headParser: Parser[Head] = atomParser <~ ":-" ^^ (atom => new Head(atom.relationName, atom.terms))
  private def bodyParser: Parser[Set[Atom]] = rep1sep(atomParser, ",") <~ "." ^^ (atoms => atoms.toSet)

  def apply(input: String): ConjunctiveQuery = {
    parseAll(queryParser, input) match {
      case Success(matched, _) => matched
      case Failure(msg, _) => throw new ArithmeticException(msg)
      case Error(msg, _) => throw new ArithmeticException(msg)
    }
  }

  def test(): Unit = {
    val input = "Answer(uu) :- Beers(Orval, 1.4, y), Location(1, 'h#i--er', 'hie\"r en daar')."
    val result = parseAll(queryParser, input)

    result match {
      case Success(matched, _) => println(matched)
      case Failure(msg, _) => println(s"Parsing failed: $msg")
      case Error(msg, _) => println(s"Error: $msg")
    }
  }

  //based on thus loosely inspired by https://github.com/scala/scala-parser-combinators/blob/main/docs/Getting_Started.md
}
