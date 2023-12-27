import org.apache.arrow.dataset.source.Dataset
import scala.util.parsing.combinator.RegexParsers
import org.apache.arrow.vector.util.Text
import scala.util.parsing.combinator.*

class QueryParser(loaded_datasets : Map[String, String]) extends RegexParsers {
  private def atomParser: Parser[Atom] = relationNameParser ~ "(" ~ termParser ~ ")" ^^ {
    case head_name ~ _ ~ b ~ _ =>
      new Atom(head_name, b, loaded_datasets.get(head_name))
  }
  private def queryParser: Parser[ConjunctiveQuery] = headParser ~ bodyParser ^^ {
    case head ~ body => ConjunctiveQuery(head, body)
  }
  private def relationNameParser: Parser[String] = """[a-zA-Z]+""".r
  private def termParser: Parser[List[Term]] = repsep( parseTermFloat | parseTermInt | parseTermString | parseVariable, ",")
  private def parseTermInt: Parser[Term] = """0|(-?[1-9]\d*)\b""".r ^^ (c => Constant[Int](c.toInt))
  private def parseTermString: Parser[Term] = parseTermStringQuotedSingle | parseTermStringQuotedDouble

  private def parseTermStringQuotedSingle: Parser[Term] = "'" ~> """[a-zA-Z][^\n\t']*\b""".r <~ "'" ^^ (c => Constant[Text](Text(c)))
  private def parseTermStringQuotedDouble: Parser[Term] = '"' ~> """[a-zA-Z][^\n\t"]*\b""".r <~ '"' ^^ (c => Constant[Text](Text(c)))

  private def parseTermFloat: Parser[Term] = """-?\d+\.\d*\b""".r ^^ (c => Constant[Float](c.toFloat))
  private def parseVariable: Parser[Variable] = """[a-zA-Z][a-zA-Z0-9_]*""".r ^^ (c => Variable(c))
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

  //loosely inspired by https://github.com/scala/scala-parser-combinators/blob/main/docs/Getting_Started.md
}
