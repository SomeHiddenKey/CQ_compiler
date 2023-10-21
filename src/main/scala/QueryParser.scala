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

object QueryParser extends JavaTokenParsers {
  private def atomParser: Parser[Atom] = relationNameParser ~ termParser <~ ")" ^^ {
    case relationName ~ terms => new Atom(relationName, terms)
  }
  private def relationNameParser: Parser[String] = """[A-Z][a-zA-Z]*\(""".r
  private def termParser: Parser[List[Term]] = rep1sep( parseTermInt | parseTermUtf8 | parseTermFloat | parseVariable, ",")
  private def parseTermInt: Parser[Term] = """(0|[1-9]\d*)""".r ^^ (c => Constant[Int](c.toInt))
  private def parseTermUtf8: Parser[Term] = """[a-z][a-zA-Z]*""".r ^^ (c => Constant[String](c))
  private def parseTermFloat: Parser[Term] = """-?\d+\.\d*[1-9]|\.\d*[1-9]|\d+\.\d*[1-9]""".r ^^ (c => Constant[Float](c.toFloat))
  private def parseVariable: Parser[Variable] = """[A-Z][a-zA-Z]*""".r ^^ (c => Variable(c))
  private def headParser: Parser[Head] = atomParser <~ """,?""".r <~ ":-" ^^ (atom => new Head(atom.relationName, atom.terms))
  private def bodyParser: Parser[Body] = rep1sep(atomParser, ",") ^^ (atoms => Body(atoms.toSet))

  private def parseQuery: Parser[ConjunctiveQuery] = headParser ~ bodyParser ^^ {
    case head ~ body => ConjunctiveQuery(head, body)
  }
  def parse(input: String): Option[ConjunctiveQuery] = {
    parseAll(parseQuery, input) match {
      case Success(result, _) => Some(result)
      case _ => None
    }
  }

  def main(args: Array[String]): Unit = {
    val input = "Answer(z) :- Beers(Orval, 1, y), Location(1, 65, Hier)"
    val result = parseAll(parseQuery, input)

    result match {
      case Success(matched, _) => println(matched)
      case Failure(msg, _) => println(s"Parsing failed: $msg")
      case Error(msg, _) => println(s"Error: $msg")
    }
  }
}
