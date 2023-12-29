import conjunctive_querry.{Atom, ConjunctiveQuery, Constant, Head, Term, Variable}

import scala.util.parsing.combinator.RegexParsers
import org.apache.arrow.vector.util.Text

/**
 * Regex parser to translates CQ query strings to actual [[ConjunctiveQuery]]
 * @param loaded_datasets name-path container for the saved csv files that the conjunctive querry can look up with
 * @note loosely inspired by https://github.com/scala/scala-parser-combinators/blob/main/docs/Getting_Started.md
 */
class QueryParser(loaded_datasets : Map[String, String]) extends RegexParsers {
  private def atomParser: Parser[Atom] = relationNameParser ~ "(" ~ termParser ~ ")" ^^ {
    case head_name ~ _ ~ b ~ _ =>
      new Atom(head_name, b, loaded_datasets.get(head_name.toLowerCase))
  }
  private def queryParser: Parser[ConjunctiveQuery] = headParser ~ bodyParser ^^ {
    case head ~ body => ConjunctiveQuery(head, body)
  }
  private def relationNameParser: Parser[String] = """[a-zA-Z]+""".r
  private def termParser: Parser[List[Term]] = repsep( parseTermDouble | parseTermInt | parseTermString | parseVariable, ",")
  private def parseTermInt: Parser[Term] = """0|(-?[1-9]\d*)\b""".r ^^ (c => Constant[Int](c.toInt))
  private def parseTermString: Parser[Term] = parseTermStringQuotedSingle | parseTermStringQuotedDouble

  private def parseTermStringQuotedSingle: Parser[Term] = "'" ~> """[a-zA-Z][^\n\t']*""".r <~ "'" ^^ (c => Constant[Text](Text(c)))
  private def parseTermStringQuotedDouble: Parser[Term] = '"' ~> """[a-zA-Z][^\n\t"]*""".r <~ '"' ^^ (c => Constant[Text](Text(c)))

  private def parseTermDouble: Parser[Term] = """-?\d+\.\d*\b""".r ^^ (c => Constant[Double](c.toDouble))
  private def parseVariable: Parser[Variable] = """[a-zA-Z][a-zA-Z0-9_]*""".r ^^ (c => Variable(c))
  private def headParser: Parser[Head] = atomParser <~ ":-" ^^ (atom => new Head(atom.relationName, atom.terms))
  private def bodyParser: Parser[Set[Atom]] = rep1sep(atomParser, ",") <~ "." ^^ (atoms => atoms.toSet)

  /**
   * Conjunctive Querry string parser
   * @param input [[String]] in correct CQ notation
   * @return correctly corresponding [[ConjunctiveQuery]]
   * @throws ArithmeticException string is not in the correct CQ notation and thus not recognizable
   */
  def apply(input: String): ConjunctiveQuery = {
    parseAll(queryParser, input) match {
      case Success(matched, _) => matched
      case Failure(msg, _) => throw new ArithmeticException(msg)
      case Error(msg, _) => throw new ArithmeticException(msg)
    }
  }
}
