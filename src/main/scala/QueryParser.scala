import org.apache.arrow.dataset.source.Dataset
import scala.util.parsing.combinator.RegexParsers

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
}