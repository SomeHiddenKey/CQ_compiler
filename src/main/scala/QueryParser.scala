import org.apache.arrow.dataset.source.Dataset
import scala.util.parsing.combinator._
import scala.util.parsing.combinator.RegexParsers

object QueryParser extends RegexParsers {
  def apply(input: String)(using loaded_datasets : Map[String, Dataset]): Atom = parseAll(relation, input) match {
    case Success(result, _) => result
    case failure: NoSuccess => scala.sys.error(failure.msg)
  }

  def body: Parser[List[term]] = rep1sep(wordConstant | numberConstant | variable, ", ") ^^ {
    case c => c
  }
  def wordConstant: Parser[term]   = """[a-z][a-zA-Z]*""".r ^^  { case c => Constant[String](c.toString) }
  def numberConstant: Parser[term] = """(0|[1-9]\d*)""".r ^^    { case c => Constant[Int](c.toInt) }
  def variable: Parser[term] = """[A-Z][a-zA-Z]*""".r ^^        { case c => Variable(c.toString) }
  def relation(using loaded_datasets : Map[String, Dataset]): Parser[Atom] =
    ("""[a-z][a-zA-Z]+""".r ~ "(" ~ body ~ ")") ^^ {
      case head_name ~ sl ~ b ~ sr =>
        loaded_datasets.get(head_name) match
          case Some(ds) => new Atom(b.toSet, ds)
          case None => throw new Exception(f"dataset \"$head_name\" not found")
    }
}