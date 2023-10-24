import org.apache.arrow.dataset.source.Dataset
import org.apache.arrow.flatbuf

/*type apacheType = flatbuf.Int | flatbuf.Utf8 | flatbuf.FloatingPoint
trait Term

class Atom(val terms: List[Term], val dataset : Dataset)

case class Constant[T <: Any](value : T) extends Term
case class Variable(name : String) extends Term
*/

type apacheType = Int | String | Float
trait Term

class Atom(val relationName : String, val terms : List[Term], val dataset : Option[Dataset] = None):
  var uniqueTerms: Set[String] = terms.collect {
    case v : Variable => v.name
  }.toSet

  override def toString: String =
    var string: String = ""
    terms.foreach {
      case c: Constant[apacheType] => string = string + c.value + ", "
      case v: Variable => string = string + v.name + ", "
    }
    s"$relationName($string)"

case class Constant[T <: apacheType](value : T) extends Term
case class Variable(name: String) extends Term
class Head(relationName: String, terms: List[Term]) extends Atom(relationName, terms)

def head : Head = Head("Answer", List(Variable("x")))
def query : ConjunctiveQuery = new ConjunctiveQuery(head, Set(Atom("Beer", List(Constant(1), Variable("y"))), Atom("Location", List(Variable("y"), Variable("x")))))

//@main def start() : Unit =
//  println(query)
//  query.body.foreach(element => {
//    println(element.uniqueTerms)
//  })
