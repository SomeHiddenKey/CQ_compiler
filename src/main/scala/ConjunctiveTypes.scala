/*import org.apache.arrow.dataset.source.Dataset
import org.apache.arrow.flatbuf

type apacheType = flatbuf.Int | flatbuf.Utf8 | flatbuf.FloatingPoint
trait Term

class Atom(val terms: List[Term], val dataset : Dataset)

case class Constant[T <: Any](value : T) extends Term
case class Variable(name : String) extends Term
*/
//import org.apache.arrow.flatbuf.{Int, Utf8, FloatingPoint}
//type apacheType = Int | Utf8 | FloatingPoint
type apacheType = Int | String | Float
trait Term
class Atom(val relationName : String, val terms : List[Term]):
  var uniqueTerms: Set[Term] = terms.filter {
    case _ : Variable => true
    case _ : Constant[apacheType] => true
    case _ => false
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
case class Body(atoms : Set[Atom])
class ConjunctiveQuery(val head : Head,  val body : Body):
  override def toString : String =
    val bodyString : String = body.atoms.mkString(", ")
    s"$head :- $bodyString."



def body : Body = Body(Set(Atom("Beer", List(Constant(1), Variable("y"))), Atom("Location", List(Variable("y"), Variable("x")))))
def head : Head = Head("Answer", List(Variable("x")))
def query : ConjunctiveQuery = new ConjunctiveQuery(head, body)



@main def start() : Unit =
  println(query)
  query.body.atoms.foreach(element => {
    println(element.uniqueTerms)
  })
