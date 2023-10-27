import org.apache.arrow.dataset.source.Dataset
import org.apache.arrow.flatbuf

import scala.annotation.targetName

/*type apacheType = flatbuf.Int | flatbuf.Utf8 | flatbuf.FloatingPoint
trait Term

class Atom(val terms: List[Term], val dataset : Dataset)

case class Constant[T <: Any](value : T) extends Term
case class Variable(name : String) extends Term
*/

type apacheType = Int | String | Float
trait Term

class uniqueTerm(val variables : scala.collection.mutable.Set[Variable], var active: Boolean = true){
  def equals(value: uniqueTerm): Boolean = this.variables == value.variables

  override def toString: String = "["+variables.toString+"=>"+active+"]"
  def subsetOf(value: uniqueTerm): Boolean = this.variables.subsetOf(value.variables)
}

class Atom(val relationName : String, val terms : List[Term], val dataset : Option[Dataset] = None):
  var uniqueTerms: uniqueTerm = uniqueTerm(scala.collection.mutable.Set( terms.collect {
    case v : Variable => v.copy()
  } :_* ))

  override def toString: String =
    var string: String = ""
    terms.foreach {
      case c: Constant[apacheType] => string = string + c.value + ", "
      case v: Variable => string = string + v.name + ", "
    }
    s"$relationName($string)"

case class Constant[T <: apacheType](value : T) extends Term
case class Variable(name: String) extends Term {
  @targetName("equals")
  def ==(value : Variable): Boolean = this.name==value.name
}
class Head(relationName: String, terms: List[Term]) extends Atom(relationName, terms)

def head : Head = Head("Answer", List(Variable("x")))
def query : ConjunctiveQuery = new ConjunctiveQuery(head, Set(Atom("Beer", List(Constant(1), Variable("y"))), Atom("Location", List(Variable("y"), Variable("x")))))

//@main def start() : Unit =
//  println(query)
//  query.body.foreach(element => {
//    println(element.uniqueTerms)
//  })
