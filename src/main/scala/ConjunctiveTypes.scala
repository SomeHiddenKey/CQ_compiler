import org.apache.arrow.vector.util.Text

type apacheType = Int | Text | Double
trait Term

class uniqueTerm(val variables : scala.collection.mutable.Set[String], val node: Node){
  var active: Boolean = true
  def equals(value: uniqueTerm): Boolean = this.variables == value.variables
  override def toString: String = "["+variables.toString+"=>"+active+"]"
  def subsetOf(value: uniqueTerm): Boolean = this.variables.subsetOf(value.variables)

}

class Atom(val relationName : String, val terms : List[Term], val dataset : Option[String] = None):
  val uniqueTerms: scala.collection.mutable.Set[String] = scala.collection.mutable.Set( terms.collect {
    case v : Variable => v.name
  } :_* )

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