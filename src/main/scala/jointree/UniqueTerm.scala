package jointree

/** collection of unique variables linked to a node */
class UniqueTerm(val variables : scala.collection.mutable.Set[String], val node: Node){
  var active: Boolean = true
  def equals(value: UniqueTerm): Boolean = this.variables == value.variables
  override def toString: String = "["+variables.toString+"=>"+active+"]"
  def subsetOf(value: UniqueTerm): Boolean = this.variables.subsetOf(value.variables)
}