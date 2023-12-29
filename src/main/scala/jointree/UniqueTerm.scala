package jointree

/** collection of unique variables linked to a node */
class UniqueTerm(val variables : scala.collection.mutable.Set[String], val node: Node){
  var active: Boolean = true
  def subsetOf(value: UniqueTerm): Boolean = this.variables.subsetOf(value.variables)
}