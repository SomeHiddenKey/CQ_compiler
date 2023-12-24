class Hypergraph(val nodes : scala.collection.mutable.Set[Node] = scala.collection.mutable.Set()){
  val roots : scala.collection.mutable.Set[Node] = scala.collection.mutable.Set()
}

class Node(val atom: Atom){
  var value : List[List[AnyRef]] = List[List[AnyRef]]()
  private var parent : Option[Node] = None
  val children : scala.collection.mutable.Set[Node] = scala.collection.mutable.Set()

  def setParent(node : Node): Boolean =
    parent = Some(node)
    node.children.add(this)

  def getParent: Option[Node] = parent
}