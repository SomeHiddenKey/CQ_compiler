package jointree

import conjunctive_querry.Atom

/**
 * Node of [[Hypergraph]] that represents an [[Atom]] in the jointree
 * @param atom node value
 */
class Node(val atom: Atom){
  var value : List[List[AnyRef]] = List[List[AnyRef]]()
  private var parent : Option[Node] = None
  val children : scala.collection.mutable.Set[Node] = scala.collection.mutable.Set()

  def setParent(node : Node): Boolean =
    parent = Some(node)
    node.children.add(this)

  def getParent: Option[Node] = parent
}