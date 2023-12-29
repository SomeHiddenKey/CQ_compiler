package jointree

/**
 * hypergraph instance
 * @param nodes set of [[Node]] present in the graph. The graph may contain multiple roots so isn't necessarily connected
 */
class Hypergraph(val nodes : scala.collection.mutable.Set[Node] = scala.collection.mutable.Set()){
  val roots : scala.collection.mutable.Set[Node] = scala.collection.mutable.Set()
}

