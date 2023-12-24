import scala.collection.mutable.ListBuffer

class ConjunctiveQuery(val head : Head,  val body : Set[Atom]):
  override def toString : String =
    val bodyString : String = body.mkString(", ")
    s"$head :- $bodyString."

  def getHyperGraph: Option[Hypergraph] =
    val nodeToEdge = collection.mutable.Map[String, ListBuffer[uniqueTerm]]()
    val allTerms = collection.mutable.Set[uniqueTerm]() // all possible edges
    val hypergraph : Hypergraph = new Hypergraph()

    body.foreach {
      atom =>
        val uniques = uniqueTerm(atom.uniqueTerms,new Node(atom))
        uniques.variables.foreach {
          t =>
            nodeToEdge.update(t, nodeToEdge.getOrElse(t, ListBuffer[uniqueTerm]()) += uniques) //update pointermap from node -> all terms it relates to
            allTerms += uniques // buildup all-edges list
        }
    }
    //println(nodeToEdge)

    var changedSomething = true

    allTerms.foreach(e =>
      // initial check for all edges if they're s a subset of another node (omit being a subset of yourself)
      if e.active then
        allTerms.collectFirst { case parentEdge if e.variables != parentEdge.variables && e.subsetOf(parentEdge) =>
          hypergraph.nodes.add(e.node)
          e.node.setParent(parentEdge.node)
          e.active = false
        }
    )

    while changedSomething do {
      changedSomething = false

      //filter out all variables with only one referred edge
      nodeToEdge.filterInPlace((node , edges) => {
        var edge_actives = edges
        edges.size > 1 && {
          edge_actives = edges.filter(_.active) //filter out all inactive edges and count again
          nodeToEdge.update(node, edge_actives)
          edge_actives.size > 1
        }
        ||
        {
          // variable needs deleting
          if edge_actives.nonEmpty then {
            val cutEdge = edge_actives.head.variables.filter(v => nodeToEdge.getOrElse(v, ListBuffer()).count(_.active) > 1) //filter out any other variables only bound to one edge
            if cutEdge.nonEmpty then {
              allTerms.collectFirst {
                case parentEdge if parentEdge.active && edge_actives.head.variables != parentEdge.variables && cutEdge.subsetOf(parentEdge.variables) =>
                  //first parentEdge that fully contains cutEdge (so without the lone variables)
                  hypergraph.nodes.add(edge_actives.head.node)
                  //set parent-child relation for jointree
                  edge_actives.head.node.setParent(parentEdge.node)
                  //deactivate edge that we now consider as "removed"
                  edge_actives.head.active = false
                  changedSomething = true
              }
            } else {
              //lone edge is a new root
              hypergraph.nodes.add(edge_actives.head.node)
              hypergraph.roots.add(edge_actives.head.node)
              changedSomething = true
            }
          }
          false
        }
      })
    }

    //are there any nodes still attached to active edges -> if yes then acyclic
    if nodeToEdge.isEmpty then Some(hypergraph) else None