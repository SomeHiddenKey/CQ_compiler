package conjunctive_query

import jointree.{Hypergraph, UniqueTerm, Node}

import scala.collection.mutable.ListBuffer

/**
 * instance that represents a query as CQ
 * @param head header atom as result projection of the query
 * @param body list of atoms in the body of the query
 */
class ConjunctiveQuery(val head : Head,  val body : Set[Atom]):

  /**
   * returns the jointree of the Conjunctive Query if it is acyclic
   * @return [[Option]] either some hypergraph jointree or None
   */
  def getHyperGraph: Option[Hypergraph] =
    //mapping of all vertex to their respective edges as to quickly spot those linked to only (active) edge
    val nodeToEdge = collection.mutable.Map[String, ListBuffer[UniqueTerm]]()
    // all possible edges
    val allTerms = collection.mutable.Set[UniqueTerm]()
    // graph we will slowly build up on
    val hypergraph : Hypergraph = new Hypergraph()

    body.foreach {
      atom =>
        val uniques = UniqueTerm(atom.uniqueTerms,new Node(atom))
        uniques.variables.foreach {
          t =>
            // slowly update to collect all vertex -> edges list
            nodeToEdge.update(t, nodeToEdge.getOrElse(t, ListBuffer[UniqueTerm]()) += uniques)
            // buildup all-edges list as we go through the CQ body
            allTerms += uniques
        }
    }

    // flag to check if something changed. Once false, it makes no sense to continue as there won't be new changes to work with
    var changedSomething = true

    allTerms.foreach(e =>
      // initial check for all edges if they're s fully a subset of another node (omit being a subset of yourself)
      if e.active then
        allTerms.collectFirst { case parentEdge if e.variables != parentEdge.variables && e.subsetOf(parentEdge) =>
          // subset edge is added to the hypergraph, with the containing edge e.node as observer
          hypergraph.nodes.add(e.node)
          e.node.setParent(parentEdge.node)
          // edge has been consumed and thus set to false
          e.active = false
        }
    )

    while changedSomething do {
      changedSomething = false

      // filter out all variables with only one referred edge
      nodeToEdge.filterInPlace((node , edges) => {
        // filter out all inactive edges and count again
        val edge_actives = edges.filter(_.active)
        {
          // if some were filtered out, update the nodeToEdge as only to keep active edges
          if edges.length != edge_actives.length then nodeToEdge.update(node, edge_actives)
          // node is only connected to one edge
          edge_actives.size > 1
        }
        ||
        {
          // variable needs deleting
          if edge_actives.nonEmpty then {
            // collect all nodes with only connected (being to that specific same edge)
            val cutEdge = edge_actives.head.variables.filter(v => nodeToEdge.getOrElse(v, ListBuffer()).count(_.active) > 1)
            if cutEdge.nonEmpty then {
              // the node is not on its own
              allTerms.collectFirst {
                case parentEdge if parentEdge.active && edge_actives.head.variables != parentEdge.variables && cutEdge.subsetOf(parentEdge.variables) =>
                  // first parentEdge that fully contains cutEdge (so without the lone variables)
                  hypergraph.nodes.add(edge_actives.head.node)
                  // set parent-child relation for jointree
                  edge_actives.head.node.setParent(parentEdge.node)
                  // deactivate edge that we now consider as "removed"
                  edge_actives.head.active = false
                  changedSomething = true
              }
            } else {
              // lone edge is a new root
              hypergraph.nodes.add(edge_actives.head.node)
              hypergraph.roots.add(edge_actives.head.node)
              // deactivate edge that we now consider as "removed"
              edge_actives.head.active = false
              changedSomething = true
            }
          }
          false
        }
      })
    }

    // are there any nodes still attached to active edges -> if yes then acyclic
    if nodeToEdge.isEmpty then
      Some(hypergraph) else None