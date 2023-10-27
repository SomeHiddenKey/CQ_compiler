import scala.collection.mutable.ListBuffer

class ConjunctiveQuery(val head : Head,  val body : Set[Atom]):
  override def toString : String =
    val bodyString : String = body.mkString(", ")
    s"$head :- $bodyString."

  def checkAcyclic(): Boolean =
    val nodeToEdge = collection.mutable.Map[String, ListBuffer[uniqueTerm]]()
    val allTerms = ListBuffer[uniqueTerm]() // all possible edges
    var edgesToCheck = ListBuffer[uniqueTerm]() // list of edges to check of subsets

    body.foreach {
      atom => atom.uniqueTerms.variables.foreach {
          t =>
            nodeToEdge.update(t, nodeToEdge.getOrElse(t, ListBuffer[uniqueTerm]()) += atom.uniqueTerms) //update pointermap from node -> all terms it relates to
            allTerms += atom.uniqueTerms     // buildup all-edges list
            edgesToCheck += atom.uniqueTerms // initial pass-through for all edges in algorithm, assuming everything is new
        }
    }
    println(nodeToEdge)

    var changedSomething = true

    edgesToCheck.foreach(e =>
      // initial check for all edges if they're s a subset of another node (omit being a subset of yourself)
      if e.active && allTerms.exists((ne: uniqueTerm) => e != ne && e.subsetOf(ne) || e.variables.isEmpty) then
        e.active = false
    )

    // loop as long as nodes and edges are being changed
    while changedSomething do {
      changedSomething = false
      edgesToCheck = edgesToCheck.empty

      //remove nodes that are alone
      nodeToEdge.filterInPlace((node , edge) => {
        // filter all edges with a size < 1 or where all active edges are the same => eliminates nodes that only belong to one edge
        !(edge.size < 2 || {
          var edge_ptr = edge
          while edge_ptr.nonEmpty && !edge_ptr.head.active do edge_ptr = edge_ptr.tail //search for any active edge
          if edge_ptr.nonEmpty && edge != edge_ptr then nodeToEdge.update(node, edge_ptr) //remove all non-active edges in map
          edge_ptr.isEmpty || edge_ptr.tail.forall(e => e.equals(edge_ptr.head) || !e.active)
        }) || {
          //println("removed " + node)
          edgesToCheck.addAll(edge)
          edge.foreach(_.variables.filterInPlace(_ != node)) //removes variable in given edge(s) it relates to
          changedSomething = true
          false
        }
      })

      edgesToCheck.foreach(e =>
        //disable edges that have been updated to see if they're a subset of any other (active) edge
        if e.active && allTerms.exists((ne: uniqueTerm) => e != ne && e.subsetOf(ne) || e.variables.isEmpty) then
          e.active = false
          changedSomething = true
      )
    }
    //println(nodeToEdge)
    nodeToEdge.isEmpty