import scala.collection.mutable.ListBuffer

class ConjunctiveQuery(val head : Head,  val body : Set[Atom]):
  override def toString : String =
    val bodyString : String = body.mkString(", ")
    s"$head :- $bodyString."

  def checkAcyclic(): Boolean =
    val nodeToEdge = collection.mutable.Map[String, ListBuffer[uniqueTerm]]()
    val allTerms = ListBuffer[uniqueTerm]()
    var edgesToCheck = ListBuffer[uniqueTerm]()

    body.foreach {
      atom => atom.uniqueTerms.variables.foreach {
          t =>
            nodeToEdge.update(t, nodeToEdge.getOrElse(t, ListBuffer[uniqueTerm]()) += atom.uniqueTerms)
            allTerms += atom.uniqueTerms
            edgesToCheck += atom.uniqueTerms
        }
    }
    println(nodeToEdge)

    var changedSomething = true

    edgesToCheck.foreach(e =>
      if e.active && allTerms.exists((ne: uniqueTerm) => e != ne && e.subsetOf(ne) || e.variables.isEmpty) then
        e.active = false
        changedSomething = true
    )

    while changedSomething do {
      changedSomething = false
      edgesToCheck = edgesToCheck.empty

      //remove nodes that are alone
      nodeToEdge.filterInPlace((node , edge) => {
        !(edge.size < 2 || {
          var edge_ptr = edge
          while edge_ptr.nonEmpty && !edge_ptr.head.active do edge_ptr = edge_ptr.tail
          edge_ptr.isEmpty || edge_ptr.tail.forall(e => e.equals(edge_ptr.head) || !e.active)
        }) || {
          //println("removed " + node)
          edge.head.variables.filterInPlace(_ != node)
          edgesToCheck.addAll(edge)
          edge.foreach(_.variables.filterInPlace(_ != node))
          changedSomething = true
          false
        }
      })

      edgesToCheck.foreach(e =>
        if e.active && allTerms.exists((ne: uniqueTerm) => e != ne && e.subsetOf(ne) || e.variables.isEmpty) then
          e.active = false
          changedSomething = true
      )
      //println("-----")
    }
    //println(nodeToEdge)
    nodeToEdge.isEmpty