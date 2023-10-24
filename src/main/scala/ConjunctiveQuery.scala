class ConjunctiveQuery(val head : Head,  val body : Set[Atom]):
  override def toString : String =
    val bodyString : String = body.mkString(", ")
    s"$head :- $bodyString."

  def checkAcyclic(): Boolean =
    val nodeToEdge = collection.mutable.Map[String, collection.mutable.Set[Atom]]()
    body.foreach {
      atom =>
        atom.uniqueTerms.foreach {
          t => nodeToEdge.update(t, nodeToEdge.getOrElse(t, collection.mutable.Set[Atom]()) += atom)
        }
    }
    println(nodeToEdge)
    return true