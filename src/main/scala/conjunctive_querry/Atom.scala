package conjunctive_querry

/**
 * Relation instance in a [[ConjunctiveQuery]]
 * @param relationName name of the relation
 * @param terms terms present in the body of the relation, of type [[Term]] 
 * @param dataset path to csv linked with this relationship, or [[None]]
 */
class Atom(val relationName : String, val terms : List[Term], val dataset : Option[String] = None):
  val uniqueTerms: scala.collection.mutable.Set[String] = scala.collection.mutable.Set( terms.collect {
    case v : Variable => v.name
  } :_* )