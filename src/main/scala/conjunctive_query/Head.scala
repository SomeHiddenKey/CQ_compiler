package conjunctive_query

/**
 * Query head of a [[ConjunctiveQuery]]
 * @param relationName name of the output
 * @param terms [[Term]]s the output should project to
 */
class Head(relationName: String, terms: List[Term]) extends Atom(relationName, terms)