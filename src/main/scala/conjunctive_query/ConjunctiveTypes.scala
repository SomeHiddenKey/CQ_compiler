package conjunctive_query

import org.apache.arrow.vector.util.Text

type apacheType = Int | Text | Double

trait Term

/** constant value in [[Atom]] body */
case class Constant[T <: apacheType](value : T) extends Term

/** variable instance in [[Atom]] body */
case class Variable(name: String) extends Term