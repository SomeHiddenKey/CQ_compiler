import org.apache.arrow.dataset.source.Dataset
import org.apache.arrow.flatbuf

type apacheType = flatbuf.Int | flatbuf.Utf8 | flatbuf.FloatingPoint
trait term

class Atom(val terms: Set[term], val dataset : Dataset)

case class Constant[T <: Any](value : T) extends term
case class Variable(name : String) extends term