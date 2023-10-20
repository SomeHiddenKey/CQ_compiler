import org.apache.arrow.dataset.source.Dataset
import org.apache.arrow.flatbuf

type apacheType = flatbuf.Int | flatbuf.Utf8 | flatbuf.FloatingPoint
trait term

class Atom(val terms: List[term], val dataset : Dataset)

case class Constant[T <: Any](val value : T) extends term
case class Variable(val name : String) extends term