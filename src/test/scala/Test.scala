import Runner.{listFilesInDirectory, read}
import org.apache.arrow.dataset.scanner.ScanOptions
import org.scalatest.*
import org.scalatest.funsuite.AnyFunSuite

class Test extends AnyFunSuite {
  var options: ScanOptions = new ScanOptions(/*batchSize*/ 32768)
  var loaded_datasets: Map[String, String] = Map()
  val files: Seq[String] = listFilesInDirectory(System.getProperty("user.dir") + s"/data/")
  for (file <- files)
    read("file:///" + System.getProperty("user.dir").replace(" ", "%20") + "/data/", file)

  val q: QueryParser = QueryParser(loaded_datasets)

  test("A query should be parsed from a string") {
    assert(q("Answer() :- beers(a,b,c,d,e,f,g,h,i).").isInstanceOf[ConjunctiveQuery])
  }

  test("A query should end on a '.'") {
    assertThrows[ArithmeticException] {
      q("Answer() :- beers(a,b,c,d,e,f,g,h,i)")
    }
  }

  test("A query can have variables in the head") {
    assert(q("Answer(a, d, f) :- beers(a,b,c,d,e,f,g,h,i).").isInstanceOf[ConjunctiveQuery])
  }

  test("A query can contain constants") {
    assert(q("Answer(x,y) :- beers(1,b,c,d,e,f,g,h,i).").isInstanceOf[ConjunctiveQuery])
  }

  test("A query contains a head and a body") {
    assert(q("Answer(x,y) :- beers(1,b,c,d,e,f,g,h,i).").isInstanceOf[ConjunctiveQuery])
  }

}
