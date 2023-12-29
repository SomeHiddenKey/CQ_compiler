import Runner.{listFilesInDirectory, read}
import Yannakakis.{YannakakisEvalBoolean, apply}
import conjunctive_querry.ConjunctiveQuery
import jointree.Hypergraph
import org.apache.arrow.dataset.scanner.ScanOptions
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
    val query = q("Answer(x,y) :- beers(1,b,c,d,e,f,g,h,i).")
    assert(query.head.terms !== List.empty)
    assert(query.body !== Set.empty)
  }

  test("It can be detected whether a query is acyclic or not") {
    val acyclic = q("Answer(x,y) :- beers(1,b,c,d,e,f,g,h,i), breweries(b,j,k,l,m,n,o,p,q,r,s).")
    val cyclic = q("Answer(x, y, z) :- Beers(u1, u2, z, u3, u4, u5, x, u6), Styles(u7, y, x), Categories(y, z).")
    assert(acyclic.getHyperGraph match
      case Some(_: Hypergraph) => true
      case _ => false
    )
    assert(cyclic.getHyperGraph match
      case None => true
      case _ => false
    )
  }

  test("A query can be boolean") {
    val boolean = q("Answer() :- beers(u1, x, u2, 0.07, u3, u4, y, u5), styles(u6, z, y), categories(z, u7), locations(u9, x, u9, u10, u11), breweries(x, u12, u13, u14, u15, u16, u17, u18, u13, u14, u15).")
    println(Yannakakis(boolean))
  }

  test("The Yannakakis algorithm gets all expected results from the data") {
    val query = q("Answer(u1) :- Beers(u1, u2, u3, u4, u5, u6, u7, u8).")
   // assert(Yannakakis(query).size == 2410)
  }

}
