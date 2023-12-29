import Runner.listFilesInDirectory
import conjunctive_query.{ConjunctiveQuery, Term, Variable}
import jointree.Hypergraph
import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.arrow.vector.util.Text
import org.scalatest.funsuite.AnyFunSuite
import Yannakakis.projection

import scala.util.Try

class Test extends AnyFunSuite {
  var options: ScanOptions = new ScanOptions(/*batchSize*/ 32768)
  var loaded_datasets: Map[String, String] = Map()
  val files: Seq[String] = listFilesInDirectory(System.getProperty("user.dir") + s"/data/")
  for (file <- files)
    read("file:///" + System.getProperty("user.dir").replace(" ", "%20") + "/data/", file)

  val q: QueryParser = QueryParser(loaded_datasets)

  def read(uri: String, file_name: String): Unit =
    try {
      loaded_datasets = loaded_datasets + (file_name.split("\\.").head -> uri.+(file_name))
    }
    catch case e: Exception => e.printStackTrace()

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

  test("A jointree can have a single or multiple roots, both with a subset of 1 or more vertices") {
    val singleRoot = q("Answer() :- R(A, B), S(B, C), T(C, D).")
    val singleRootMultipleChildren = q("Answer() :- R(A, B, C), S(B, D, C), T(C, Z), U(C).")
    val multipleRoots = q("Answer() :- R(A, B), S(B, C), T(D, E), U(E, F).")
    assert(singleRoot.getHyperGraph.get.roots.size == 1)
    assert(singleRootMultipleChildren.getHyperGraph.get.roots.size == 1)
    assert(multipleRoots.getHyperGraph.get.roots.size > 1)
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
    assert(!Yannakakis.YannakakisEvalBoolean(boolean.getHyperGraph.get.roots.head))
  }

  test("The Yannakakis algorithm gets all expected results from the data") {
    val query = q("Answer(x) :- Beers(u1, u2, u3, u4, u5, u6, u7, u8).")
    assert(Yannakakis(query).size == 2410)
  }

  test("The Yannakakis algorithm can give the variables that were requested in the answer, without returning the entire result (and in the right order)"){
    val query = q("Answer(u2) :- Beers(u1, u2, u3, u4, u5, u6, u7, u8).")
    val query2 = q("Answer(u3, u1, u5, u8) :- Beers(u1, u2, u3, u4, u5, u6, u7, u8).")
    assert(Yannakakis(query).flatten.size == 2410)
    assert(Yannakakis(query2).flatten.size == (2410*4))
  }

  test("The requested variables get projected the right way") {
    val queryJoin = q("Answer(x, w, y, z) :- Beers(u1, x, u2, 0.06, u3, u4, y, u5), Styles(u6, z, y), Categories(u7, w).")
    val termList : List[Term] = List(Variable("x"), Variable("w"), Variable("y"), Variable("z"))
    val colList : List[Term] = List(Variable("x"), Variable("y"), Variable("z"), Variable("w"))
    val resStart = List(List(478, "Oatmeal Stout", 1, "British Ale"), List(478, "Oatmeal Stout", 1, "Irish Ale"), List(478, "Oatmeal Stout", 1, "North American Ale"), List(478, "Oatmeal Stout", 1, "German Ale"), List(478, "Oatmeal Stout", 1, "Belgian and French Ale"), List(478, "Oatmeal Stout", 1, "International Ale"), List(478, "Oatmeal Stout", 1, "German Lager"), List(478, "Oatmeal Stout", 1, "North American Lager"), List(478, "Oatmeal Stout", 1, "Other Lager"), List(478, "Oatmeal Stout", 1, "International Lager"), List(478, "Oatmeal Stout", 1, "Other Style"), List(361, "Oatmeal Stout", 1, "British Ale"), List(361, "Oatmeal Stout", 1, "Irish Ale"), List(361, "Oatmeal Stout", 1, "North American Ale"), List(361, "Oatmeal Stout", 1, "German Ale"), List(361, "Oatmeal Stout", 1, "Belgian and French Ale"), List(361, "Oatmeal Stout", 1, "International Ale"), List(361, "Oatmeal Stout", 1, "German Lager"), List(361, "Oatmeal Stout", 1, "North American Lager"), List(361, "Oatmeal Stout", 1, "Other Lager"), List(361, "Oatmeal Stout", 1, "International Lager"), List(361, "Oatmeal Stout", 1, "Other Style"))
    val res = resStart.map(row => row.map(_.asInstanceOf[AnyRef]))
    assert(Yannakakis(queryJoin).head.head.isInstanceOf[Long])
    assert(Yannakakis(queryJoin).head(1).isInstanceOf[Text])
    assert(Yannakakis(queryJoin).head(2).isInstanceOf[Text])
    assert(Yannakakis(queryJoin).head(3).isInstanceOf[Long])
    assert(res.head(2).isInstanceOf[Int])
    assert(projection(termList, colList, res).head(2).isInstanceOf[String])
  }

  test("Runner.start() does not throw an exception"){
    assert(Try(Runner.start()).isSuccess)
  }
}
