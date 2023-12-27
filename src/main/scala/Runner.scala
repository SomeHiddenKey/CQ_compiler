import org.apache.arrow.dataset.file.FileFormat
import org.apache.arrow.dataset.file.FileSystemDatasetFactory
import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.arrow.dataset.scanner.Scanner
import org.apache.arrow.dataset.source.Dataset
import org.apache.arrow.dataset.source.DatasetFactory
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.table.Table

import java.io.{File, PrintWriter}

object Runner {
  var options : ScanOptions = new ScanOptions(/*batchSize*/ 32768)

  var loaded_datasets : Map[String, String] = Map()

  @main def start(): Unit =
   // println("file:///" + System.getProperty("user.dir").replace(" ", "%20"))
    val files = listFilesInDirectory(System.getProperty("user.dir") + s"/data/")
    for (file <- files)
      read("file:///" + System.getProperty("user.dir").replace(" ", "%20") + "/data/", file)

    val q: QueryParser = QueryParser(loaded_datasets)
    val cq : ConjunctiveQuery = q("Answer(z, 5) :- beers(A, B), location(B, C).")
   // println(cq.getHyperGraph)
    val cq1 : ConjunctiveQuery = q("Answer(z, 5) :- beers(a, 166, a, Dee_56jj, E, F, G, G).")
    //Yannakakis.qs(cq1.body.head)
   // println(cq1.getHyperGraph)
    val cq2 : ConjunctiveQuery = q("Answer(z, 5) :- beers(A, B), beers(A, Z), beers(A, B, C), beers(B, C), beers(C, A).")
   // println(cq2.getHyperGraph)
    val cq3: ConjunctiveQuery = q("Answer(z, 5) :- beers(A, B), Beers(B, C), Beers(C, A), Beers(A, Z).")
   // println(cq3.getHyperGraph)
    val cq4: ConjunctiveQuery = q("Answer(z, 5) :- beers(A, B), beers(A, B, C), beers(B, C), beers(A, B), beers(C, A), beers(A, Z).")
   // println(cq4.getHyperGraph)
    val cq5: ConjunctiveQuery = q("Answer(z, 5) :- beers(A, A, B, B), beers(A, B, C, C), beers(B, C), beers(A, B), beers(C, A), beers(A, Z).")
   // println(cq5.getHyperGraph)
    val cq6 : ConjunctiveQuery = q("Answers(r) :- beers(C), beers(B).")

    //val cqYannakakis : ConjunctiveQuery = q("Answer(z) :- breweries(3, B, C, D, E, F, G, 'Belgium', I, J, K).")
    //Yannakakis(cqYannakakis.getHyperGraph.get)


   // val cqYannakakis2: ConjunctiveQuery = q("Answer(z) :- breweries(A, B, C, D, E, 'Or', G, H, I, J, K).")
    val cqYannakakis2: ConjunctiveQuery = q("Answer(x, y, z) :- Breweries(w, x,'Westmalle', u1, u2, u3, u4, u5, u6 ,u7 ,u8), Locations(u9, w, y, z, u10).")
    val res2 = Yannakakis(cqYannakakis2)
    //println(res2)


    val queries: List[String] = List(
      "Answer() :- beers(u1, x, u2, 0.07, u3, u4, y, u5), styles(u6, z, y), categories(z, u7), locations(u9, x, u9, u10, u11), breweries(x, u12, u13, u14, u15, u16, u17, u18, u13, u14, u15).",
      //   "Answer(x, y, z) :- Breweries(w, x,'Westmalle', u1, u2, u3, u4, u5, u6 ,u7 ,u8), Locations(u9, w, y, z, u10).",
      //   "Answer(x, y, z) :- Beers(u1, u2, z, u3, u4, u5, x, u6), Styles(u7, y, x), Categories(y, z).",
      //   "Answer(x, y, z, w) :- Beers(u1, v, x, '0.05', '18', u2, 'Vienna Lager', u3), Locations(u4, v, y, z, w).",
      //   "Answer(x, y, z, w) :- Beers(u1, x, u2, '0.06', u3, u4, y, u5), Styles(u6, z, y), Categories(z, w), Locations(u8, x, u9, u10, u11), Breweries(x, u12, u13, u14, u15, u16, u17, u18, u13, u14, u15)."
    )

    var data : List[Map[String, Any]] = List.empty

    for ((query, index) <- queries.zipWithIndex) {
      val res = scala.collection.mutable.Map[String, Any]("query_id" -> index)
      val conjunctiveQuery: ConjunctiveQuery = q(query)
      conjunctiveQuery.getHyperGraph match
        case Some(_) => res += ("is_acyclic" -> 0)
        case None => res += ("is_acyclic" -> 1)
      res += ("bool_answer" -> Yannakakis.YannakakisEvalBoolean(conjunctiveQuery.getHyperGraph.get.roots.head))
      res += ("attr_x_answer" -> "")
      res += ("attr_y_answer" -> "")
      res += ("attr_z_answer" -> "")
      res += ("attr_w_answer" -> "")
      data = data :+ res.toMap
    }
    write(data)

  private def read(uri: String, file_name : String): Unit =
    try {
      loaded_datasets = loaded_datasets + (file_name.split("\\.").head -> uri.+(file_name) )
    }
    catch case e: Exception => e.printStackTrace()

  def listFilesInDirectory(directoryPath: String): List[String] = {
    val directory = new File(directoryPath)
    if (directory.exists && directory.isDirectory) {
      directory.listFiles.filter(_.isFile).map(_.getName).toList
    } else {
      List.empty
    }
  }

  private def write(data: List[Map[String, Any]]): Unit =
    val headers = List("query_id", "is_acyclic", "bool_answer", "attr_x_answer", "attr_y_answer", "attr_z_answer", "attr_w_answer")
    val filePath = System.getProperty("user.dir") + "/data/output.csv"
    val writer = new PrintWriter(new File(filePath))

    writer.println(headers.mkString(","))
    data.foreach { row =>
      writer.println(headers.map(col => row(col)).mkString(","))
    }

    writer.close()
}