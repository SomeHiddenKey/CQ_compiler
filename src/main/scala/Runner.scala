import org.apache.arrow.dataset.scanner.ScanOptions

import java.io.{File, PrintWriter}

object Runner {
  var options : ScanOptions = new ScanOptions(/*batchSize*/ 32768)

  var loaded_datasets : Map[String, String] = Map()

  @main def start(): Unit =
    val files = listFilesInDirectory(System.getProperty("user.dir") + s"/data/")
    for (file <- files)
      read("file:///" + System.getProperty("user.dir").replace(" ", "%20") + "/data/", file)

    val q: QueryParser = QueryParser(loaded_datasets)

    val queries: List[String] = List(
      "Answer() :- beers(u1, x, u2, 0.07, u3, u4, y, u5), styles(u6, z, y), categories(z, u7), locations(u9, x, u9, u10, u11), breweries(x, u12, u13, u14, u15, u16, u17, u18, u13, u14, u15).",
      "Answer(x, y, z) :- Breweries(w, x,'Westmalle', u1, u2, u3, u4, u5, u6 ,u7 ,u8), Locations(u9, w, y, z, u10).",
      "Answer(x, y, z) :- Beers(u1, u2, z, u3, u4, u5, x, u6), Styles(u7, y, x), Categories(y, z).",
      "Answer(x, y, z, w) :- Beers(u1, v, x, 0.05, 18, u2, 'Vienna Lager', u3), Locations(u4, v, y, z, w).",
      "Answer(x, y, z, w) :- Beers(u1, x, u2, 0.06, u3, u4, y, u5), Styles(u6, z, y), Categories(z, w), Locations(u8, x, u9, u10, u11), Breweries(x, u12, u13, u14, u15, u16, u17, u18, u13, u14, u15)."
    )
    var data : List[Map[String, Any]] = List.empty

    for ((query, index) <- queries.zipWithIndex) {
      val res = scala.collection.mutable.Map("query_id" -> (index + 1), "is_acyclic" -> 0, "bool_answer" -> "", "attr_x_answer" -> "", "attr_y_answer" -> "", "attr_z_answer" -> "", "attr_w_answer" -> "")

      q(query).getHyperGraph match {
        case Some(_) =>
          res("is_acyclic") = 1
          if (q(query).head.terms.nonEmpty) {
            val answer = Yannakakis(q(query))
            if (answer.nonEmpty) {
              answer.foreach(row => {
                data :+= (res ++ Map("bool_answer" -> "", "attr_x_answer" -> row.head, "attr_y_answer" -> row(1), "attr_z_answer" -> row(2), "attr_w_answer" -> row(3))).toMap
              })
             } else {
              data :+= res.toMap
            }
          } else if (Yannakakis.YannakakisEvalBoolean(q(query).getHyperGraph.get.roots.head)) {
            data :+= (res ++ Map("bool_answer" -> 1)).toMap
          } else {
            data :+= (res ++ Map("bool_answer" -> 0)).toMap
          }
        case None =>
          data :+= res.toMap
      }
    }
    write(data)

  def read(uri: String, file_name : String): Unit =
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