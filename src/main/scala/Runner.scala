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

import java.io.File

object Runner {
  var options : ScanOptions = new ScanOptions(/*batchSize*/ 32768)

  var loaded_datasets : Map[String, Dataset] = Map()

  @main def start(): Unit =
    println("file:///" + System.getProperty("user.dir").replace(" ", "%20"))
    val files = listFilesInDirectory(System.getProperty("user.dir") + s"/data/")
    for (file <- files)
      read("file:///" + System.getProperty("user.dir").replace(" ", "%20") + "/data/", file)

    val q: QueryParser = QueryParser(loaded_datasets)
    val cq : ConjunctiveQuery = q("Answer(z, 5) :- beers(A, B), location(B, C).")
    println(cq.getHyperGraph)
    val cq1 : ConjunctiveQuery = q("Answer(z, 5) :- beers(A, 166, C, D, E, F, G, G).")
    Yanakakis.qs(cq1.body.head)
    println(cq1.getHyperGraph)
    val cq2 : ConjunctiveQuery = q("Answer(z, 5) :- beers(A, B), beers(A, Z), beers(A, B, C), beers(B, C), beers(C, A).")
    println(cq2.getHyperGraph)
    val cq3: ConjunctiveQuery = q("Answer(z, 5) :- beers(A, B), Beers(B, C), Beers(C, A), Beers(A, Z).")
    println(cq3.getHyperGraph)
    val cq4: ConjunctiveQuery = q("Answer(z, 5) :- beers(A, B), beers(A, B, C), beers(B, C), beers(A, B), beers(C, A), beers(A, Z).")
    println(cq4.getHyperGraph)
    val cq5: ConjunctiveQuery = q("Answer(z, 5) :- beers(A, A, B, B), beers(A, B, C, C), beers(B, C), beers(A, B), beers(C, A), beers(A, Z).")
    println(cq5.getHyperGraph)
    val cq6 : ConjunctiveQuery = q("Answers(r) :- beers(C), beers(B).")

    val cqYannakakis : ConjunctiveQuery = q("Answer(z) :- beers(A, B, C, D, E, F, G, H), locations(I, B, J, K, L).")
    Yanakakis(cqYannakakis.getHyperGraph.get)
  private def read(uri: String, file_name : String): Unit =
    try {
      val allocator: BufferAllocator = new RootAllocator()
      val datasetFactory: DatasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault, FileFormat.CSV, uri+file_name)
      val dataset: Dataset = datasetFactory.finish()
      loaded_datasets += file_name.split("\\.").head -> dataset
      val scanner: Scanner = dataset.newScan(options)
      val reader: ArrowReader = scanner.scanBatches()
      var totalBatchSize: Int = 0
      while reader.loadNextBatch() do {
        val root: VectorSchemaRoot = reader.getVectorSchemaRoot
        val tbl = Table(root)
        tbl.iterator()
        totalBatchSize += root.getRowCount
        println(root.contentToTSVString())
      }

      println("Loaded: " + file_name)
//      println("Total batch size: " + totalBatchSize)
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
}