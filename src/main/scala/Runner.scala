import org.apache.arrow.dataset.file.FileFormat
import org.apache.arrow.dataset.file.FileSystemDatasetFactory
import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.arrow.dataset.scanner.Scanner
import org.apache.arrow.dataset.source.Dataset
import org.apache.arrow.dataset.source.DatasetFactory
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowReader

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
    //val cq : ConjunctiveQuery = q("Answer(z, 5) :- Beers(A, B), Location(A, C).")
    val cq : ConjunctiveQuery = q("Answer(z, 5) :- Beers(A, B), Beers(B, C), Beers(C, A), Beers(A, B, C).")
    println(cq.checkAcyclic())

  private def read(uri: String, file_name : String): Unit =
    try {
      val allocator: BufferAllocator = new RootAllocator()
      val datasetFactory: DatasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault, FileFormat.CSV, uri+file_name)
      val dataset: Dataset = datasetFactory.finish()
      loaded_datasets += file_name.split("\\.").head -> dataset
      val scanner: Scanner = dataset.newScan(options)
      val reader: ArrowReader = scanner.scanBatches()
      var totalBatchSize: Int = 0
//      while reader.loadNextBatch() do {
//        val root: VectorSchemaRoot = reader.getVectorSchemaRoot
//        totalBatchSize += root.getRowCount
//        println(root.contentToTSVString())
//      }
//
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