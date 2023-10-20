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

import java.io.File

var options : ScanOptions = new ScanOptions(/*batchSize*/ 32768)

@main private def main() : Unit =
  val files = listFilesInDirectory(System.getProperty("user.dir") + s"/data/")
  for (file <- files)
    read("file:///" + System.getProperty("user.dir") + s"/data/$file")


private def read(uri: String) : Unit =
  try {
    val allocator: BufferAllocator = new RootAllocator()
    val datasetFactory: DatasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault, FileFormat.CSV, uri)
    val dataset: Dataset = datasetFactory.finish()
    val scanner: Scanner = dataset.newScan(options)
    val reader: ArrowReader = scanner.scanBatches()
    var totalBatchSize: Int = 0
    while reader.loadNextBatch() do {
      val root: VectorSchemaRoot = reader.getVectorSchemaRoot
      totalBatchSize += root.getRowCount
      println(root.contentToTSVString())
    }

    println("Total batch size: " + totalBatchSize)
  }
  catch case e : Exception => e.printStackTrace()

def listFilesInDirectory(directoryPath: String): List[String] = {
  val directory = new File(directoryPath)
  if (directory.exists && directory.isDirectory) {
    directory.listFiles.filter(_.isFile).map(_.getName).toList
  } else {
    List.empty
  }
}