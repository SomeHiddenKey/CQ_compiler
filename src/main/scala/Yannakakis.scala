import org.apache.arrow.dataset.file.FileSystemDatasetFactory
import org.apache.arrow.dataset.file.FileFormat
import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.arrow.dataset.source.Dataset
import org.apache.arrow.dataset.source.DatasetFactory
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.arrow.dataset.scanner.Scanner
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.table.{Row, Table}

import scala.jdk.CollectionConverters.*
import org.apache.arrow.vector.util.Text

object Yannakakis {
  private def getOrDefault(x: AnyRef) : AnyRef =
    x match {
      case null => Text("")
      case e => e
    }

  private def qs(a: Atom): List[List[AnyRef]] =
    var values : List[List[AnyRef]] = List[List[AnyRef]]()

    a.dataset match {
      case Some(uri) =>
        val allocator: BufferAllocator = new RootAllocator()
        val datasetFactory: DatasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault, FileFormat.CSV, uri)
        val dataset: Dataset = datasetFactory.finish()
        val constants_filter = a.terms
          .zipWithIndex
          .collect{case (Constant(value),i) => (i,value)}

        val variable_filter = a.terms
          .zipWithIndex
          .collect{case (Variable(k),i) => (k,i)}
          .groupBy(_._1)
          .filter(_._2.length > 1) //only variables that appear at least twice

        val scanner: Scanner = dataset.newScan(ScanOptions(32768))
        val reader: ArrowReader = scanner.scanBatches()
        while reader.loadNextBatch() do {

          val schema : Array[FieldVector]= reader.getVectorSchemaRoot.getFieldVectors.toArray.map[FieldVector]{
            case e:org.apache.arrow.vector.NullVector => org.apache.arrow.vector.VarCharVector(e.getName, allocator)
            case e:FieldVector => e
          }
          val tbl = Table(schema.toSeq.asJava)
          val vectors = reader.getVectorSchemaRoot.getFieldVectors
          val size = vectors.size()


          for e: Row <- tbl.iterator().asScala do
            val dummy = 0
            if
              //check if all index's with constant values have that value
              (constants_filter.forall((i, v) => getOrDefault(e.getExtensionType(i)) == v)
                &&
                //check if all index's with the same variable name have the same value
                variable_filter.forall((_, v) => v.tail.forall((_,i) => getOrDefault(e.getExtensionType(v.head._2)) == getOrDefault(e.getExtensionType(i)))))
            then
              var rowValues: List[AnyRef] = List[AnyRef]()
              for i <- 0 until size do
                rowValues = rowValues.appended(getOrDefault(e.getExtensionType(i))) //append all values of each column one by one of the current row
              values = values.appended(rowValues)
        }
    }
    values

  private def semiJoin(values1: List[List[AnyRef]], values2: List[List[AnyRef]], atom1: Atom, atom2: Atom): List[List[AnyRef]] =
    // values1 ⋉ values2
    // use atom for variable name schematic
    val common = atom1.terms.intersect(atom2.terms)   //get term which occurs in both atoms
    val dict2: Map[AnyRef, List[List[AnyRef]]] = values2.groupMap(_(atom2.terms.indexOf(common.head)))(identity)  // create dictionary of rows in values2 (key = the element which both value-lists had in common)
    values1.filter(row => {dict2.contains(row(atom1.terms.indexOf(common.head)))}) // only keep the elements which occur in the map

  private def fullJoin(values1: List[List[AnyRef]], values2: List[List[AnyRef]], parent_terms: List[Term], child_terms: List[Term]): (List[List[AnyRef]], List[Term]) =
    // values1 ⨝ values2
    // use atom for variable name schematic
    val common = parent_terms.intersect(child_terms)
    var dict1: Map[AnyRef, List[List[AnyRef]]] = values1.groupMap(_(parent_terms.indexOf(common.head)))(identity) // create dictionary of rows in values1 (key = the element which both value-lists had in common)
    var dict2: Map[AnyRef, List[List[AnyRef]]] = values2.groupMap(_(child_terms.indexOf(common.head)))(identity) // create dictionary of rows in values2 (key = the element which both value-lists had in common)
    val allKeys = (dict1.keys ++ dict2.keys).toSet
    allKeys.foreach(key => {
      if !dict1.contains(key) then
        val list = List.fill(parent_terms.size)(null).updated(parent_terms.indexOf(common.head), key)
        dict1 = dict1.updated(key, List(list))
      else if !dict2.contains(key) then
        val list = List.fill(child_terms.size)(null).updated(child_terms.indexOf(common.head), key)
        dict2 = dict2.updated(key, List(list))
    })
    val res = for {
      key <- allKeys.toList
      val1 = dict1(key)
      val2 = dict2(key)
      v1 <- val1
      v2 <- val2
    } yield v1 ::: v2
    (res, parent_terms ++ child_terms)

  private def cartesianJoin(values1: List[List[AnyRef]], values2: List[List[AnyRef]]): List[List[AnyRef]] =
    for {
      v1 <- values1
      v2 <- values2
    } yield v1.appendedAll(v2)


  private def QsEval(n: Node): List[List[AnyRef]] =
    n.value = qs(n.atom) // calculate qs
    n.children.foreach(child => //for all children i
      n.value = semiJoin(n.value, QsEval(child), n.atom, child.atom) //Qs(D) ∶= ⋂ ( qs(D) ⋉ Qsi(D) )
    )
    n.value

  private def AsEval(n: Node): List[Term] =
    var parent_terms: List[Term] = n.atom.terms
    var child_terms: List[Term] = List[Term]()
    n.children.foreach(child => child.value = semiJoin(child.value, n.value, child.atom, n.atom)) //As′(D) ∶= Qs′(D) ⋉ As(D)
    n.children.foreach(child => {
      child_terms = AsEval(child) //recursively do As evaluation from root to leaves
      fullJoin(n.value, child.value, parent_terms, child_terms) match {case (a,b) => n.value = a; parent_terms = b}  // Os(D) ∶= π[s∪x] ( Os(D) ⨝ Osj(D) )
    })
    parent_terms // updated joined terms list
  /*
    private def YannakakisEval(root: Node): List[List[AnyRef]] =
      QsEval(root)
      AsEval(root)
      root.value
      */


  def YannakakisEvalBoolean(root: Node): Boolean =
    QsEval(root)
    root.value.nonEmpty
  /*
    def apply(c : ConjunctiveQuery): List[List[AnyRef]] =
      c.getHyperGraph match {
        case Some(graph) =>
          if c.head.terms.isEmpty then
            if graph.roots.forall(root => YannakakisEvalBoolean(root)) then List[List[AnyRef]](List[AnyRef]()) else List[List[AnyRef]]()
          else
            val res = graph.roots.tail.foldRight[List[List[AnyRef]]](YannakakisEval(graph.roots.head))((newRoot, values) => cartesianJoin(YannakakisEval(newRoot), values))
        //    println("res:" + graph.roots.head.atom)
            projection(c, res)
           // res
        case None => null
      }
   */

  private def YannakakisEval(root: Node, head: Head): (List[List[AnyRef]], List[Term]) =
    QsEval(root)
    val res = AsEval(root)
    val bodyIndices = head.terms.map(element => res.indexOf(element))
    val result = root.value.map(row => bodyIndices.collect { case i if i >= 0 && i < row.length => row(i) })
    (result, head.terms)

  def apply(c : ConjunctiveQuery): List[List[AnyRef]] =
    c.getHyperGraph match {
      case Some(graph) =>
        if c.head.terms.isEmpty then
          if graph.roots.forall(root => YannakakisEvalBoolean(root)) then List[List[AnyRef]](List[AnyRef]()) else List[List[AnyRef]]()
        else
          var (output_result, columns) = YannakakisEval(graph.roots.head, c.head)
          output_result = graph.roots.tail.foldRight[List[List[AnyRef]]](output_result)((newRoot, intermediate_result) =>
            YannakakisEval(newRoot, c.head) match { case (new_result, newcolumns) =>
              columns = columns.appendedAll(newcolumns)
              cartesianJoin(intermediate_result, new_result)
            })
         /* if columns == c.head.terms then
            output_result
          else*/
            projection(c.head.terms, columns, output_result)
      case None => null
    }

  def projection(headTerms: List[Term], cols: List[Term], res: List[List[AnyRef]]): List[List[AnyRef]] =
    println(headTerms == cols)
    println("headterms: " + headTerms)
    println("cols: " + cols)
    println("res: " + res)
    //   println("wanted: " + wanted)
    //val bodyList = cols.flatMap(el => el.terms)
    //   println("bodyList: " + bodyList)
    //val bodyIndices = headTerms.map(element => bodyList.indexOf(element))
    //   println("bodyIndices: " + bodyIndices)
    //   println(res)
    //res.map(row => bodyIndices.collect {case i if i >= 0 && i < row.length => row(i)})
    null


  //in case we have multiple roots, we just full cartesian join everything given that the graphs are fully independent anyways
}
