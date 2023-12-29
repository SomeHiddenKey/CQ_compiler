import conjunctive_querry.{Atom, ConjunctiveQuery, Constant, Head, Term, Variable}
import jointree.Node
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
  /** helper function to transform null values from empty Nullvector channels to empty strings for comparison */
  private def getOrDefault(x: AnyRef) : AnyRef =
    x match {
      case null => Text("")
      case e => e
    }

  /** filtering of dataset values based on it's variables and constant values from the [[Atom]]*/
  private def qs(a: Atom): List[List[AnyRef]] =
    //forced to transform it into a list of rows again due to the limited functionalities of Apache for scala
    var values : List[List[AnyRef]] = List[List[AnyRef]]()

    a.dataset match {
      case Some(uri) =>
        // some correctly loaded path was found
        val allocator: BufferAllocator = new RootAllocator()
        val datasetFactory: DatasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault, FileFormat.CSV, uri)
        val dataset: Dataset = datasetFactory.finish()

        // collection of index->constant-value pairs to check the constant values on those index columns
        val constants_filter = a.terms
          .zipWithIndex
          .collect{case (Constant(value),i) => (i,value)}

        // collection of variable-name->list-of-indexes to check same values for every row on those index columns
        val variable_filter = a.terms
          .zipWithIndex
          .collect{case (Variable(k),i) => (k,i)}
          .groupBy(_._1)
          .filter(_._2.length > 1) //only variables that appear at least twice

        val scanner: Scanner = dataset.newScan(ScanOptions(32768))
        val reader: ArrowReader = scanner.scanBatches()
        while reader.loadNextBatch() do {

          // mapping to get around bug in Apache where empty columns create a NullVector without allocator (which prevents Table creation
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
              // check if all index's with constant values have that value
              constants_filter.forall((i, v) => getOrDefault(e.getExtensionType(i)) == v)
              &&
              // check if all index's with the same variable name have the same value
              variable_filter.forall((_, v) => v.tail.forall((_,i) => getOrDefault(e.getExtensionType(v.head._2)) == getOrDefault(e.getExtensionType(i))))
            then
              // all atom conditions apply for given row so we can append it to the list
              var rowValues: List[AnyRef] = List[AnyRef]()
              for i <- 0 until size do
                // append all values of each column one by one to the current row
                rowValues = rowValues.appended(getOrDefault(e.getExtensionType(i)))
              values = values.appended(rowValues)
        }
    }
    values

  /**
   * returns values1 ⋉ values2
   * @param values1 list of row values for atom 1
   * @param values2 list of row values for atom 1
   * @param atom1_terms list of column names for atom 1
   * @param atom2_terms list of column names for atom 1
   * @return semi-joined output values
   */
  private def semiJoin(values1: List[List[AnyRef]], values2: List[List[AnyRef]], atom1_terms: List[Term], atom2_terms: List[Term]): List[List[AnyRef]] =
    // get term which occurs in both atoms
    val common = atom1_terms.intersect(atom2_terms)
    // create dictionary of rows in values2 (key = the element which both value-lists had in common)
    val dict2: Map[AnyRef, List[List[AnyRef]]] = values2.groupMap(_(atom2_terms.indexOf(common.head)))(identity)
    // only keep the elements which occur in the map
    values1.filter(row => {dict2.contains(row(atom1_terms.indexOf(common.head)))})

  /**
   * returns values1 ⨝ values2
   * @param values1 list of row values for atom 1
   * @param values2 list of row values for atom 2
   * @param parent_terms list of column names for atom 1
   * @param child_terms list of column names for atom 2
   * @return tuple of full-joined output values with the updated column name list
   */
  private def fullJoin(values1: List[List[AnyRef]], values2: List[List[AnyRef]], parent_terms: List[Term], child_terms: List[Term]): (List[List[AnyRef]], List[Term]) =
    val common = parent_terms.intersect(child_terms)
    // create dictionary of rows in values1 (key = the element which both value-lists had in common)
    var dict1: Map[AnyRef, List[List[AnyRef]]] = values1.groupMap(_(parent_terms.indexOf(common.head)))(identity)
    // create dictionary of rows in values2 (key = the element which both value-lists had in common)
    var dict2: Map[AnyRef, List[List[AnyRef]]] = values2.groupMap(_(child_terms.indexOf(common.head)))(identity)
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

  /**
   * returns values1 x values2
   * @param values1 list of row values for atom 1
   * @param values2 list of row values for atom 2
   * @return joined output values. You can assume that the updated column order is simply columns1 + columns2
   */
  private def cartesianJoin(values1: List[List[AnyRef]], values2: List[List[AnyRef]]): List[List[AnyRef]] =
    for {
      v1 <- values1
      v2 <- values2
    } yield v1.appendedAll(v2)

  private def QsEval(n: Node): List[List[AnyRef]] =
    // calculate qs to get all list of row values and keep that Qs in node.value
    n.value = qs(n.atom)
    // semi-join your values with all children -> Qs(D) ∶= ⋂ ( qs(D) ⋉ Qsi(D) )
    n.children.foreach(child =>
      n.value = semiJoin(n.value, QsEval(child), n.atom.terms, child.atom.terms)
    )
    n.value

  private def AsEval(n: Node): List[Term] =
    // list of node atom column names
    var parent_terms: List[Term] = n.atom.terms
    //semi-join all children with yourself -> As′(D) ∶= Qs′(D) ⋉ As(D)
    n.children.foreach(child => child.value = semiJoin(child.value, n.value, child.atom.terms, n.atom.terms))
    n.children.foreach(child => {
      // recursively do As evaluation from root to leaves
      val child_terms = AsEval(child)
      // full-join with all children's consistent database back from leave to root -> Os(D) ∶= π[s∪x] ( Os(D) ⨝ Osj(D) )
      fullJoin(n.value, child.value, parent_terms, child_terms) match {case (a,b) =>
        n.value = a
        parent_terms = b
      }
    })
    // return updated/joined column names list
    parent_terms

  /**
   * Evaluation for boolean queries
   * @param root root of hypergraph jointree
   * @return true if some satisfaction exists for the give jointree
   */
  def YannakakisEvalBoolean(root: Node): Boolean =
    QsEval(root)
    root.value.nonEmpty

  /**
   * Evaluation for non-boolean queries
   * @param root root of hypergraph jointree
   * @param head head of the query, containing the asked columns
   * @return output columns satisfying the jointree based on the head and the list of column names for that result
   */
  private def YannakakisEval(root: Node, head: Head): (List[List[AnyRef]], List[Term]) =
    QsEval(root)
    // store resulting row values in res
    val res = AsEval(root)
    val bodyIndices = head.terms.map(element => res.indexOf(element))
    // filter away the columns not present in the head. They won't be needed in other root calculations because those are independent trees
    val result = root.value.map(row => bodyIndices.collect { case i if i >= 0 && i < row.length => row(i) })
    (result, head.terms.filter(el => res.contains(el)))

  /**
   * Computer the Conjunctive Query evaluation for acyclic queries
   * @param c given query to evaluate
   * @return List of values to return, null if it's cyclic, an empty list if the boolean function returns false and a list with an empty row if the boolean function returns true
   */
  def apply(c : ConjunctiveQuery): List[List[AnyRef]] =
    c.getHyperGraph match {
      case Some(graph) =>
        if c.head.terms.isEmpty then
          // check that all independent jointrees have a satisfaction (and thus return true)
          if graph.roots.forall(root => YannakakisEvalBoolean(root)) then List[List[AnyRef]](List[AnyRef]()) else List[List[AnyRef]]()
        else
          // calculate the initial output values and list of columns for the first root
          var (output_result, columns) = YannakakisEval(graph.roots.head, c.head)
          output_result = graph.roots.tail.foldRight[List[List[AnyRef]]](output_result)((newRoot, intermediate_result) =>
            YannakakisEval(newRoot, c.head) match { case (new_result, newcolumns) =>
              // append the new columns to the existing ones
              columns = columns.appendedAll(newcolumns)
              // cartesian-join the results as they are totally independent
              cartesianJoin(intermediate_result, new_result)
            })
          // check if columns are, by chance, already in the correct order. else, project them to the correct order as per head terms order
          if columns == c.head.terms then
            output_result
          else
            projection(c.head.terms, columns, output_result)
      case None => null
    }

  /**
   * Projection of values with given columns
   * @param headTerms input column order terms
   * @param cols expected output column order terms
   * @param input_values input row values
   * @return correctly projected output row values
   */
  def projection(headTerms: List[Term], cols: List[Term], input_values: List[List[AnyRef]]): List[List[AnyRef]] =
    val bodyIndices = headTerms.map(cols.indexOf)
    input_values.map(row => bodyIndices.collect {case i if i >= 0 && i < row.length => row(i)})
}
