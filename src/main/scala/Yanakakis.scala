import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.arrow.dataset.scanner.Scanner
import org.apache.arrow.dataset.source.Dataset
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.table.{Row, Table}

import scala.jdk.CollectionConverters.*

object Yanakakis {
  def qs(a: Atom): List[List[AnyRef]] =
    var values : List[List[AnyRef]] = List[List[AnyRef]]()
    a.dataset match {
      case Some(ds) =>
        val constants_filter = a.terms
          .zipWithIndex
          .collect{case (Constant(value),i) => (i,value)}

        val variable_filter = a.terms
          .zipWithIndex
          .collect{case (Variable(k),i) => (k,i)}
          .groupBy(_._1)
          .filter(_._2.length > 1) //only variables that appear at least twice

        val scanner1: Scanner = ds.newScan(ScanOptions(32768))
        val reader1: ArrowReader = scanner1.scanBatches()
        while reader1.loadNextBatch() do {
          val tbl = Table(reader1.getVectorSchemaRoot)
          val vectors = reader1.getVectorSchemaRoot.getFieldVectors
          val size = vectors.size()

          for e: Row <- tbl.iterator().asScala do
            if
              //check if all index's with constant values have that value
              (constants_filter.forall((i, v) => e.getExtensionType(i) == v)
                &&
              //check if all index's with the same variable name have the same value
              variable_filter.forall((_, v) => v.tail.forall((_,i) => e.getExtensionType(v.head._2) == e.getExtensionType(i))))
            then
              var rowValues: List[AnyRef] = List[AnyRef]()
              for i <- 0 until size do
                rowValues = rowValues.appended(e.getExtensionType(i)) //append all values of each column one by one of the current row
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

  private def fulljoin(values1: List[List[AnyRef]], values2: List[List[AnyRef]], atom1: Atom, atom2: Atom): List[List[AnyRef]] =
    // values1 ⨝ values2
    // use atom for variable name schematic
    throw NotImplementedError()

  private def cartesianjoin(values1: List[List[AnyRef]], values2: List[List[AnyRef]]): List[List[AnyRef]] =
    for {
      v1 <- values1
      v2 <- values2
    } yield v1.appendedAll(v2)


  def QsEval(n: Node): List[List[AnyRef]] =
    n.value = qs(n.atom) // calculate qs
    println("start n.children")
    println(n.children.toString)
    println("end n.children")
    n.children.foreach(child => //for all children i
      n.value = semiJoin(n.value, QsEval(child), n.atom, child.atom) //Qs(D) ∶= ⋂ ( qs(D) ⋉ Qsi(D) )
    )
    n.value

  private def AsEval(n: Node): List[List[AnyRef]] =
    n.children.foreach(child => child.value = semiJoin(child.value, n.value, child.atom, n.atom)) //As′(D) ∶= Qs′(D) ⋉ As(D)
    n.children.foreach(child => AsEval(child)) //recursively do As evalution from root to leaves
    n.children.foreach(child => n.value = fulljoin(n.value, child.value, n.atom, child.atom)) // Os(D) ∶= π[s∪x] ( Os(D) ⨝ Osj(D) )
    n.value //answer Or(D)

  private def YanakakisEval(root: Node): List[List[AnyRef]] =
    QsEval(root)
    AsEval(root)

  def apply(graph: Hypergraph): List[List[AnyRef]] =
    graph.roots.tail.foldRight[List[List[AnyRef]]](YanakakisEval(graph.roots.head))((newRoot, values) => cartesianjoin(YanakakisEval(newRoot), values))
  //in case we have multiple roots, we just full cartesian join everything given that the graphs are fully independant anyways
}
