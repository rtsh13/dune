package engine.storage

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SmartLoader {

  val SPARSITY_THRESHOLD = 0.90

  def loadMatrix(
      sc: SparkContext,
      filepath: String,
      forceSparse: Boolean = false,
      forceDense: Boolean = false
  ): Matrix = {

    println(s"Loading matrix from: $filepath")

    val cooEntries = COOLoader.loadSparseMatrix(sc, filepath)
    val (numRows, numCols) = COOLoader.getMatrixDimensions(cooEntries)
    val numNonZeros = cooEntries.count() // Distributed count

    val totalElements = numRows.toLong * numCols
    val sparsity = 1.0 - (numNonZeros.toDouble / totalElements)

    println(f"Matrix: $numRows x $numCols")
    println(f"Non-zeros: $numNonZeros")
    println(f"Sparsity: ${sparsity * 100}%.2f%%")

    val useSparse = if (forceSparse) {
      println("Using SPARSE representation (forced)")
      true
    } else if (forceDense) {
      println("Using DENSE representation (forced)")
      false
    } else if (sparsity > SPARSITY_THRESHOLD) {
      println("Using SPARSE representation (auto-detected)")
      true
    } else {
      println("Using DENSE representation (auto-detected)")
      false
    }

    if (useSparse) {
      SparseMatrixCOO(cooEntries, numRows, numCols)
    } else {
      DenseMatrixRDD(
        cooEntries.map(e => (e.row, e.col, e.value)),
        numRows,
        numCols
      )
    }
  }

  def loadVector(
      sc: SparkContext,
      filepath: String,
      forceSparse: Boolean = false,
      forceDense: Boolean = false
  ): Vector = {

    println(s"Loading vector from: $filepath")

    val parsedEntries = COOLoader.loadDenseVectorRDD(sc, filepath)
    parsedEntries.cache()

    val numEntries = parsedEntries.count()
    val size = COOLoader.getVectorSize(parsedEntries)

    val sparsity = 1.0 - (numEntries.toDouble / size)

    println(f"Vector size: $size")
    println(f"Non-zeros: $numEntries")
    println(f"Sparsity: ${sparsity * 100}%.2f%%")

    val useSparse = if (forceSparse) {
      println("Using SPARSE vector (forced)")
      true
    } else if (forceDense) {
      println("Using DENSE vector (forced)")
      false
    } else if (sparsity > SPARSITY_THRESHOLD) {
      println("Using SPARSE vector (auto-detected)")
      true
    } else {
      println("Using DENSE vector (auto-detected)")
      false
    }

    if (useSparse) {
      val entries = parsedEntries.map { case (idx, value) =>
        SparseVectorEntry(idx, value)
      }
      SparseVector(entries, size)
    } else {
      DenseVectorRDD(parsedEntries, size)
    }
  }

  def toCSR(matrix: Matrix, sc: SparkContext): SparseMatrixCSR = matrix match {

    case SparseMatrixCOO(entries, numRows, numCols) =>
      val csrRows =
        FormatConverter.cooToDistributedCSR(entries, numRows, numCols)
      SparseMatrixCSR(csrRows, numRows, numCols)

    case SparseMatrixCSR(rows, numRows, numCols) =>
      SparseMatrixCSR(rows, numRows, numCols) // Already CSR

    case DenseMatrixRDD(entries, numRows, numCols) =>
      val cooEntries = FormatConverter.denseRDDToCOO(entries)
      val csrRows =
        FormatConverter.cooToDistributedCSR(cooEntries, numRows, numCols)
      SparseMatrixCSR(csrRows, numRows, numCols)
  }

  def toCOO(matrix: Matrix): SparseMatrixCOO = matrix match {

    case coo: SparseMatrixCOO =>
      coo

    case SparseMatrixCSR(rows, numRows, numCols) =>
      val cooEntries = FormatConverter.distributedCSRToCOO(rows)
      SparseMatrixCOO(cooEntries, numRows, numCols)

    case DenseMatrixRDD(entries, numRows, numCols) =>
      val cooEntries = FormatConverter.denseRDDToCOO(entries)
      SparseMatrixCOO(cooEntries, numRows, numCols)
  }
}
