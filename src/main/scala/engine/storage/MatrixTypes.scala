package engine.storage

import org.apache.spark.rdd.RDD
import engine.operations.MultiplicationOps

// MATRIX TRAIT

sealed trait Matrix {
  def numRows: Int
  def numCols: Int
  def numNonZeros: Long
  
  def sparsity: Double = {
    val total = numRows.toLong * numCols
    1.0 - (numNonZeros.toDouble / total)
  }
  
  def isSparse: Boolean = sparsity > 0.90
  def isDense: Boolean = !isSparse
  
  def multiply(vector: Vector): ResultVector
  def multiply(matrix: Matrix): ResultMatrix
  def add(other: Matrix): ResultMatrix
  def transpose: Matrix
  def toCOO: SparseMatrixCOO
  
  def *(vector: Vector): ResultVector = multiply(vector)
  def *(matrix: Matrix): ResultMatrix = multiply(matrix)
  def +(other: Matrix): ResultMatrix = add(other)
}

// SPARSE MATRIX COO

case class SparseMatrixCOO(
  entries: RDD[COOEntry],
  numRows: Int,
  numCols: Int
) extends Matrix {
  
  lazy val numNonZeros: Long = entries.count()
  
  override def toString: String = {
    f"SparseMatrixCOO($numRows x $numCols, $numNonZeros non-zeros, ${sparsity*100}%.1f%% sparse)"
  }
  
  override def multiply(vector: Vector): ResultVector = {
    vector match {
      case denseVec: DenseVectorRDD =>
        val resultRDD = MultiplicationOps.sparseMatrixDenseVector(
          this.entries,
          denseVec.entries
        )
        ResultVector(resultRDD, this.numRows)
      
      case sparseVec: SparseVector =>
        val resultRDD = MultiplicationOps.sparseMatrixSparseVector(
          this.entries,
          sparseVec.entries
        )
        ResultVector(resultRDD, this.numRows)
    }
  }
  
  override def multiply(matrix: Matrix): ResultMatrix = {
    matrix match {
      case denseMat: DenseMatrixRDD =>
        val resultRDD = MultiplicationOps.sparseMatrixDenseMatrix(
          this.entries,
          denseMat.entries
        )
        ResultMatrix(resultRDD, this.numRows, matrix.numCols)
      
      case sparseMat: SparseMatrixCOO =>
        val resultCOO = MultiplicationOps.sparseMatrixSparseMatrix(
          this.entries,
          sparseMat.entries
        )
        ResultMatrix(
          resultCOO.map(e => (e.row, e.col, e.value)),
          this.numRows,
          matrix.numCols
        )
      
      case csrMat: SparseMatrixCSR =>
        this.multiply(csrMat.toCOO)
    }
  }
  
  override def add(other: Matrix): ResultMatrix = {
    val otherCOO = other.toCOO
    val combined = this.entries.union(otherCOO.entries)
    val summed = combined
      .map(e => ((e.row, e.col), e.value))
      .reduceByKey(_ + _)
      .map { case ((row, col), value) => (row, col, value) }
    ResultMatrix(summed, this.numRows, this.numCols)
  }
  
  override def transpose: Matrix = {
    val transposedEntries = this.entries.map(e => COOEntry(e.col, e.row, e.value))
    SparseMatrixCOO(transposedEntries, this.numCols, this.numRows)
  }
  
  override def toCOO: SparseMatrixCOO = this
  
  def toCSR: SparseMatrixCSR = {
    val csrRows = FormatConverter.cooToDistributedCSR(this.entries, this.numRows, this.numCols)
    SparseMatrixCSR(csrRows, this.numRows, this.numCols)
  }
  
  def slice(rowStart: Int, rowEnd: Int, colStart: Int, colEnd: Int): SparseMatrixCOO = {
    val slicedEntries = this.entries.filter { entry =>
      entry.row >= rowStart && entry.row < rowEnd &&
      entry.col >= colStart && entry.col < colEnd
    }.map { entry =>
      COOEntry(entry.row - rowStart, entry.col - colStart, entry.value)
    }
    SparseMatrixCOO(slicedEntries, rowEnd - rowStart, colEnd - colStart)
  }
}

// SPARSE MATRIX CSR

case class SparseMatrixCSR(
  rows: RDD[FormatConverter.CSRRow],
  numRows: Int,
  numCols: Int
) extends Matrix {
  
  lazy val numNonZeros: Long = rows.map(_.nnz.toLong).reduce(_ + _)
  
  override def toString: String = {
    f"SparseMatrixCSR($numRows x $numCols, $numNonZeros non-zeros, ${sparsity*100}%.1f%% sparse)"
  }
  
  override def multiply(vector: Vector): ResultVector = {
    val resultRDD = MultiplicationOps.csrMatrixDenseVector(
      this.rows,
      vector.toIndexValueRDD
    )
    ResultVector(resultRDD, this.numRows)
  }
  
  override def multiply(matrix: Matrix): ResultMatrix = {
    this.toCOO.multiply(matrix)
  }
  
  override def add(other: Matrix): ResultMatrix = {
    this.toCOO.add(other)
  }
  
  override def transpose: Matrix = {
    this.toCOO.transpose
  }
  
  override def toCOO: SparseMatrixCOO = {
    val cooEntries = FormatConverter.distributedCSRToCOO(this.rows)
    SparseMatrixCOO(cooEntries, this.numRows, this.numCols)
  }
}

// DENSE MATRIX RDD

case class DenseMatrixRDD(
  entries: RDD[(Int, Int, Double)],
  numRows: Int,
  numCols: Int
) extends Matrix {
  
  lazy val numNonZeros: Long = entries.count()
  
  override def toString: String = {
    s"DenseMatrixRDD($numRows x $numCols, $numNonZeros entries)"
  }
  
  override def multiply(vector: Vector): ResultVector = {
    val matrixByCol = this.entries.map { case (row, col, value) =>
      (col, (row, value))
    }
    val vectorRDD = vector.toIndexValueRDD
    val joined = matrixByCol.join(vectorRDD)
    val result = joined
      .map { case (col, ((row, mVal), vVal)) => (row, mVal * vVal) }
      .reduceByKey(_ + _)
    ResultVector(result, this.numRows)
  }
  
  override def multiply(matrix: Matrix): ResultMatrix = {
    matrix match {
      case denseMat: DenseMatrixRDD =>
        val matrixAByCol = this.entries.map { case (r, c, v) => (c, (r, v)) }
        val matrixBByRow = denseMat.entries.map { case (r, c, v) => (r, (c, v)) }
        val joined = matrixAByCol.join(matrixBByRow)
        val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
          ((i, k), vA * vB)
        }
        val resultCells = partialProducts.reduceByKey(_ + _)
        val result = resultCells.map { case ((i, k), v) => (i, k, v) }
        ResultMatrix(result, this.numRows, matrix.numCols)
      
      case sparseMat: SparseMatrixCOO =>
        val matrixAByCol = this.entries.map { case (r, c, v) => (c, (r, v)) }
        val matrixBByRow = sparseMat.entries.map(e => (e.row, (e.col, e.value)))
        val joined = matrixAByCol.join(matrixBByRow)
        val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
          ((i, k), vA * vB)
        }
        val resultCells = partialProducts.reduceByKey(_ + _)
        val result = resultCells.map { case ((i, k), v) => (i, k, v) }
        ResultMatrix(result, this.numRows, matrix.numCols)
      
      case csrMat: SparseMatrixCSR =>
        this.multiply(csrMat.toCOO)
    }
  }
  
  override def add(other: Matrix): ResultMatrix = {
    val otherEntries = other match {
      case dense: DenseMatrixRDD => dense.entries
      case coo: SparseMatrixCOO => coo.entries.map(e => (e.row, e.col, e.value))
      case csr: SparseMatrixCSR => csr.toCOO.entries.map(e => (e.row, e.col, e.value))
    }
    val combined = this.entries.union(otherEntries)
    val summed = combined
      .map { case (row, col, value) => ((row, col), value) }
      .reduceByKey(_ + _)
      .map { case ((row, col), value) => (row, col, value) }
    ResultMatrix(summed, this.numRows, this.numCols)
  }
  
  override def transpose: Matrix = {
    val transposedEntries = this.entries.map { case (row, col, value) =>
      (col, row, value)
    }
    DenseMatrixRDD(transposedEntries, this.numCols, this.numRows)
  }
  
  override def toCOO: SparseMatrixCOO = {
    val cooEntries = this.entries
      .map { case (row, col, value) => COOEntry(row, col, value) }
      .filter(!_.isZero)
    SparseMatrixCOO(cooEntries, this.numRows, this.numCols)
  }
}

// VECTORS

sealed trait Vector {
  def size: Int
  def numNonZeros: Long
  def sparsity: Double = 1.0 - (numNonZeros.toDouble / size)
  def isSparse: Boolean = sparsity > 0.90
  def isDense: Boolean = !isSparse
  def toIndexValueRDD: RDD[(Int, Double)]
}

case class DenseVectorRDD(
  entries: RDD[(Int, Double)],
  size: Int
) extends Vector {
  lazy val numNonZeros: Long = entries.count()
  override def toIndexValueRDD: RDD[(Int, Double)] = entries
  override def toString: String = s"DenseVectorRDD(size=$size, $numNonZeros entries)"
}

case class SparseVector(
  entries: RDD[SparseVectorEntry],
  size: Int
) extends Vector {
  lazy val numNonZeros: Long = entries.count()
  override def toIndexValueRDD: RDD[(Int, Double)] = entries.map(e => (e.index, e.value))
  override def toString: String = f"SparseVector(size=$size, $numNonZeros non-zeros, ${sparsity*100}%.1f%% sparse)"
}

// RESULTS

case class ResultVector(
  entries: RDD[(Int, Double)],
  size: Int
) {
  def saveAsTextFile(path: String): Unit = {
    entries.sortByKey().map { case (idx, value) => s"$idx,$value" }.saveAsTextFile(path)
  }
  def preview(n: Int = 10): Seq[(Int, Double)] = entries.sortByKey().take(n)
  def toVector: Vector = DenseVectorRDD(entries, size)
  override def toString: String = s"ResultVector(size=$size)"
}

case class ResultMatrix(
  entries: RDD[(Int, Int, Double)],
  numRows: Int,
  numCols: Int
) {
  def saveAsTextFile(path: String): Unit = {
    entries.sortBy(e => (e._1, e._2)).map { case (row, col, value) => s"$row,$col,$value" }.saveAsTextFile(path)
  }
  def toCOO: SparseMatrixCOO = {
    val cooEntries = entries.map { case (row, col, value) => COOEntry(row, col, value) }
    SparseMatrixCOO(cooEntries, numRows, numCols)
  }
  def preview(n: Int = 10): Seq[(Int, Int, Double)] = entries.sortBy(e => (e._1, e._2)).take(n)
  override def toString: String = s"ResultMatrix($numRows x $numCols)"
}