package engine.storage

import org.apache.spark.rdd.RDD

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
}

case class SparseMatrixCOO(
  entries: RDD[COOEntry],
  numRows: Int,
  numCols: Int
) extends Matrix {
  
  lazy val numNonZeros: Long = entries.count()
  
  override def toString: String = {
    f"SparseMatrixCOO($numRows x $numCols, $numNonZeros non-zeros, ${sparsity*100}%.1f%% sparse)"
  }
}

case class SparseMatrixCSR(
  rows: RDD[FormatConverter.CSRRow],
  numRows: Int,
  numCols: Int
) extends Matrix {
  
  lazy val numNonZeros: Long = rows.map(_.nnz.toLong).reduce(_ + _)
  
  override def toString: String = {
    f"SparseMatrixCSR($numRows x $numCols, $numNonZeros non-zeros, ${sparsity*100}%.1f%% sparse)"
  }
}

case class DenseMatrixRDD(
  entries: RDD[(Int, Int, Double)],
  numRows: Int,
  numCols: Int
) extends Matrix {
  
  lazy val numNonZeros: Long = entries.count()
  
  override def toString: String = {
    s"DenseMatrixRDD($numRows x $numCols, $numNonZeros entries)"
  }
}

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
  
  override def toString: String = {
    s"DenseVectorRDD(size=$size, $numNonZeros entries)"
  }
}

case class SparseVector(
  entries: RDD[SparseVectorEntry],
  size: Int
) extends Vector {
  
  lazy val numNonZeros: Long = entries.count()
  
  override def toIndexValueRDD: RDD[(Int, Double)] = {
    entries.map(e => (e.index, e.value))
  }
  
  override def toString: String = {
    f"SparseVector(size=$size, $numNonZeros non-zeros, ${sparsity*100}%.1f%% sparse)"
  }
}

case class ResultVector(
  entries: RDD[(Int, Double)],
  size: Int
) {
  def saveAsTextFile(path: String): Unit = {
    entries
      .sortByKey()
      .map { case (idx, value) => s"$idx,$value" }
      .saveAsTextFile(path)
  }

  def preview(n: Int = 10): Seq[(Int, Double)] = {
    entries.sortByKey().take(n)
  }

  def toVector: Vector = {
    DenseVectorRDD(entries, size)
  }
  
  override def toString: String = {
    s"ResultVector(size=$size)"
  }
}

case class ResultMatrix(
  entries: RDD[(Int, Int, Double)],
  numRows: Int,
  numCols: Int
) {
  def saveAsTextFile(path: String): Unit = {
    entries
      .sortBy(e => (e._1, e._2))  // Sort by row, then col
      .map { case (row, col, value) => s"$row,$col,$value" }
      .saveAsTextFile(path)
  }

  def toCOO: SparseMatrixCOO = {
    val cooEntries = entries.map { case (row, col, value) =>
      COOEntry(row, col, value)
    }
    SparseMatrixCOO(cooEntries, numRows, numCols)
  }

  def preview(n: Int = 10): Seq[(Int, Int, Double)] = {
    entries.sortBy(e => (e._1, e._2)).take(n)
  }
  
  override def toString: String = {
    s"ResultMatrix($numRows x $numCols)"
  }
}