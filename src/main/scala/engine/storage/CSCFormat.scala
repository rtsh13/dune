package engine.storage

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

/** Compressed Sparse Column (CSC) format
  * Optimized for column-wise operations
  */
object CSCFormat {

  /** CSC Column - stores all non-zeros in a single column
    */
  case class CSCColumn(
    columnId: Int,
    values: Array[Double],
    rowIndices: Array[Int],
    numRows: Int
  ) {
    
    def getElements: Seq[(Int, Double)] = {
      rowIndices.zip(values).toSeq
    }

    def nnz: Int = values.length

    def elementsIterator: Iterator[(Int, Double)] = {
      rowIndices.iterator.zip(values.iterator)
    }

    override def toString: String = {
      s"Column $columnId: ${values.length} non-zeros"
    }
  }

  /** Convert COO format to distributed CSC format
    */
  def cooToDistributedCSC(
    cooEntries: RDD[COOEntry],
    numRows: Int,
    numCols: Int
  ): RDD[CSCColumn] = {

    println("Converting COO to Distributed CSC format...")

    // Group by column
    val groupedByColumn = cooEntries
      .map(entry => (entry.col, (entry.row, entry.value)))
      .groupByKey()

    // Convert each column to CSC format
    val cscColumns = groupedByColumn
      .map { case (columnId, rowValPairs) =>
        // Sort by row within each column
        val sorted = rowValPairs.toArray.sortBy(_._1)

        // Split into separate arrays
        val values = sorted.map(_._2)
        val rowIndices = sorted.map(_._1)

        CSCColumn(columnId, values, rowIndices, numRows)
      }
      .sortBy(_.columnId)

    cscColumns.cache()

    val totalNonZeros = cscColumns.map(_.nnz.toLong).reduce(_ + _)

    println(s"Distributed CSC created: $totalNonZeros non-zeros")

    cscColumns
  }

  /** Convert CSC back to COO format
    */
  def cscToCOO(cscColumns: RDD[CSCColumn]): RDD[COOEntry] = {
    cscColumns.flatMap { column =>
      column.elementsIterator.map { case (row, value) =>
        COOEntry(row, column.columnId, value)
      }
    }
  }
}