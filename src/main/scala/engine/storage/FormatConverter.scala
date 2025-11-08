package engine.storage

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object FormatConverter {

  case class CSRRow(
      rowId: Int,
      values: Array[Double],
      colIndices: Array[Int],
      numCols: Int
  ) {
    def getElements: Seq[(Int, Double)] = {
      colIndices.zip(values).toSeq
    }

    // Number of non-zeros in this row
    def nnz: Int = values.length

    def elementsIterator: Iterator[(Int, Double)] = {
      colIndices.iterator.zip(values.iterator)
    }

    override def toString: String = {
      s"Row $rowId: ${values.length} non-zeros"
    }
  }

  def cooToDistributedCSR(
      cooEntries: RDD[COOEntry],
      numRows: Int,
      numCols: Int
  ): RDD[CSRRow] = {

    println("Converting COO to Distributed CSR...")

    // Group by row - this is a SHUFFLE but stays distributed
    val groupedByRow = cooEntries
      .map(entry => (entry.row, (entry.col, entry.value)))
      .groupByKey()

    // Convert each row to CSR format
    val csrRows = groupedByRow
      .map { case (rowId, colValPairs) =>
        // Sort by column within each row
        val sorted = colValPairs.toArray.sortBy(_._1)

        // Split into separate arrays
        val values = sorted.map(_._2)
        val colIndices = sorted.map(_._1)

        CSRRow(rowId, values, colIndices, numCols)
      }
      .sortBy(_.rowId)

    csrRows.cache()

    val totalNonZeros = csrRows
      .map(_.nnz.toLong)
      .reduce(_ + _)

    println(s"Distributed CSR: $totalNonZeros non-zeros")

    csrRows
  }

  def csrToFlatRDD(csrRows: RDD[CSRRow]): RDD[(Int, (Int, Double))] = {
    csrRows.flatMap { row =>
      row.elementsIterator.map { case (col, value) =>
        (row.rowId, (col, value))
      }
    }
  }

  def csrToColumnKeyedRDD(csrRows: RDD[CSRRow]): RDD[(Int, (Int, Double))] = {
    csrRows.flatMap { row =>
      row.elementsIterator.map { case (col, value) =>
        (col, (row.rowId, value))
      }
    }
  }

  def distributedCSRToCOO(csrRows: RDD[CSRRow]): RDD[COOEntry] = {
    csrRows.flatMap { row =>
      row.elementsIterator.map { case (col, value) =>
        COOEntry(row.rowId, col, value)
      }
    }
  }

  def denseRDDToCOO(
      entries: RDD[(Int, Int, Double)]
  ): RDD[COOEntry] = {
    entries
      .map { case (row, col, value) => COOEntry(row, col, value) }
      .filter(!_.isZero)
  }

  def cooToDenseRDD(
      cooEntries: RDD[COOEntry],
      numRows: Int,
      numCols: Int,
      sc: SparkContext
  ): RDD[(Int, Int, Double)] = {

    // For dense representation, we need all (row, col) pairs
    // Generate all possible (row, col) coordinates
    val allCoordinates = sc.parallelize(
      for {
        row <- 0 until numRows
        col <- 0 until numCols
      } yield (row, col)
    )

    // Create map of existing values
    val existingValues = cooEntries
      .map(e => ((e.row, e.col), e.value))

    // Left outer join to fill in zeros
    allCoordinates
      .map(coord => (coord, 0.0))
      .leftOuterJoin(existingValues)
      .map { case ((row, col), (_, valueOpt)) =>
        (row, col, valueOpt.getOrElse(0.0))
      }
  }

  def cooToRowKeyedRDD(cooEntries: RDD[COOEntry]): RDD[(Int, (Int, Double))] = {
    cooEntries.map(e => (e.row, (e.col, e.value)))
  }

  def cooToColumnKeyedRDD(
      cooEntries: RDD[COOEntry]
  ): RDD[(Int, (Int, Double))] = {
    cooEntries.map(e => (e.col, (e.row, e.value)))
  }
}
