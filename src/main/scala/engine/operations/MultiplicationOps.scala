package engine.operations

import org.apache.spark.rdd.RDD
import engine.storage._

object MultiplicationOps {
  def sparseMatrixDenseVector(
      matrixA: RDD[COOEntry],
      vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {
    val matrixByCol =
      matrixA.map(entry => (entry.col, (entry.row, entry.value)))

    val joined = matrixByCol.join(vectorX)

    val products = joined.map { case (col, ((row, matVal), vecVal)) =>
      (row, matVal * vecVal)
    }

    products.reduceByKey(_ + _)
  }

  def sparseMatrixSparseVector(
      matrixA: RDD[COOEntry],
      vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {
    val matrixByCol =
      matrixA.map(entry => (entry.col, (entry.row, entry.value)))

    val vectorByIndex = vectorX.map(entry => (entry.index, entry.value))

    val joined = matrixByCol.join(vectorByIndex)

    val products = joined.map { case (col, ((row, matVal), vecVal)) =>
      (row, matVal * vecVal)
    }

    products.reduceByKey(_ + _)
  }

  def sparseMatrixDenseMatrix(
      matrixA: RDD[COOEntry],
      matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {
    val matrixAByCol =
      matrixA.map(entry => (entry.col, (entry.row, entry.value)))

    val matrixBByRow = matrixB.map { case (row, col, value) =>
      (row, (col, value))
    }

    val joined = matrixAByCol.join(matrixBByRow)

    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }

    val resultCells = partialProducts.reduceByKey(_ + _)

    resultCells.map { case ((i, k), value) => (i, k, value) }
  }

  def sparseMatrixSparseMatrix(
      matrixA: RDD[COOEntry],
      matrixB: RDD[COOEntry]
  ): RDD[COOEntry] = {
    val matrixAByCol =
      matrixA.map(entry => (entry.col, (entry.row, entry.value)))
    val matrixBByRow =
      matrixB.map(entry => (entry.row, (entry.col, entry.value)))
    val joined = matrixAByCol.join(matrixBByRow)

    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }

    val resultCells = partialProducts.reduceByKey(_ + _)

    resultCells
      .map { case ((i, k), value) => COOEntry(i, k, value) }
      .filter(!_.isZero)
  }

  def csrMatrixDenseVector(
      csrMatrix: RDD[FormatConverter.CSRRow],
      vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("Computing CSR Matrix * Dense Vector (baseline)...")

    CSROperations.spMV(csrMatrix, vectorX)
  }

  def displayStats(
      operationName: String,
      result: RDD[(Int, Double)]
  ): Unit = {
    val count = result.count()
    val sum = result.map(_._2).reduce(_ + _)
    val maxEntry = result.reduce { (a, b) =>
      if (math.abs(a._2) > math.abs(b._2)) a else b
    }

    println(s"\n$operationName Results:")
    println(f"Non-zero entries: $count")
    println(f"Sum of values: $sum%.6f")
    println(f"Max absolute value: ${maxEntry._1} -> ${maxEntry._2}%.6f")
  }
}
