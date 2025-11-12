package engine.operations

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import engine.storage._

object COOOperations extends MatrixOperations[RDD[COOEntry]] {
  
  def spMV(
      matrix: RDD[COOEntry],
      vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("COO Format: Sparse Matrix times Dense Vector")

    val numPartitions = matrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val matrixByCol = matrix
      .map(entry => (entry.col, (entry.row, entry.value)))
      .partitionBy(partitioner)

    val vectorPartitioned = vectorX.partitionBy(partitioner)

    val joined = matrixByCol.join(vectorPartitioned)

    val products = joined.map { case (col, ((row, matVal), vecVal)) =>
      (row, matVal * vecVal)
    }

    products.reduceByKey(_ + _)
  }

  def spMVSparse(
      matrix: RDD[COOEntry],
      vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {

    println("COO Format: Sparse Matrix times Sparse Vector")

    val numPartitions = matrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val matrixByCol = matrix
      .map(entry => (entry.col, (entry.row, entry.value)))
      .partitionBy(partitioner)

    val vectorByIndex = vectorX
      .map(entry => (entry.index, entry.value))
      .partitionBy(partitioner)

    val joined = matrixByCol.join(vectorByIndex)

    val products = joined.map { case (col, ((row, matVal), vecVal)) =>
      (row, matVal * vecVal)
    }

    products.reduceByKey(_ + _)
  }

  def spMMDense(
      matrix: RDD[COOEntry],
      matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("COO Format: Sparse Matrix times Dense Matrix")

    val numPartitions = matrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val matrixAByCol = matrix
      .map(entry => (entry.col, (entry.row, entry.value)))
      .partitionBy(partitioner)

    val matrixBByRow = matrixB
      .map { case (row, col, value) => (row, (col, value)) }
      .partitionBy(partitioner)

    val joined = matrixAByCol.join(matrixBByRow)

    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }

    val resultCells = partialProducts.reduceByKey(_ + _)

    resultCells.map { case ((i, k), value) => (i, k, value) }
  }

  def spMMSparse(
      matrixA: RDD[COOEntry],
      matrixB: RDD[COOEntry]
  ): RDD[COOEntry] = {

    println("COO Format: Sparse Matrix times Sparse Matrix")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val matrixAByCol = matrixA
      .map(entry => (entry.col, (entry.row, entry.value)))
      .partitionBy(partitioner)

    val matrixBByRow = matrixB
      .map(entry => (entry.row, (entry.col, entry.value)))
      .partitionBy(partitioner)

    val joined = matrixAByCol.join(matrixBByRow)

    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }

    val resultCells = partialProducts.reduceByKey(_ + _)

    resultCells
      .map { case ((i, k), value) => COOEntry(i, k, value) }
      .filter(!_.isZero)
  }
}