package engine.operations

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import engine.storage._
import engine.optimization.AdaptiveOps

object MultiplicationOps {

  /** Sparse Matrix × Dense Vector: y = A.x
   */
  def sparseMatrixDenseVector(
      matrixA: RDD[COOEntry],
      vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("Computing Sparse Matrix x Dense Vector...")

    // Use adaptive strategy - NO COLLECT
    AdaptiveOps.adaptiveSpMV(matrixA, vectorX)
  }

  /** Sparse Matrix × Sparse Vector: y = A × x
   */
  def sparseMatrixSparseVector(
      matrixA: RDD[COOEntry],
      vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {

    println("Computing Sparse Matrix x Sparse Vector...")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    // Partition matrix by column
    val matrixByCol = matrixA
      .map(entry => (entry.col, (entry.row, entry.value)))
      .partitionBy(partitioner)

    // Partition vector by index
    val vectorByIndex = vectorX
      .map(entry => (entry.index, entry.value))
      .partitionBy(partitioner)

    // Join - co-located
    val joined = matrixByCol.join(vectorByIndex)

    // Compute products
    val products = joined.map { case (col, ((row, matVal), vecVal)) =>
      (row, matVal * vecVal)
    }

    // Aggregate
    val result = products.reduceByKey(_ + _)

    println(s"Sparse SpMV complete. Result has ${result.count()} non-zero entries.")

    result
  }

  /** Sparse Matrix × Dense Matrix: C = A × B
   */
  def sparseMatrixDenseMatrix(
      matrixA: RDD[COOEntry],
      matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("Computing Sparse Matrix x Dense Matrix...")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    // Partition A by column (inner dimension)
    val matrixAByCol = matrixA
      .map(entry => (entry.col, (entry.row, entry.value)))
      .partitionBy(partitioner)

    // Partition B by row (inner dimension)
    val matrixBByRow = matrixB
      .map { case (row, col, value) => (row, (col, value)) }
      .partitionBy(partitioner)

    // Join on inner dimension
    val joined = matrixAByCol.join(matrixBByRow)

    // Compute partial products
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }

    // Aggregate
    val resultCells = partialProducts.reduceByKey(_ + _)

    // Convert to result format
    val result = resultCells.map { case ((i, k), value) =>
      (i, k, value)
    }

    println(s"SpMM (dense) complete. Result has ${result.count()} entries.")

    result
  }

  /** Sparse Matrix × Sparse Matrix: C = A × B
   */
  def sparseMatrixSparseMatrix(
      matrixA: RDD[COOEntry],
      matrixB: RDD[COOEntry]
  ): RDD[COOEntry] = {

    println("Computing Sparse Matrix x Sparse Matrix...")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    // Partition by inner dimension
    val matrixAByCol = matrixA
      .map(entry => (entry.col, (entry.row, entry.value)))
      .partitionBy(partitioner)

    val matrixBByRow = matrixB
      .map(entry => (entry.row, (entry.col, entry.value)))
      .partitionBy(partitioner)

    // Join
    val joined = matrixAByCol.join(matrixBByRow)

    // Compute products
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }

    // Aggregate
    val resultCells = partialProducts.reduceByKey(_ + _)

    // Convert to COOEntry and filter zeros
    val result = resultCells
      .map { case ((i, k), value) => COOEntry(i, k, value) }
      .filter(!_.isZero)

    println(s"SpMM (sparse) complete. Result has ${result.count()} non-zero entries.")

    result
  }

  /** Sparse Matrix (CSR) × Dense Vector: y = A × x
   */
  def csrMatrixDenseVector(
      csrMatrix: RDD[FormatConverter.CSRRow],
      vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("Computing CSR Matrix x Dense Vector...")

    // Use optimized CSR method from AdaptiveOps
    AdaptiveOps.csrSpMV(csrMatrix, vectorX)
  }

  /** Compute and display statistics about an operation
   * Uses distributed aggregations - NO collect()
   */
  def displayStats(
      operationName: String,
      result: RDD[(Int, Double)]
  ): Unit = {
    val count = result.count()
    val sum = result.map(_._2).reduce(_ + _)
    val maxEntry = result.reduce { (a, b) =>
      if (math.abs(a._2) > math.abs(b._2)) a else b
    }

    println(s"\n=== $operationName Statistics ===")
    println(f"Non-zero entries: $count")
    println(f"Sum of values: $sum%.6f")
    println(f"Max absolute value: ${maxEntry._1} -> ${maxEntry._2}%.6f")
  }
}