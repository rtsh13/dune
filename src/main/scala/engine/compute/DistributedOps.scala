package engine.compute

import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import engine.storage._
import scala.collection.Map
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel

/**
 * Contains all core distributed algebra operations.
 */
object DistributedOps {

  /**
   * Performs Sparse Matrix-Dense Vector Multiplication (SpMV) using a broadcast join.
   *
   * This is the y = A * x operation.
   * @param matrixA The sparse matrix A, as an RDD of COOEntry(row, col, value).
   * @param vectorX_bcast The dense vector x, broadcast as a Map[Int, Double].
   * @return The resulting dense vector y, as an RDD[(Int, Double)] of (row, value).
   */
  def spmv_broadcast(
    matrixA: RDD[COOEntry],
    vectorX_bcast: Broadcast[Map[Int, Double]]
  ): RDD[(Int, Double)] = {

    val partialProducts: RDD[(Int, Double)] = matrixA.flatMap {
      case COOEntry(row, col, value) =>
        
        // Access the broadcasted map
        val vectorMap = vectorX_bcast.value
        
        // Get the corresponding vector value
        val vectorValue = vectorMap.getOrElse(col, 0.0)

        if (vectorValue == 0.0) {
          // Optimization: if vector value is 0, product is 0.
          None
        } else {
          // Emit the partial product, keyed by row
          Some((row, value * vectorValue))
        }
    }

    // Aggregate all partial products by row
    val resultVector: RDD[(Int, Double)] = partialProducts
      .reduceByKey(_ + _)

    resultVector
  }

  /**
   * Performs Sparse Matrix-Sparse Vector Multiplication (SpMV) using an RDD join.
   *
   * This is the y = A * x operation, where both A and x are sparse and distributed.
   * @param matrixA The sparse matrix A, as an RDD[COOEntry(row, col, value)].
   * @param vectorX The sparse vector x, as an RDD[SparseVectorEntry(index, value)].
   * @return The resulting (potentially) sparse vector y, as an RDD[(Int, Double)] of (row, value).
   */
  def spmv_sparse(
    matrixA: RDD[COOEntry],
    vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {

    // 1. Re-key the matrix RDD: (col -> (row, value))
    val matrixByCol = matrixA.map {
      case COOEntry(row, col, value) => (col, (row, value))
    }

    // 2. Re-key the vector RDD: (index -> value)
    val vectorByIndex = vectorX.map {
      case SparseVectorEntry(index, value) => (index, value)
    }

    // 3. OPTIMIZATION: Create a partitioner and apply it to both RDDs
    val numPartitions = matrixA.getNumPartitions
    val partitioner = new HashPartitioner(numPartitions)

    val matrixPartitioned = matrixByCol
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK) // Persist for join

    val vectorPartitioned = vectorByIndex
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK) // Persist for join

    // 4. Join the co-located RDDs.
    //    (j, ((i, v_A), v_x))
    val joined = matrixPartitioned.join(vectorPartitioned)

    // 5. Compute partial products.
    //    (row, partial_product)
    val partialProducts = joined.map {
      case (col_j, ((row_i, matrix_val_A), vector_val_x)) =>
        (row_i, matrix_val_A * vector_val_x)
    }

    // 6. Aggregate partial products by row
    val resultVector = partialProducts
      .reduceByKey(_ + _)

    // 7. Clean up persisted RDDs
    matrixPartitioned.unpersist()
    vectorPartitioned.unpersist()

    resultVector
  }

  /**
   * Performs Sparse Matrix-Dense Matrix Multiplication (SpMM) using an RDD join.
   *
   * This is the C = A * B operation.
   *
   * This algorithm is optimized using a HashPartitioner to co-locate the
   * join keys (the inner dimension 'j'), minimizing shuffle.
   *
   * @param matrixA The sparse matrix A, as an RDD[COOEntry(i, j, vA)].
   * @param matrixB The dense matrix B, as an RDD[(j, k, vB)].
   * @return The resulting dense matrix C, as an RDD[(i, k, vC)].
   */
  def spmm_dense(
    matrixA: RDD[COOEntry],
    matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    // 1. Re-key Matrix A by its column (j): (j, (i, vA))
    val matrixAByCol_j = matrixA.map {
      case COOEntry(i, j, vA) => (j, (i, vA))
    }

    // 2. Re-key Matrix B by its row (j): (j, (k, vB))
    val matrixBByRow_j = matrixB.map {
      case (j, k, vB) => (j, (k, vB))
    }

    // 3. OPTIMIZATION: Partition both RDDs by the join key (j)
    val numPartitions = matrixA.getNumPartitions
    val partitioner = new HashPartitioner(numPartitions)

    val matrixAPartitioned = matrixAByCol_j
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val matrixBPartitioned = matrixBByRow_j
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // 4. FIRST SHUFFLE (Join):
    //    Result is: (j, ((i, vA), (k, vB)))
    val joined = matrixAPartitioned.join(matrixBPartitioned)

    // 5. Compute Partial Products (Map):
    //    Emit ( (i, k), partial_product )
    val partialProducts = joined.map {
      case (j, ((i, vA), (k, vB))) =>
        val partialProduct = vA * vB
        ((i, k), partialProduct) // Key by (i, k) for final aggregation
    }

    // 6. SECOND SHUFFLE (Aggregate):
    //    Sum all partial products for each output cell (i, k).
    val resultMatrixCells = partialProducts
      .reduceByKey(_ + _)

    // 7. Clean up persisted RDDs
    matrixAPartitioned.unpersist()
    matrixBPartitioned.unpersist()

    // 8. Reformat to the standard (row, col, value) format
    val resultMatrix = resultMatrixCells.map {
      case ((i, k), vC) => (i, k, vC)
    }

    resultMatrix
  }

  /**
   * Performs Sparse Matrix-Sparse Matrix Multiplication (SpMM) using an RDD join.
   *
   * This is the C = A * B operation.
   * @param matrixA The sparse matrix A, as an RDD[COOEntry(i, j, vA)].
   * @param matrixB The sparse matrix B, as an RDD[COOEntry(j, k, vB)].
   * @return The resulting sparse matrix C, as an RDD[COOEntry(i, k, vC)].
   */
  def spmm_sparse(
    matrixA: RDD[COOEntry],
    matrixB: RDD[COOEntry]
  ): RDD[COOEntry] = {

    // 1. Re-key Matrix A by its column (j): (j, (i, vA))
    val matrixAByCol_j = matrixA.map {
      case COOEntry(i, j, vA) => (j, (i, vA))
    }

    // 2. Re-key Matrix B by its row (j): (j, (k, vB))
    val matrixBByRow_j = matrixB.map {
      case COOEntry(j, k, vB) => (j, (k, vB))
    }

    // 3. OPTIMIZATION: Partition both RDDs by the join key (j)
    val numPartitions = matrixA.getNumPartitions
    val partitioner = new HashPartitioner(numPartitions)

    val matrixAPartitioned = matrixAByCol_j
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val matrixBPartitioned = matrixBByRow_j
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // 4. FIRST SHUFFLE (Join):
    //    Result is: (j, ((i, vA), (k, vB)))
    val joined = matrixAPartitioned.join(matrixBPartitioned)

    // 5. Compute Partial Products (Map):
    //    Emit ( (i, k), partial_product )
    val partialProducts = joined.map {
      case (j, ((i, vA), (k, vB))) =>
        val partialProduct = vA * vB
        ((i, k), partialProduct) // Key by (i, k) for final aggregation
    }

    // 6. SECOND SHUFFLE (Aggregate):
    //    Sum all partial products for each output cell (i, k).
    val resultMatrixCells = partialProducts
      .reduceByKey(_ + _)

    // 7. Clean up persisted RDDs
    matrixAPartitioned.unpersist()
    matrixBPartitioned.unpersist()

    // 8. Reformat to COOEntry format
    //    Filter out any near-zero results to maintain sparsity
    val resultMatrix = resultMatrixCells
      .map {
        case ((i, k), vC) => COOEntry(i, k, vC)
      }
      .filter(entry => !entry.isZero) // Maintain sparsity

    resultMatrix
  }

}