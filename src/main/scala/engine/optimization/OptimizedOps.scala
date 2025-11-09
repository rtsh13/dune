package engine.optimization

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import engine.storage._

/** Optimized operations with real performance improvements
  */
object OptimizedOps {

  /** Optimized SpMV - uses efficient strategy
   */
  def optimizedSpMV(
      matrixA: RDD[COOEntry],
      vectorX: RDD[(Int, Double)],
      numPartitions: Option[Int] = None
  ): RDD[(Int, Double)] = {

    println("Computing OPTIMIZED Sparse Matrix x Dense Vector...")

    // Use efficient in-partition aggregation
    AdaptiveOps.efficientSpMV(matrixA, vectorX)
  }

  /** Optimized SpMM with co-partitioning
   */
  def optimizedSpMM(
      matrixA: RDD[COOEntry],
      matrixB: RDD[COOEntry],
      numPartitions: Option[Int] = None
  ): RDD[COOEntry] = {

    println("Computing OPTIMIZED Sparse Matrix x Sparse Matrix...")

    val nParts = numPartitions.getOrElse(
      matrixA.sparkContext.defaultParallelism * 2
    )
    val partitioner = new HashPartitioner(nParts)

    println(s"Using $nParts partitions with in-partition aggregation")

    // Partition by inner dimension
    val matrixAByCol = matrixA
      .map(entry => (entry.col, (entry.row, entry.value)))
      .partitionBy(partitioner)

    val matrixBByRow = matrixB
      .map(entry => (entry.row, (entry.col, entry.value)))
      .partitionBy(partitioner)

    println("Matrices co-partitioned by inner dimension")

    // Join with in-partition aggregation
    val joined = matrixAByCol.join(matrixBByRow)

    val partialProducts = joined.mapPartitions { partition =>
      // Aggregate within partition first
      val accumulator = scala.collection.mutable.HashMap[(Int, Int), Double]()

      partition.foreach { case (j, ((i, vA), (k, vB))) =>
        val key = (i, k)
        val product = vA * vB
        accumulator(key) = accumulator.getOrElse(key, 0.0) + product
      }

      accumulator.iterator
    }

    // Final aggregation across partitions
    val resultCells = partialProducts.reduceByKey(_ + _)

    // Convert to COOEntry
    val result = resultCells
      .map { case ((i, k), value) => COOEntry(i, k, value) }
      .filter(!_.isZero)

    println(s"Optimized SpMM complete. Result has ${result.count()} non-zeros.")

    result
  }

  /** Block-Partitioned SpMM - for very large matrices
   */
  def blockPartitionedSpMM(
      matrixA: RDD[COOEntry],
      matrixB: RDD[COOEntry],
      blockSize: Int = 1000
  ): RDD[COOEntry] = {

    println(s"Computing Block-Partitioned SpMM (block size: $blockSize)...")

    def blockId(index: Int): Int = index / blockSize

    // Group by blocks
    val matrixABlocks = matrixA
      .map { entry =>
        val rowBlock = blockId(entry.row)
        val colBlock = blockId(entry.col)
        ((rowBlock, colBlock), entry)
      }
      .groupByKey()

    val matrixBBlocks = matrixB
      .map { entry =>
        val rowBlock = blockId(entry.row)
        val colBlock = blockId(entry.col)
        ((rowBlock, colBlock), entry)
      }
      .groupByKey()

    println("Matrices partitioned into blocks")

    // Rekey for join
    val aBlocksForJoin = matrixABlocks.map {
      case ((rowBlock, colBlock), entries) =>
        (colBlock, (rowBlock, entries))
    }

    val bBlocksForJoin = matrixBBlocks.map {
      case ((rowBlock, colBlock), entries) =>
        (rowBlock, (colBlock, entries))
    }

    // Join matching blocks
    val joinedBlocks = aBlocksForJoin.join(bBlocksForJoin)

    // Multiply blocks
    val resultEntries = joinedBlocks.flatMap {
      case (innerBlock, ((rowBlockA, entriesA), (colBlockB, entriesB))) =>
        // Local block multiplication with aggregation
        val localResults = scala.collection.mutable.HashMap[(Int, Int), Double]()

        for {
          entryA <- entriesA
          entryB <- entriesB
          if entryA.col == entryB.row
        } {
          val key = (entryA.row, entryB.col)
          val product = entryA.value * entryB.value
          localResults(key) = localResults.getOrElse(key, 0.0) + product
        }

        localResults.iterator
    }

    // Final aggregation
    val result = resultEntries
      .reduceByKey(_ + _)
      .map { case ((i, k), value) => COOEntry(i, k, value) }
      .filter(!_.isZero)

    println(s"Block-partitioned SpMM complete. Result has ${result.count()} non-zeros.")

    result
  }

  /** Cache matrix for iterative operations
   */
  def cacheMatrix(
      matrix: RDD[COOEntry],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
  ): RDD[COOEntry] = {

    println(s"Caching matrix with storage level: $storageLevel")
    matrix.persist(storageLevel)

    val count = matrix.count()
    println(s"Matrix cached: $count entries")

    matrix
  }

  /** Release cached matrix
   */
  def uncacheMatrix(matrix: RDD[COOEntry]): Unit = {
    println("Releasing cached matrix")
    matrix.unpersist(blocking = false)
  }

  /** Compute optimal partitions without collect
   */
  def computeOptimalPartitions(
      numEntries: Long,
      numRows: Int,
      numCols: Int,
      minPartitions: Int = 8,
      maxPartitions: Int = 1000
  ): Int = {

    val targetEntriesPerPartition = 50000
    val calculatedPartitions = (numEntries / targetEntriesPerPartition).toInt
    val optimalPartitions = math.max(minPartitions, math.min(maxPartitions, calculatedPartitions))

    println(s"Computed optimal partitions: $optimalPartitions")
    println(s"  (for $numEntries entries, target: $targetEntriesPerPartition per partition)")

    optimalPartitions
  }
}