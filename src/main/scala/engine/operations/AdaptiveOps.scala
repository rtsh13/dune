package engine.optimization

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import engine.storage._

/** Adaptive operations that choose optimal strategy based on data characteristics
  */
object AdaptiveOps {

  case class DataStats(
    estimatedSize: Long,
    numPartitions: Int,
    avgPartitionSize: Long
  )

  /** Estimate RDD size without collect() - uses sampling
    */
  def estimateSize(rdd: RDD[_]): Long = {
    val sampleFraction = 0.01 // 1% sample
    val sampledCount = rdd.sample(withReplacement = false, sampleFraction).count()
    (sampledCount / sampleFraction).toLong
  }

  /** Adaptive SpMV - chooses strategy based on data characteristics
    * 
    * Strategies:
    * 1. Map-side join (co-partitioned) for medium data
    * 2. Optimized shuffle for large data
    */
  def adaptiveSpMV(
    matrixA: RDD[COOEntry],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("Using ADAPTIVE Sparse Matrix x Dense Vector...")

    // Estimate vector size without collect
    val vectorSizeEstimate = estimateSize(vectorX)
    
    println(s"Estimated vector size: $vectorSizeEstimate")

    // Choose strategy based on estimated size
    if (vectorSizeEstimate < 50000) {
      println("Strategy: MAP-SIDE JOIN (co-partitioned)")
      mapSideJoinSpMV(matrixA, vectorX)
    } else {
      println("Strategy: OPTIMIZED SHUFFLE")
      optimizedShuffleSpMV(matrixA, vectorX)
    }
  }

  /** Map-side join with co-partitioning
    * Both RDDs partitioned by same key, join happens locally
    * 
    * PUBLIC - can be called from benchmarks
    */
  def mapSideJoinSpMV(
    matrixA: RDD[COOEntry],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    val numPartitions = math.max(matrixA.getNumPartitions, vectorX.getNumPartitions)
    val partitioner = new HashPartitioner(numPartitions)

    println(s"Using $numPartitions partitions for co-location")

    // Partition matrix by column (join key)
    val matrixByCol = matrixA
      .map(entry => (entry.col, (entry.row, entry.value)))
      .partitionBy(partitioner)

    // Partition vector by index (join key)
    val vectorPartitioned = vectorX.partitionBy(partitioner)

    // Join - now LOCAL, no shuffle needed!
    val joined = matrixByCol.join(vectorPartitioned)

    // Compute products
    val products = joined.map { case (col, ((row, matVal), vecVal)) =>
      (row, matVal * vecVal)
    }

    // Aggregate by row
    products.reduceByKey(_ + _)
  }

  /** Optimized shuffle with smart partitioning
    * 
    * PUBLIC - can be called from benchmarks
    */
  def optimizedShuffleSpMV(
    matrixA: RDD[COOEntry],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    // Partition by join key to minimize shuffle
    val matrixByCol = matrixA
      .map(entry => (entry.col, (entry.row, entry.value)))
      .partitionBy(partitioner)

    val vectorByIndex = vectorX.partitionBy(partitioner)

    // Join with co-located data
    val joined = matrixByCol.join(vectorByIndex)

    // Map to products
    val products = joined.map { case (col, ((row, matVal), vecVal)) =>
      (row, matVal * vecVal)
    }

    // Aggregate
    products.reduceByKey(_ + _)
  }

  /** Efficient SpMV with in-partition aggregation
    * Reduces shuffle volume by pre-aggregating within partitions
    * 
    * PUBLIC - can be called from benchmarks
    */
  def efficientSpMV(
    matrixA: RDD[COOEntry],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("Using EFFICIENT SpMV with in-partition aggregation...")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    // Co-partition both RDDs
    val matrixByCol = matrixA
      .map(entry => (entry.col, (entry.row, entry.value)))
      .partitionBy(partitioner)

    val vectorByIndex = vectorX.partitionBy(partitioner)

    // Join and aggregate within partitions
    val products = matrixByCol.join(vectorByIndex).mapPartitions { partition =>
      // Use mutable map for efficiency within partition
      val accumulator = scala.collection.mutable.HashMap[Int, Double]()

      partition.foreach { case (col, ((row, matVal), vecVal)) =>
        val product = matVal * vecVal
        accumulator(row) = accumulator.getOrElse(row, 0.0) + product
      }

      accumulator.iterator
    }

    // Final aggregation across partitions
    products.reduceByKey(_ + _)
  }

  /** Balanced SpMV - repartitions matrix by row for better load balance
    * 
    * PUBLIC - can be called from benchmarks
    */
  def balancedSpMV(
    matrixA: RDD[COOEntry],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("Using BALANCED SpMV...")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 3
    val partitioner = new HashPartitioner(numPartitions)

    // Partition matrix by ROW for output balance
    val matrixByRow = matrixA
      .map(entry => (entry.row, entry))
      .partitionBy(partitioner)
      .values

    // Partition vector by index
    val vectorByIndex = vectorX.partitionBy(partitioner)

    // Now join: matrix rekey by col, then join with vector
    val matrixByCol = matrixByRow.map(e => (e.col, (e.row, e.value)))

    val joined = matrixByCol.join(vectorByIndex)

    val products = joined.map { case (col, ((row, matVal), vecVal)) =>
      (row, matVal * vecVal)
    }

    products.reduceByKey(_ + _)
  }

  /** CSR-optimized SpMV without any collect operations
    * 
    * PUBLIC - can be called from benchmarks
    */
  def csrSpMV(
    csrMatrix: RDD[FormatConverter.CSRRow],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("Using CSR-optimized SpMV...")

    val numPartitions = csrMatrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    // Partition vector by index
    val vectorPartitioned = vectorX.partitionBy(partitioner)

    // Flatten CSR rows and partition by column
    val csrFlattened = csrMatrix.flatMap { row =>
      row.elementsIterator.map { case (col, value) =>
        (col, (row.rowId, value))
      }
    }.partitionBy(partitioner)

    // Join with vector
    val joined = csrFlattened.join(vectorPartitioned)

    // Compute products and aggregate by row
    joined
      .map { case (col, ((row, matVal), vecVal)) =>
        (row, matVal * vecVal)
      }
      .reduceByKey(_ + _)
  }

  /** Iterative SpMV for algorithms like PageRank
    * Caches matrix, reuses for multiple vector multiplications
    * 
    * PUBLIC - can be called from benchmarks
    */
  def iterativeSpMV(
    matrixA: RDD[COOEntry],
    vectors: Seq[RDD[(Int, Double)]],
    iterations: Int = 10
  ): Seq[RDD[(Int, Double)]] = {

    println(s"Using ITERATIVE SpMV for $iterations iterations...")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    // Partition and cache matrix once
    val matrixPartitioned = matrixA
      .map(e => (e.col, (e.row, e.value)))
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK)

    matrixPartitioned.count() // Force caching

    println("Matrix cached and partitioned")

    // Multiply with each vector
    val results = vectors.map { vector =>
      val vectorPartitioned = vector.partitionBy(partitioner)

      val joined = matrixPartitioned.join(vectorPartitioned)

      joined
        .map { case (col, ((row, matVal), vecVal)) =>
          (row, matVal * vecVal)
        }
        .reduceByKey(_ + _)
    }

    // Cleanup
    matrixPartitioned.unpersist(blocking = false)

    println("Iterative SpMV complete")

    results
  }

  /** Simple baseline SpMV without optimizations
    * For comparison in ablation studies
    * 
    * PUBLIC - can be called from benchmarks
    */
  def baselineSpMV(
    matrixA: RDD[COOEntry],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("Using BASELINE SpMV (no optimizations)...")

    val matrixByCol = matrixA.map(e => (e.col, (e.row, e.value)))
    val joined = matrixByCol.join(vectorX)
    
    joined
      .map { case (col, ((row, matVal), vecVal)) =>
        (row, matVal * vecVal)
      }
      .reduceByKey(_ + _)
  }
}