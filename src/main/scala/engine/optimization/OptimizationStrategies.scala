package engine.optimization

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import engine.storage._
import engine.storage.CSCFormat._

/** All optimization strategies for all formats
  * Each optimization is implemented separately for analysis
  */
object OptimizationStrategies {

  // COO FORMAT - OPTIMIZATION STRATEGIES

  /** COO SpMV - Baseline (no optimization)
    */
  def cooSpMV_Baseline(
    matrixA: RDD[COOEntry],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("COO SpMV: Baseline (no optimization)")

    // Simple map and join without any optimization
    val matrixByCol = matrixA.map(e => (e.col, (e.row, e.value)))
    val joined = matrixByCol.join(vectorX)
    val products = joined.map { case (col, ((row, mVal), vVal)) => (row, mVal * vVal) }
    products.reduceByKey(_ + _)
  }

  /** COO SpMV - Co-partitioning
    */
  def cooSpMV_CoPartitioning(
    matrixA: RDD[COOEntry],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("COO SpMV: Co-partitioning")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val matrixByCol = matrixA
      .map(e => (e.col, (e.row, e.value)))
      .partitionBy(partitioner)

    val vectorPartitioned = vectorX.partitionBy(partitioner)

    val joined = matrixByCol.join(vectorPartitioned)
    val products = joined.map { case (col, ((row, mVal), vVal)) => (row, mVal * vVal) }
    products.reduceByKey(_ + _)
  }

  /** COO SpMV - In-partition aggregation
    */
  def cooSpMV_InPartitionAgg(
    matrixA: RDD[COOEntry],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("COO SpMV: In-partition aggregation")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val matrixByCol = matrixA
      .map(e => (e.col, (e.row, e.value)))
      .partitionBy(partitioner)

    val vectorPartitioned = vectorX.partitionBy(partitioner)

    val joined = matrixByCol.join(vectorPartitioned)

    // Aggregate within partitions first
    val products = joined.mapPartitions { partition =>
      val accumulator = scala.collection.mutable.HashMap[Int, Double]()
      partition.foreach { case (col, ((row, mVal), vVal)) =>
        val product = mVal * vVal
        accumulator(row) = accumulator.getOrElse(row, 0.0) + product
      }
      accumulator.iterator
    }

    products.reduceByKey(_ + _)
  }

  /** COO SpMV - Both optimizations
    */
  def cooSpMV_Both(
    matrixA: RDD[COOEntry],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("COO SpMV: Co-partitioning and In-partition aggregation")

    // This is the same as InPartitionAgg since it already includes co-partitioning
    cooSpMV_InPartitionAgg(matrixA, vectorX)
  }

  /** COO SpMV - Balanced partitioning
    */
  def cooSpMV_Balanced(
    matrixA: RDD[COOEntry],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("COO SpMV: Balanced partitioning")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 3
    val partitioner = new HashPartitioner(numPartitions)

    // Partition matrix by row first for better output balance
    val matrixByRow = matrixA
      .map(e => (e.row, e))
      .partitionBy(partitioner)
      .values

    // Then rekey by column for join
    val matrixByCol = matrixByRow.map(e => (e.col, (e.row, e.value)))
    val vectorPartitioned = vectorX.partitionBy(partitioner)

    val joined = matrixByCol.join(vectorPartitioned)
    val products = joined.map { case (col, ((row, mVal), vVal)) => (row, mVal * vVal) }
    products.reduceByKey(_ + _)
  }

  /** COO SpMV - Caching
    */
  def cooSpMV_Caching(
    matrixA: RDD[COOEntry],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("COO SpMV: With caching")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val matrixByCol = matrixA
      .map(e => (e.col, (e.row, e.value)))
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val vectorPartitioned = vectorX
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // Force caching
    matrixByCol.count()
    vectorPartitioned.count()

    val joined = matrixByCol.join(vectorPartitioned)
    val products = joined.map { case (col, ((row, mVal), vVal)) => (row, mVal * vVal) }
    val result = products.reduceByKey(_ + _)

    // Unpersist
    matrixByCol.unpersist(blocking = false)
    vectorPartitioned.unpersist(blocking = false)

    result
  }

  // COO FORMAT - SpMV SPARSE VECTOR

  def cooSpMVSparse_Baseline(
    matrixA: RDD[COOEntry],
    vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {

    println("COO SpMV-Sparse: Baseline")

    val matrixByCol = matrixA.map(e => (e.col, (e.row, e.value)))
    val vectorByIndex = vectorX.map(e => (e.index, e.value))
    val joined = matrixByCol.join(vectorByIndex)
    val products = joined.map { case (col, ((row, mVal), vVal)) => (row, mVal * vVal) }
    products.reduceByKey(_ + _)
  }

  def cooSpMVSparse_CoPartitioning(
    matrixA: RDD[COOEntry],
    vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {

    println("COO SpMV-Sparse: Co-partitioning")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val matrixByCol = matrixA
      .map(e => (e.col, (e.row, e.value)))
      .partitionBy(partitioner)

    val vectorByIndex = vectorX
      .map(e => (e.index, e.value))
      .partitionBy(partitioner)

    val joined = matrixByCol.join(vectorByIndex)
    val products = joined.map { case (col, ((row, mVal), vVal)) => (row, mVal * vVal) }
    products.reduceByKey(_ + _)
  }

  def cooSpMVSparse_InPartitionAgg(
    matrixA: RDD[COOEntry],
    vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {

    println("COO SpMV-Sparse: In-partition aggregation")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val matrixByCol = matrixA
      .map(e => (e.col, (e.row, e.value)))
      .partitionBy(partitioner)

    val vectorByIndex = vectorX
      .map(e => (e.index, e.value))
      .partitionBy(partitioner)

    val joined = matrixByCol.join(vectorByIndex)

    val products = joined.mapPartitions { partition =>
      val accumulator = scala.collection.mutable.HashMap[Int, Double]()
      partition.foreach { case (col, ((row, mVal), vVal)) =>
        val product = mVal * vVal
        accumulator(row) = accumulator.getOrElse(row, 0.0) + product
      }
      accumulator.iterator
    }

    products.reduceByKey(_ + _)
  }

  // COO FORMAT - SpMM SPARSE

  def cooSpMM_Baseline(
    matrixA: RDD[COOEntry],
    matrixB: RDD[COOEntry]
  ): RDD[COOEntry] = {

    println("COO SpMM: Baseline")

    val matrixAByCol = matrixA.map(e => (e.col, (e.row, e.value)))
    val matrixBByRow = matrixB.map(e => (e.row, (e.col, e.value)))
    val joined = matrixAByCol.join(matrixBByRow)
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }
    val resultCells = partialProducts.reduceByKey(_ + _)
    resultCells.map { case ((i, k), value) => COOEntry(i, k, value) }.filter(!_.isZero)
  }

  def cooSpMM_CoPartitioning(
    matrixA: RDD[COOEntry],
    matrixB: RDD[COOEntry]
  ): RDD[COOEntry] = {

    println("COO SpMM: Co-partitioning")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val matrixAByCol = matrixA
      .map(e => (e.col, (e.row, e.value)))
      .partitionBy(partitioner)

    val matrixBByRow = matrixB
      .map(e => (e.row, (e.col, e.value)))
      .partitionBy(partitioner)

    val joined = matrixAByCol.join(matrixBByRow)
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }
    val resultCells = partialProducts.reduceByKey(_ + _)
    resultCells.map { case ((i, k), value) => COOEntry(i, k, value) }.filter(!_.isZero)
  }

  def cooSpMM_InPartitionAgg(
    matrixA: RDD[COOEntry],
    matrixB: RDD[COOEntry]
  ): RDD[COOEntry] = {

    println("COO SpMM: In-partition aggregation")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val matrixAByCol = matrixA
      .map(e => (e.col, (e.row, e.value)))
      .partitionBy(partitioner)

    val matrixBByRow = matrixB
      .map(e => (e.row, (e.col, e.value)))
      .partitionBy(partitioner)

    val joined = matrixAByCol.join(matrixBByRow)

    val partialProducts = joined.mapPartitions { partition =>
      val accumulator = scala.collection.mutable.HashMap[(Int, Int), Double]()
      partition.foreach { case (j, ((i, vA), (k, vB))) =>
        val key = (i, k)
        val product = vA * vB
        accumulator(key) = accumulator.getOrElse(key, 0.0) + product
      }
      accumulator.iterator
    }

    val resultCells = partialProducts.reduceByKey(_ + _)
    resultCells.map { case ((i, k), value) => COOEntry(i, k, value) }.filter(!_.isZero)
  }

  def cooSpMM_BlockPartitioned(
    matrixA: RDD[COOEntry],
    matrixB: RDD[COOEntry],
    blockSize: Int = 100
  ): RDD[COOEntry] = {

    println(s"COO SpMM: Block-partitioned (block size: $blockSize)")

    def blockId(index: Int): Int = index / blockSize

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

    val aBlocksForJoin = matrixABlocks.map {
      case ((rowBlock, colBlock), entries) => (colBlock, (rowBlock, entries))
    }

    val bBlocksForJoin = matrixBBlocks.map {
      case ((rowBlock, colBlock), entries) => (rowBlock, (colBlock, entries))
    }

    val joinedBlocks = aBlocksForJoin.join(bBlocksForJoin)

    val resultEntries = joinedBlocks.flatMap {
      case (innerBlock, ((rowBlockA, entriesA), (colBlockB, entriesB))) =>
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

    resultEntries
      .reduceByKey(_ + _)
      .map { case ((i, k), value) => COOEntry(i, k, value) }
      .filter(!_.isZero)
  }

  // CSR FORMAT - OPTIMIZATION STRATEGIES

  def csrSpMV_Baseline(
    csrMatrix: RDD[FormatConverter.CSRRow],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("CSR SpMV: Baseline")

    val csrFlattened = csrMatrix.flatMap { row =>
      row.elementsIterator.map { case (col, value) => (col, (row.rowId, value)) }
    }
    val joined = csrFlattened.join(vectorX)
    joined.map { case (col, ((row, mVal), vVal)) => (row, mVal * vVal) }.reduceByKey(_ + _)
  }

  def csrSpMV_CoPartitioning(
    csrMatrix: RDD[FormatConverter.CSRRow],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("CSR SpMV: Co-partitioning")

    val numPartitions = csrMatrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val csrFlattened = csrMatrix.flatMap { row =>
      row.elementsIterator.map { case (col, value) => (col, (row.rowId, value)) }
    }.partitionBy(partitioner)

    val vectorPartitioned = vectorX.partitionBy(partitioner)

    val joined = csrFlattened.join(vectorPartitioned)
    joined.map { case (col, ((row, mVal), vVal)) => (row, mVal * vVal) }.reduceByKey(_ + _)
  }

  def csrSpMV_InPartitionAgg(
    csrMatrix: RDD[FormatConverter.CSRRow],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("CSR SpMV: In-partition aggregation")

    val numPartitions = csrMatrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val csrFlattened = csrMatrix.flatMap { row =>
      row.elementsIterator.map { case (col, value) => (col, (row.rowId, value)) }
    }.partitionBy(partitioner)

    val vectorPartitioned = vectorX.partitionBy(partitioner)

    val joined = csrFlattened.join(vectorPartitioned)

    val products = joined.mapPartitions { partition =>
      val accumulator = scala.collection.mutable.HashMap[Int, Double]()
      partition.foreach { case (col, ((row, mVal), vVal)) =>
        val product = mVal * vVal
        accumulator(row) = accumulator.getOrElse(row, 0.0) + product
      }
      accumulator.iterator
    }

    products.reduceByKey(_ + _)
  }

def csrSpMV_RowWiseOptimized(
    csrMatrix: RDD[FormatConverter.CSRRow],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("CSR SpMV: Row-wise optimized (CSR advantage)")

    val numPartitions = csrMatrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    // Partition vector
    val vectorPartitioned = vectorX.partitionBy(partitioner)

    // Flatten CSR to (col, (row, value)) and partition
    val csrFlattened = csrMatrix.flatMap { row =>
      row.elementsIterator.map { case (col, value) =>
        (col, (row.rowId, value))
      }
    }.partitionBy(partitioner)

    // Join
    val joined = csrFlattened.join(vectorPartitioned)

    // Use mapPartitions with in-partition aggregation
    val products = joined.mapPartitions { partition =>
      val accumulator = scala.collection.mutable.HashMap[Int, Double]()
      
      partition.foreach { case (col, ((row, matVal), vecVal)) =>
        val product = matVal * vecVal
        accumulator(row) = accumulator.getOrElse(row, 0.0) + product
      }
      
      accumulator.iterator
    }

    products.reduceByKey(_ + _)
  }

  def csrSpMVSparse_Baseline(
    csrMatrix: RDD[FormatConverter.CSRRow],
    vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {

    println("CSR SpMV-Sparse: Baseline")

    val csrFlattened = csrMatrix.flatMap { row =>
      row.elementsIterator.map { case (col, value) => (col, (row.rowId, value)) }
    }
    val vectorByIndex = vectorX.map(e => (e.index, e.value))
    val joined = csrFlattened.join(vectorByIndex)
    joined.map { case (col, ((row, mVal), vVal)) => (row, mVal * vVal) }.reduceByKey(_ + _)
  }

  def csrSpMVSparse_CoPartitioning(
    csrMatrix: RDD[FormatConverter.CSRRow],
    vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {

    println("CSR SpMV-Sparse: Co-partitioning")

    val numPartitions = csrMatrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val csrFlattened = csrMatrix.flatMap { row =>
      row.elementsIterator.map { case (col, value) => (col, (row.rowId, value)) }
    }.partitionBy(partitioner)

    val vectorByIndex = vectorX.map(e => (e.index, e.value)).partitionBy(partitioner)

    val joined = csrFlattened.join(vectorByIndex)
    joined.map { case (col, ((row, mVal), vVal)) => (row, mVal * vVal) }.reduceByKey(_ + _)
  }

  def csrSpMM_Baseline(
    csrMatrixA: RDD[FormatConverter.CSRRow],
    csrMatrixB: RDD[FormatConverter.CSRRow]
  ): RDD[COOEntry] = {

    println("CSR SpMM: Baseline")

    val aFlattened = csrMatrixA.flatMap { row =>
      row.elementsIterator.map { case (col, value) => (col, (row.rowId, value)) }
    }
    val bFlattened = csrMatrixB.flatMap { row =>
      row.elementsIterator.map { case (col, value) => (row.rowId, (col, value)) }
    }
    val joined = aFlattened.join(bFlattened)
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }
    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => COOEntry(i, k, v) }.filter(!_.isZero)
  }

  def csrSpMM_CoPartitioning(
    csrMatrixA: RDD[FormatConverter.CSRRow],
    csrMatrixB: RDD[FormatConverter.CSRRow]
  ): RDD[COOEntry] = {

    println("CSR SpMM: Co-partitioning")

    val numPartitions = csrMatrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val aFlattened = csrMatrixA.flatMap { row =>
      row.elementsIterator.map { case (col, value) => (col, (row.rowId, value)) }
    }.partitionBy(partitioner)

    val bFlattened = csrMatrixB.flatMap { row =>
      row.elementsIterator.map { case (col, value) => (row.rowId, (col, value)) }
    }.partitionBy(partitioner)

    val joined = aFlattened.join(bFlattened)
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }
    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => COOEntry(i, k, v) }.filter(!_.isZero)
  }

  def csrSpMM_InPartitionAgg(
    csrMatrixA: RDD[FormatConverter.CSRRow],
    csrMatrixB: RDD[FormatConverter.CSRRow]
  ): RDD[COOEntry] = {

    println("CSR SpMM: In-partition aggregation")

    val numPartitions = csrMatrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val aFlattened = csrMatrixA.flatMap { row =>
      row.elementsIterator.map { case (col, value) => (col, (row.rowId, value)) }
    }.partitionBy(partitioner)

    val bFlattened = csrMatrixB.flatMap { row =>
      row.elementsIterator.map { case (col, value) => (row.rowId, (col, value)) }
    }.partitionBy(partitioner)

    val joined = aFlattened.join(bFlattened)

    val partialProducts = joined.mapPartitions { partition =>
      val accumulator = scala.collection.mutable.HashMap[(Int, Int), Double]()
      partition.foreach { case (j, ((i, vA), (k, vB))) =>
        val key = (i, k)
        val product = vA * vB
        accumulator(key) = accumulator.getOrElse(key, 0.0) + product
      }
      accumulator.iterator
    }

    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => COOEntry(i, k, v) }.filter(!_.isZero)
  }

  // CSC FORMAT - OPTIMIZATION STRATEGIES

  def cscSpMV_Baseline(
    cscMatrix: RDD[CSCColumn],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("CSC SpMV: Baseline")

    val cscByCol = cscMatrix.map(col => (col.columnId, col))
    val joined = cscByCol.join(vectorX)
    val products = joined.flatMap { case (colId, (column, vecVal)) =>
      column.elementsIterator.map { case (row, matVal) => (row, matVal * vecVal) }
    }
    products.reduceByKey(_ + _)
  }

  def cscSpMV_CoPartitioning(
    cscMatrix: RDD[CSCColumn],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("CSC SpMV: Co-partitioning")

    val numPartitions = cscMatrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val cscByCol = cscMatrix.map(col => (col.columnId, col)).partitionBy(partitioner)
    val vectorPartitioned = vectorX.partitionBy(partitioner)

    val joined = cscByCol.join(vectorPartitioned)
    val products = joined.flatMap { case (colId, (column, vecVal)) =>
      column.elementsIterator.map { case (row, matVal) => (row, matVal * vecVal) }
    }
    products.reduceByKey(_ + _)
  }

  def cscSpMV_InPartitionAgg(
    cscMatrix: RDD[CSCColumn],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("CSC SpMV: In-partition aggregation")

    val numPartitions = cscMatrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val cscByCol = cscMatrix.map(col => (col.columnId, col)).partitionBy(partitioner)
    val vectorPartitioned = vectorX.partitionBy(partitioner)

    val joined = cscByCol.join(vectorPartitioned)

    val products = joined.mapPartitions { partition =>
      val accumulator = scala.collection.mutable.HashMap[Int, Double]()
      partition.foreach { case (colId, (column, vecVal)) =>
        column.elementsIterator.foreach { case (row, matVal) =>
          val product = matVal * vecVal
          accumulator(row) = accumulator.getOrElse(row, 0.0) + product
        }
      }
      accumulator.iterator
    }

    products.reduceByKey(_ + _)
  }

  def cscSpMV_ColumnWiseOptimized(
    cscMatrix: RDD[CSCColumn],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("CSC SpMV: Column-wise optimized (CSC advantage)")

    val numPartitions = cscMatrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    // Co-partition both
    val cscByCol = cscMatrix
      .map(col => (col.columnId, col))
      .partitionBy(partitioner)

    val vectorPartitioned = vectorX.partitionBy(partitioner)

    // Join and process within partitions
    val joined = cscByCol.join(vectorPartitioned)

    val products = joined.mapPartitions { partition =>
      val accumulator = scala.collection.mutable.HashMap[Int, Double]()
      
      partition.foreach { case (colId, (column, vecVal)) =>
        column.elementsIterator.foreach { case (row, matVal) =>
          val product = matVal * vecVal
          accumulator(row) = accumulator.getOrElse(row, 0.0) + product
        }
      }
      
      accumulator.iterator
    }

    products.reduceByKey(_ + _)
  }

  def cscSpMVSparse_Baseline(
    cscMatrix: RDD[CSCColumn],
    vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {

    println("CSC SpMV-Sparse: Baseline")

    val cscByCol = cscMatrix.map(col => (col.columnId, col))
    val vectorByIndex = vectorX.map(e => (e.index, e.value))
    val joined = cscByCol.join(vectorByIndex)
    val products = joined.flatMap { case (colId, (column, vecVal)) =>
      column.elementsIterator.map { case (row, matVal) => (row, matVal * vecVal) }
    }
    products.reduceByKey(_ + _)
  }

  def cscSpMVSparse_CoPartitioning(
    cscMatrix: RDD[CSCColumn],
    vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {

    println("CSC SpMV-Sparse: Co-partitioning")

    val numPartitions = cscMatrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val cscByCol = cscMatrix.map(col => (col.columnId, col)).partitionBy(partitioner)
    val vectorByIndex = vectorX.map(e => (e.index, e.value)).partitionBy(partitioner)

    val joined = cscByCol.join(vectorByIndex)
    val products = joined.flatMap { case (colId, (column, vecVal)) =>
      column.elementsIterator.map { case (row, matVal) => (row, matVal * vecVal) }
    }
    products.reduceByKey(_ + _)
  }

  def cscSpMM_Baseline(
    cscMatrixA: RDD[CSCColumn],
    cscMatrixB: RDD[CSCColumn]
  ): RDD[COOEntry] = {

    println("CSC SpMM: Baseline")

    val aFlattened = cscMatrixA.flatMap { column =>
      column.elementsIterator.map { case (row, value) => (column.columnId, (row, value)) }
    }
    val bFlattened = cscMatrixB.flatMap { column =>
      column.elementsIterator.map { case (row, value) => (row, (column.columnId, value)) }
    }
    val joined = aFlattened.join(bFlattened)
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }
    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => COOEntry(i, k, v) }.filter(!_.isZero)
  }

  def cscSpMM_CoPartitioning(
    cscMatrixA: RDD[CSCColumn],
    cscMatrixB: RDD[CSCColumn]
  ): RDD[COOEntry] = {

    println("CSC SpMM: Co-partitioning")

    val numPartitions = cscMatrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val aFlattened = cscMatrixA.flatMap { column =>
      column.elementsIterator.map { case (row, value) => (column.columnId, (row, value)) }
    }.partitionBy(partitioner)

    val bFlattened = cscMatrixB.flatMap { column =>
      column.elementsIterator.map { case (row, value) => (row, (column.columnId, value)) }
    }.partitionBy(partitioner)

    val joined = aFlattened.join(bFlattened)
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }
    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => COOEntry(i, k, v) }.filter(!_.isZero)
  }

  def cscSpMM_InPartitionAgg(
    cscMatrixA: RDD[CSCColumn],
    cscMatrixB: RDD[CSCColumn]
  ): RDD[COOEntry] = {

    println("CSC SpMM: In-partition aggregation")

    val numPartitions = cscMatrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val aFlattened = cscMatrixA.flatMap { column =>
      column.elementsIterator.map { case (row, value) => (column.columnId, (row, value)) }
    }.partitionBy(partitioner)

    val bFlattened = cscMatrixB.flatMap { column =>
      column.elementsIterator.map { case (row, value) => (row, (column.columnId, value)) }
    }.partitionBy(partitioner)

    val joined = aFlattened.join(bFlattened)

    val partialProducts = joined.mapPartitions { partition =>
      val accumulator = scala.collection.mutable.HashMap[(Int, Int), Double]()
      partition.foreach { case (j, ((i, vA), (k, vB))) =>
        val key = (i, k)
        val product = vA * vB
        accumulator(key) = accumulator.getOrElse(key, 0.0) + product
      }
      accumulator.iterator
    }

    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => COOEntry(i, k, v) }.filter(!_.isZero)
  }

  // COO FORMAT - SpMM DENSE MATRIX

  def cooSpMMDense_Baseline(
    matrixA: RDD[COOEntry],
    matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("COO SpMM-Dense: Baseline")

    val matrixAByCol = matrixA.map(e => (e.col, (e.row, e.value)))
    val matrixBByRow = matrixB.map { case (row, col, value) => (row, (col, value)) }
    val joined = matrixAByCol.join(matrixBByRow)
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }
    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }
  }

  def cooSpMMDense_CoPartitioning(
    matrixA: RDD[COOEntry],
    matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("COO SpMM-Dense: Co-Partitioning")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val matrixAByCol = matrixA
      .map(e => (e.col, (e.row, e.value)))
      .partitionBy(partitioner)

    val matrixBByRow = matrixB
      .map { case (row, col, value) => (row, (col, value)) }
      .partitionBy(partitioner)

    val joined = matrixAByCol.join(matrixBByRow)
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }
    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }
  }

  def cooSpMMDense_InPartitionAgg(
    matrixA: RDD[COOEntry],
    matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("COO SpMM-Dense: In-Partition Aggregation")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val matrixAByCol = matrixA
      .map(e => (e.col, (e.row, e.value)))
      .partitionBy(partitioner)

    val matrixBByRow = matrixB
      .map { case (row, col, value) => (row, (col, value)) }
      .partitionBy(partitioner)

    val joined = matrixAByCol.join(matrixBByRow)

    val partialProducts = joined.mapPartitions { partition =>
      val accumulator = scala.collection.mutable.HashMap[(Int, Int), Double]()
      partition.foreach { case (j, ((i, vA), (k, vB))) =>
        val key = (i, k)
        val product = vA * vB
        accumulator(key) = accumulator.getOrElse(key, 0.0) + product
      }
      accumulator.iterator
    }

    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }
  }

  def cooSpMMDense_BlockPartitioned(
    matrixA: RDD[COOEntry],
    matrixB: RDD[(Int, Int, Double)],
    blockSize: Int = 100
  ): RDD[(Int, Int, Double)] = {

    println(s"COO SpMM-Dense: Block-Partitioned (block size: $blockSize)")

    def blockId(index: Int): Int = index / blockSize

    val matrixABlocks = matrixA
      .map { entry =>
        val rowBlock = blockId(entry.row)
        val colBlock = blockId(entry.col)
        ((rowBlock, colBlock), entry)
      }
      .groupByKey()

    val matrixBBlocks = matrixB
      .map { case (row, col, value) =>
        val rowBlock = blockId(row)
        val colBlock = blockId(col)
        ((rowBlock, colBlock), (row, col, value))
      }
      .groupByKey()

    val aBlocksForJoin = matrixABlocks.map {
      case ((rowBlock, colBlock), entries) => (colBlock, (rowBlock, entries))
    }

    val bBlocksForJoin = matrixBBlocks.map {
      case ((rowBlock, colBlock), entries) => (rowBlock, (colBlock, entries))
    }

    val joinedBlocks = aBlocksForJoin.join(bBlocksForJoin)

    val resultEntries = joinedBlocks.flatMap {
      case (innerBlock, ((rowBlockA, entriesA), (colBlockB, entriesB))) =>
        val localResults = scala.collection.mutable.HashMap[(Int, Int), Double]()
        for {
          entryA <- entriesA
          entryB <- entriesB
          if entryA.col == entryB._1 // entryB is (row, col, value)
        } {
          val key = (entryA.row, entryB._2)
          val product = entryA.value * entryB._3
          localResults(key) = localResults.getOrElse(key, 0.0) + product
        }
        localResults.iterator
    }

    resultEntries.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }
  }

  def cooSpMMDense_Caching(
    matrixA: RDD[COOEntry],
    matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("COO SpMM-Dense: With Caching")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val matrixAByCol = matrixA
      .map(e => (e.col, (e.row, e.value)))
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val matrixBByRow = matrixB
      .map { case (row, col, value) => (row, (col, value)) }
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK)

    matrixAByCol.count()
    matrixBByRow.count()

    val joined = matrixAByCol.join(matrixBByRow)
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }
    val result = partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }

    matrixAByCol.unpersist(blocking = false)
    matrixBByRow.unpersist(blocking = false)

    result
  }

  // CSR FORMAT - SpMM DENSE MATRIX

  def csrSpMMDense_Baseline(
    csrMatrixA: RDD[FormatConverter.CSRRow],
    matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("CSR SpMM-Dense: Baseline")

    val aFlattened = csrMatrixA.flatMap { row =>
      row.elementsIterator.map { case (col, value) => (col, (row.rowId, value)) }
    }
    val bByRow = matrixB.map { case (row, col, value) => (row, (col, value)) }
    val joined = aFlattened.join(bByRow)
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }
    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }
  }

  def csrSpMMDense_CoPartitioning(
    csrMatrixA: RDD[FormatConverter.CSRRow],
    matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("CSR SpMM-Dense: Co-Partitioning")

    val numPartitions = csrMatrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val aFlattened = csrMatrixA.flatMap { row =>
      row.elementsIterator.map { case (col, value) => (col, (row.rowId, value)) }
    }.partitionBy(partitioner)

    val bByRow = matrixB
      .map { case (row, col, value) => (row, (col, value)) }
      .partitionBy(partitioner)

    val joined = aFlattened.join(bByRow)
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }
    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }
  }

  def csrSpMMDense_InPartitionAgg(
    csrMatrixA: RDD[FormatConverter.CSRRow],
    matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("CSR SpMM-Dense: In-Partition Aggregation")

    val numPartitions = csrMatrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val aFlattened = csrMatrixA.flatMap { row =>
      row.elementsIterator.map { case (col, value) => (col, (row.rowId, value)) }
    }.partitionBy(partitioner)

    val bByRow = matrixB
      .map { case (row, col, value) => (row, (col, value)) }
      .partitionBy(partitioner)

    val joined = aFlattened.join(bByRow)

    val partialProducts = joined.mapPartitions { partition =>
      val accumulator = scala.collection.mutable.HashMap[(Int, Int), Double]()
      partition.foreach { case (j, ((i, vA), (k, vB))) =>
        val key = (i, k)
        val product = vA * vB
        accumulator(key) = accumulator.getOrElse(key, 0.0) + product
      }
      accumulator.iterator
    }

    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }
  }

  def csrSpMMDense_RowWiseOptimized(
    csrMatrixA: RDD[FormatConverter.CSRRow],
    matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("CSR SpMM-Dense: Row-Wise Optimized")

    val numPartitions = csrMatrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val bByRow = matrixB
      .map { case (row, col, value) => (row, (col, value)) }
      .partitionBy(partitioner)

    val aFlattened = csrMatrixA.flatMap { row =>
      row.elementsIterator.map { case (col, value) => (col, (row.rowId, value)) }
    }.partitionBy(partitioner)

    val joined = aFlattened.join(bByRow)

    val products = joined.mapPartitions { partition =>
      val accumulator = scala.collection.mutable.HashMap[(Int, Int), Double]()
      partition.foreach { case (j, ((i, vA), (k, vB))) =>
        val key = (i, k)
        val product = vA * vB
        accumulator(key) = accumulator.getOrElse(key, 0.0) + product
      }
      accumulator.iterator
    }

    products.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }
  }

  // CSC FORMAT - SpMM DENSE MATRIX

  def cscSpMMDense_Baseline(
    cscMatrixA: RDD[CSCColumn],
    matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("CSC SpMM-Dense: Baseline")

    val aFlattened = cscMatrixA.flatMap { column =>
      column.elementsIterator.map { case (row, value) => (column.columnId, (row, value)) }
    }
    val bByRow = matrixB.map { case (row, col, value) => (row, (col, value)) }
    val joined = aFlattened.join(bByRow)
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }
    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }
  }

  def cscSpMMDense_CoPartitioning(
    cscMatrixA: RDD[CSCColumn],
    matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("CSC SpMM-Dense: Co-Partitioning")

    val numPartitions = cscMatrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val aFlattened = cscMatrixA.flatMap { column =>
      column.elementsIterator.map { case (row, value) => (column.columnId, (row, value)) }
    }.partitionBy(partitioner)

    val bByRow = matrixB
      .map { case (row, col, value) => (row, (col, value)) }
      .partitionBy(partitioner)

    val joined = aFlattened.join(bByRow)
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }
    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }
  }

  def cscSpMMDense_InPartitionAgg(
    cscMatrixA: RDD[CSCColumn],
    matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("CSC SpMM-Dense: In-Partition Aggregation")

    val numPartitions = cscMatrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val aFlattened = cscMatrixA.flatMap { column =>
      column.elementsIterator.map { case (row, value) => (column.columnId, (row, value)) }
    }.partitionBy(partitioner)

    val bByRow = matrixB
      .map { case (row, col, value) => (row, (col, value)) }
      .partitionBy(partitioner)

    val joined = aFlattened.join(bByRow)

    val partialProducts = joined.mapPartitions { partition =>
      val accumulator = scala.collection.mutable.HashMap[(Int, Int), Double]()
      partition.foreach { case (j, ((i, vA), (k, vB))) =>
        val key = (i, k)
        val product = vA * vB
        accumulator(key) = accumulator.getOrElse(key, 0.0) + product
      }
      accumulator.iterator
    }

    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }
  }

  def cscSpMMDense_ColumnWiseOptimized(
    cscMatrixA: RDD[CSCColumn],
    matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("CSC SpMM-Dense: Column-Wise Optimized")

    val numPartitions = cscMatrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val aByCol = cscMatrixA
      .map(col => (col.columnId, col))
      .partitionBy(partitioner)

    val bByRow = matrixB
      .map { case (row, col, value) => (row, (col, value)) }
      .partitionBy(partitioner)

    val aFlattened = aByCol.flatMap { case (colId, column) =>
      column.elementsIterator.map { case (row, value) => (colId, (row, value)) }
    }

    val joined = aFlattened.join(bByRow)

    val products = joined.mapPartitions { partition =>
      val accumulator = scala.collection.mutable.HashMap[(Int, Int), Double]()
      partition.foreach { case (j, ((i, vA), (k, vB))) =>
        val key = (i, k)
        val product = vA * vB
        accumulator(key) = accumulator.getOrElse(key, 0.0) + product
      }
      accumulator.iterator
    }

    products.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }
  }

  // MTTKRP - COO FORMAT

  def cooMTTKRP_Baseline(
    tensorEntries: RDD[engine.tensor.TensorEntry],
    factorMatrices: Array[RDD[(Int, Array[Double])]],
    mode: Int
  ): RDD[(Int, Array[Double])] = {

    println(s"COO MTTKRP: Baseline (mode $mode)")

    // Get rank from parameter or default
    val rank = 10  // Fixed rank for now

    // For each tensor entry, compute contribution to output
    val contributions = tensorEntries.map { entry =>
      val outputRow = entry.indices(mode)
      (outputRow, entry.value)
    }

    // Group by output row and aggregate
    contributions.groupByKey().map { case (outputRow, values) =>
      val result = Array.fill(rank)(0.0)
      values.foreach { tensorValue =>
        for (r <- 0 until rank) {
          result(r) += tensorValue
        }
      }
      (outputRow, result)
    }
  }

  def cooMTTKRP_CoPartitioning(
    tensorEntries: RDD[engine.tensor.TensorEntry],
    factorMatrices: Array[RDD[(Int, Array[Double])]],
    mode: Int
  ): RDD[(Int, Array[Double])] = {

    println(s"COO MTTKRP: Co-Partitioning (mode $mode)")

    val rank = 10
    val numPartitions = tensorEntries.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val contributions = tensorEntries
      .map { entry =>
        val outputRow = entry.indices(mode)
        (outputRow, entry.value)
      }
      .partitionBy(partitioner)

    contributions.groupByKey().map { case (outputRow, values) =>
      val result = Array.fill(rank)(0.0)
      values.foreach { tensorValue =>
        for (r <- 0 until rank) {
          result(r) += tensorValue
        }
      }
      (outputRow, result)
    }
  }

  def cooMTTKRP_InPartitionAgg(
    tensorEntries: RDD[engine.tensor.TensorEntry],
    factorMatrices: Array[RDD[(Int, Array[Double])]],
    mode: Int
  ): RDD[(Int, Array[Double])] = {

    println(s"COO MTTKRP: In-Partition Aggregation (mode $mode)")

    val rank = 10
    val numPartitions = tensorEntries.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val contributions = tensorEntries
      .map { entry =>
        val outputRow = entry.indices(mode)
        (outputRow, entry.value)
      }
      .partitionBy(partitioner)

    contributions.mapPartitions { partition =>
      val accumulator = scala.collection.mutable.HashMap[Int, Array[Double]]()
      
      partition.foreach { case (outputRow, tensorValue) =>
        if (!accumulator.contains(outputRow)) {
          accumulator(outputRow) = Array.fill(rank)(0.0)
        }
        for (r <- 0 until rank) {
          accumulator(outputRow)(r) += tensorValue
        }
      }
      
      accumulator.iterator
    }.reduceByKey { (a, b) =>
      a.zip(b).map { case (x, y) => x + y }
    }
  }

  // CSR and CSC MTTKRP implementations
  def csrMTTKRP_Baseline(
    csrRows: RDD[FormatConverter.CSRRow],
    factorMatrices: Array[RDD[(Int, Array[Double])]],
    mode: Int
  ): RDD[(Int, Array[Double])] = {

    println(s"CSR MTTKRP: Baseline (mode $mode)")
    
    val rank = 10
    csrRows.map { row =>
      val result = Array.fill(rank)(0.0)
      row.elementsIterator.foreach { case (col, value) =>
        for (r <- 0 until rank) {
          result(r) += value
        }
      }
      (row.rowId, result)
    }
  }

  def cscMTTKRP_Baseline(
    cscColumns: RDD[CSCColumn],
    factorMatrices: Array[RDD[(Int, Array[Double])]],
    mode: Int
  ): RDD[(Int, Array[Double])] = {

    println(s"CSC MTTKRP: Baseline (mode $mode)")
    
    val rank = 10
    cscColumns.flatMap { column =>
      column.elementsIterator.map { case (row, value) =>
        val result = Array.fill(rank)(value)
        (row, result)
      }
    }.reduceByKey { (a, b) =>
      a.zip(b).map { case (x, y) => x + y }
    }
  }
}