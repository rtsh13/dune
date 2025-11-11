package engine.operations

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import engine.storage._
import engine.storage.CSCFormat._

/** Format-specific multiplication operations
  * Implements all 5 operations for COO, CSR, and CSC formats
  */
object FormatSpecificOps {

  // COO FORMAT OPERATIONS

  /** COO: Sparse Matrix times Dense Vector
    */
  def cooSpMV(
    matrixA: RDD[COOEntry],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("COO Format: Sparse Matrix times Dense Vector")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    // Partition matrix by column (join key)
    val matrixByCol = matrixA
      .map(entry => (entry.col, (entry.row, entry.value)))
      .partitionBy(partitioner)

    // Partition vector by index
    val vectorPartitioned = vectorX.partitionBy(partitioner)

    // Join and compute products
    val joined = matrixByCol.join(vectorPartitioned)

    val products = joined.map { case (col, ((row, matVal), vecVal)) =>
      (row, matVal * vecVal)
    }

    // Aggregate by row
    products.reduceByKey(_ + _)
  }

  /** COO: Sparse Matrix times Sparse Vector
    */
  def cooSpMVSparse(
    matrixA: RDD[COOEntry],
    vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {

    println("COO Format: Sparse Matrix times Sparse Vector")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val matrixByCol = matrixA
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

  /** COO: Sparse Matrix times Dense Matrix
    */
  def cooSpMMDense(
    matrixA: RDD[COOEntry],
    matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("COO Format: Sparse Matrix times Dense Matrix")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val matrixAByCol = matrixA
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

  /** COO: Sparse Matrix times Sparse Matrix
    */
  def cooSpMMSparse(
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

  // CSR FORMAT OPERATIONS

  /** CSR: Sparse Matrix times Dense Vector
    */
  def csrSpMV(
    csrMatrix: RDD[FormatConverter.CSRRow],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("CSR Format: Sparse Matrix times Dense Vector")

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

  /** CSR: Sparse Matrix times Sparse Vector
    */
  def csrSpMVSparse(
    csrMatrix: RDD[FormatConverter.CSRRow],
    vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {

    println("CSR Format: Sparse Matrix times Sparse Vector")

    val numPartitions = csrMatrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val vectorByIndex = vectorX
      .map(entry => (entry.index, entry.value))
      .partitionBy(partitioner)

    val csrFlattened = csrMatrix.flatMap { row =>
      row.elementsIterator.map { case (col, value) =>
        (col, (row.rowId, value))
      }
    }.partitionBy(partitioner)

    val joined = csrFlattened.join(vectorByIndex)

    joined
      .map { case (col, ((row, matVal), vecVal)) =>
        (row, matVal * vecVal)
      }
      .reduceByKey(_ + _)
  }

  /** CSR: Sparse Matrix times Dense Matrix
    */
  def csrSpMMDense(
    csrMatrix: RDD[FormatConverter.CSRRow],
    matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("CSR Format: Sparse Matrix times Dense Matrix")

    val numPartitions = csrMatrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val csrByCol = csrMatrix.flatMap { row =>
      row.elementsIterator.map { case (col, value) =>
        (col, (row.rowId, value))
      }
    }.partitionBy(partitioner)

    val matrixBByRow = matrixB
      .map { case (row, col, value) => (row, (col, value)) }
      .partitionBy(partitioner)

    val joined = csrByCol.join(matrixBByRow)

    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }

    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }
  }

  /** CSR: Sparse Matrix times Sparse Matrix
    */
  def csrSpMMSparse(
    csrMatrixA: RDD[FormatConverter.CSRRow],
    csrMatrixB: RDD[FormatConverter.CSRRow]
  ): RDD[COOEntry] = {

    println("CSR Format: Sparse Matrix times Sparse Matrix")

    val numPartitions = csrMatrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val aByCol = csrMatrixA.flatMap { row =>
      row.elementsIterator.map { case (col, value) =>
        (col, (row.rowId, value))
      }
    }.partitionBy(partitioner)

    val bByRow = csrMatrixB.flatMap { row =>
      row.elementsIterator.map { case (col, value) =>
        (row.rowId, (col, value))
      }
    }.partitionBy(partitioner)

    val joined = aByCol.join(bByRow)

    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }

    partialProducts
      .reduceByKey(_ + _)
      .map { case ((i, k), value) => COOEntry(i, k, value) }
      .filter(!_.isZero)
  }

  // CSC FORMAT OPERATIONS

  /** CSC: Sparse Matrix times Dense Vector
    */
  def cscSpMV(
    cscMatrix: RDD[CSCColumn],
    vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("CSC Format: Sparse Matrix times Dense Vector")

    val numPartitions = cscMatrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    // Partition vector by index
    val vectorPartitioned = vectorX.partitionBy(partitioner)

    // Rekey CSC columns by column ID
    val cscByCol = cscMatrix
      .map(col => (col.columnId, col))
      .partitionBy(partitioner)

    // Join CSC columns with vector
    val joined = cscByCol.join(vectorPartitioned)

    // For each column, multiply all its entries by vector value
    val products = joined.flatMap { case (colId, (column, vecVal)) =>
      column.elementsIterator.map { case (row, matVal) =>
        (row, matVal * vecVal)
      }
    }

    products.reduceByKey(_ + _)
  }

  /** CSC: Sparse Matrix times Sparse Vector
    */
  def cscSpMVSparse(
    cscMatrix: RDD[CSCColumn],
    vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {

    println("CSC Format: Sparse Matrix times Sparse Vector")

    val numPartitions = cscMatrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val vectorByIndex = vectorX
      .map(entry => (entry.index, entry.value))
      .partitionBy(partitioner)

    val cscByCol = cscMatrix
      .map(col => (col.columnId, col))
      .partitionBy(partitioner)

    val joined = cscByCol.join(vectorByIndex)

    val products = joined.flatMap { case (colId, (column, vecVal)) =>
      column.elementsIterator.map { case (row, matVal) =>
        (row, matVal * vecVal)
      }
    }

    products.reduceByKey(_ + _)
  }

  /** CSC: Sparse Matrix times Dense Matrix
    */
  def cscSpMMDense(
    cscMatrix: RDD[CSCColumn],
    matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("CSC Format: Sparse Matrix times Dense Matrix")

    val numPartitions = cscMatrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    // Flatten CSC to (col, (row, value))
    val cscFlattened = cscMatrix.flatMap { column =>
      column.elementsIterator.map { case (row, value) =>
        (column.columnId, (row, value))
      }
    }.partitionBy(partitioner)

    val matrixBByRow = matrixB
      .map { case (row, col, value) => (row, (col, value)) }
      .partitionBy(partitioner)

    val joined = cscFlattened.join(matrixBByRow)

    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }

    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }
  }

  /** CSC: Sparse Matrix times Sparse Matrix
    */
  def cscSpMMSparse(
    cscMatrixA: RDD[CSCColumn],
    cscMatrixB: RDD[CSCColumn]
  ): RDD[COOEntry] = {

    println("CSC Format: Sparse Matrix times Sparse Matrix")

    val numPartitions = cscMatrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    // Flatten A: (col, (row, value))
    val aFlattened = cscMatrixA.flatMap { column =>
      column.elementsIterator.map { case (row, value) =>
        (column.columnId, (row, value))
      }
    }.partitionBy(partitioner)

    // Flatten B and rekey: (row, (col, value))
    val bFlattened = cscMatrixB.flatMap { column =>
      column.elementsIterator.map { case (row, value) =>
        (row, (column.columnId, value))
      }
    }.partitionBy(partitioner)

    val joined = aFlattened.join(bFlattened)

    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }

    partialProducts
      .reduceByKey(_ + _)
      .map { case ((i, k), value) => COOEntry(i, k, value) }
      .filter(!_.isZero)
  }
}