package engine.operations

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import engine.storage._

object CSROperations extends MatrixOperations[RDD[FormatConverter.CSRRow]] {
  
  def spMV(
      matrix: RDD[FormatConverter.CSRRow],
      vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("CSR Format: Sparse Matrix times Dense Vector")

    val numPartitions = matrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val vectorPartitioned = vectorX.partitionBy(partitioner)

    val csrFlattened = matrix
      .flatMap { row =>
        row.elementsIterator.map { case (col, value) =>
          (col, (row.rowId, value))
        }
      }
      .partitionBy(partitioner)

    val joined = csrFlattened.join(vectorPartitioned)

    joined
      .map { case (col, ((row, matVal), vecVal)) =>
        (row, matVal * vecVal)
      }
      .reduceByKey(_ + _)
  }

  def spMVSparse(
      matrix: RDD[FormatConverter.CSRRow],
      vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {

    println("CSR Format: Sparse Matrix times Sparse Vector")

    val numPartitions = matrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val vectorByIndex = vectorX
      .map(entry => (entry.index, entry.value))
      .partitionBy(partitioner)

    val csrFlattened = matrix
      .flatMap { row =>
        row.elementsIterator.map { case (col, value) =>
          (col, (row.rowId, value))
        }
      }
      .partitionBy(partitioner)

    val joined = csrFlattened.join(vectorByIndex)

    joined
      .map { case (col, ((row, matVal), vecVal)) =>
        (row, matVal * vecVal)
      }
      .reduceByKey(_ + _)
  }

  def spMMDense(
      matrix: RDD[FormatConverter.CSRRow],
      matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("CSR Format: Sparse Matrix times Dense Matrix")

    val numPartitions = matrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val csrByCol = matrix
      .flatMap { row =>
        row.elementsIterator.map { case (col, value) =>
          (col, (row.rowId, value))
        }
      }
      .partitionBy(partitioner)

    val matrixBByRow = matrixB
      .map { case (row, col, value) => (row, (col, value)) }
      .partitionBy(partitioner)

    val joined = csrByCol.join(matrixBByRow)

    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }

    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }
  }

  def spMMSparse(
      matrixA: RDD[FormatConverter.CSRRow],
      matrixB: RDD[FormatConverter.CSRRow]
  ): RDD[COOEntry] = {

    println("CSR Format: Sparse Matrix times Sparse Matrix")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val aByCol = matrixA
      .flatMap { row =>
        row.elementsIterator.map { case (col, value) =>
          (col, (row.rowId, value))
        }
      }
      .partitionBy(partitioner)

    val bByRow = matrixB
      .flatMap { row =>
        row.elementsIterator.map { case (col, value) =>
          (row.rowId, (col, value))
        }
      }
      .partitionBy(partitioner)

    val joined = aByCol.join(bByRow)

    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }

    partialProducts
      .reduceByKey(_ + _)
      .map { case ((i, k), value) => COOEntry(i, k, value) }
      .filter(!_.isZero)
  }
}