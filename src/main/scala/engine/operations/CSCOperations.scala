package engine.operations

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import engine.storage._
import engine.storage.CSCFormat._

object CSCOperations extends MatrixOperations[RDD[CSCColumn]] {
  
  def spMV(
      matrix: RDD[CSCColumn],
      vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("CSC Format: Sparse Matrix times Dense Vector")

    val numPartitions = matrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val vectorPartitioned = vectorX.partitionBy(partitioner)

    val cscByCol = matrix
      .map(col => (col.columnId, col))
      .partitionBy(partitioner)

    val joined = cscByCol.join(vectorPartitioned)

    val products = joined.flatMap { case (colId, (column, vecVal)) =>
      column.elementsIterator.map { case (row, matVal) =>
        (row, matVal * vecVal)
      }
    }

    products.reduceByKey(_ + _)
  }

  def spMVSparse(
      matrix: RDD[CSCColumn],
      vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {

    println("CSC Format: Sparse Matrix times Sparse Vector")

    val numPartitions = matrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val vectorByIndex = vectorX
      .map(entry => (entry.index, entry.value))
      .partitionBy(partitioner)

    val cscByCol = matrix
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

  def spMMDense(
      matrix: RDD[CSCColumn],
      matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("CSC Format: Sparse Matrix times Dense Matrix")

    val numPartitions = matrix.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val cscFlattened = matrix
      .flatMap { column =>
        column.elementsIterator.map { case (row, value) =>
          (column.columnId, (row, value))
        }
      }
      .partitionBy(partitioner)

    val matrixBByRow = matrixB
      .map { case (row, col, value) => (row, (col, value)) }
      .partitionBy(partitioner)

    val joined = cscFlattened.join(matrixBByRow)

    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }

    partialProducts.reduceByKey(_ + _).map { case ((i, k), v) => (i, k, v) }
  }

  def spMMSparse(
      matrixA: RDD[CSCColumn],
      matrixB: RDD[CSCColumn]
  ): RDD[COOEntry] = {

    println("CSC Format: Sparse Matrix times Sparse Matrix")

    val numPartitions = matrixA.sparkContext.defaultParallelism * 2
    val partitioner = new HashPartitioner(numPartitions)

    val aFlattened = matrixA
      .flatMap { column =>
        column.elementsIterator.map { case (row, value) =>
          (column.columnId, (row, value))
        }
      }
      .partitionBy(partitioner)

    val bFlattened = matrixB
      .flatMap { column =>
        column.elementsIterator.map { case (row, value) =>
          (row, (column.columnId, value))
        }
      }
      .partitionBy(partitioner)

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