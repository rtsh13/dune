package engine.storage

import org.apache.spark.rdd.RDD

object JoinHelpers {
  def joinMatrixWithVector(
    matrixRowKeyed: RDD[(Int, (Int, Double))],  // (row, (col, matrixVal))
    vector: RDD[(Int, Double)]                    // (col, vectorVal)
  ): RDD[(Int, Int, Double, Double)] = {
    
    // Rekey matrix by column for joining with vector
    val matrixColKeyed = matrixRowKeyed.map { case (row, (col, matVal)) =>
      (col, (row, matVal))  // (col, (row, matrixVal))
    }
    
    // Join on column index
    // Result: (col, ((row, matrixVal), vectorVal))
    val joined = matrixColKeyed.join(vector)
    
    // Reshape to (row, col, matrixVal, vectorVal)
    joined.map { case (col, ((row, matVal), vecVal)) =>
      (row, col, matVal, vecVal)
    }
  }

  def joinMatricesForMultiplication(
    matrixAColKeyed: RDD[(Int, (Int, Double))],  // A: (colA, (rowA, valA))
    matrixBRowKeyed: RDD[(Int, (Int, Double))]   // B: (rowB, (colB, valB))
  ): RDD[(Int, Int, Double, Double)] = {
    
    // Join on matching dimension: A's columns = B's rows
    // Result: (k, ((rowA, valA), (colB, valB)))
    val joined = matrixAColKeyed.join(matrixBRowKeyed)
    
    // Reshape to (rowA, colB, valA, valB)
    joined.map { case (k, ((rowA, valA), (colB, valB))) =>
      (rowA, colB, valA, valB)
    }
  }

  def partitionByRow(
    entries: RDD[(Int, Int, Double)],
    numPartitions: Int
  ): RDD[(Int, Int, Double)] = {
    entries
      .map { case (row, col, value) => (row, (col, value)) }
      .partitionBy(new org.apache.spark.HashPartitioner(numPartitions))
      .map { case (row, (col, value)) => (row, col, value) }
  }

  def partitionByColumn(
    entries: RDD[(Int, Int, Double)],
    numPartitions: Int
  ): RDD[(Int, Int, Double)] = {
    entries
      .map { case (row, col, value) => (col, (row, value)) }
      .partitionBy(new org.apache.spark.HashPartitioner(numPartitions))
      .map { case (col, (row, value)) => (row, col, value) }
  }

  def aggregateProducts(
    partialProducts: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {
    partialProducts
      .map { case (row, col, product) => ((row, col), product) }
      .reduceByKey(_ + _)
      .map { case ((row, col), sum) => (row, col, sum) }
  }

  def aggregateVectorProducts(
    partialProducts: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {
    partialProducts.reduceByKey(_ + _)
  }
}