package sparsematrix

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SpMV {
  
  // Local version for testing
  def multiply(matrix: SparseMatrix, vector: Array[Double]): Array[Double] = {
    require(matrix.numCols == vector.length, "Dimension mismatch")
    
    val result = Array.fill(matrix.numRows)(0.0)
    
    matrix.entries.foreach { entry =>
      result(entry.row) += entry.value * vector(entry.col)
    }
    
    result
  }
  
  // Spark distributed version
  def multiplySpark(
    matrixRDD: RDD[MatrixEntry], 
    vector: Array[Double]
  ): Array[Double] = {
    
    val vectorBc = matrixRDD.sparkContext.broadcast(vector)
    
    matrixRDD
      .map { entry =>
        (entry.row, entry.value * vectorBc.value(entry.col))
      }
      .reduceByKey(_ + _)
      .sortByKey()
      .map(_._2)
      .collect()
  }
}