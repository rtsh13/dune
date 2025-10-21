package sparsematrix

import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object MatrixLoader {
  
  def loadFromCSV(path: String): SparseMatrix = {
    val lines = Source.fromFile(path).getLines().toList
    val entries = lines.map { line =>
      val parts = line.split(",")
      MatrixEntry(parts(0).toInt, parts(1).toInt, parts(2).toDouble)
    }
    
    val maxRow = entries.map(_.row).max
    val maxCol = entries.map(_.col).max
    
    SparseMatrix(entries, maxRow + 1, maxCol + 1)
  }
  
  def loadFromCSVSpark(spark: SparkSession, path: String): RDD[MatrixEntry] = {
    spark.sparkContext.textFile(path).map { line =>
      val parts = line.split(",")
      MatrixEntry(parts(0).toInt, parts(1).toInt, parts(2).toDouble)
    }
  }
}