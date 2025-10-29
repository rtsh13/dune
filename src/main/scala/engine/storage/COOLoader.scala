package engine.storage

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

/**
 * Loader for converting CSV files to COO format
 */
object COOLoader {
  
  /**
   * Loads a sparse matrix from CSV file into COO format
   * Returns RDD - fully distributed
   */
  def loadSparseMatrix(
    sc: SparkContext,
    filepath: String
  ): RDD[COOEntry] = {
    
    println(s"Loading sparse matrix from: $filepath")
    
    val rawData = sc.textFile(filepath)
    
    val cooEntries = rawData
      .filter(line => !line.startsWith("row"))  
      .filter(line => line.trim.nonEmpty)        
      .flatMap { line =>
        try {
          val parts = line.split(",")
          val row = parts(0).trim.toInt
          val col = parts(1).trim.toInt
          val value = parts(2).trim.toDouble
          
          val entry = COOEntry(row, col, value)
          
          if (entry.isZero) None else Some(entry)
          
        } catch {
          case e: Exception =>
            None  // Skipping malformed lines
        }
      }
    
    cooEntries.cache()
    
    val count = cooEntries.count()
    println(s"Loaded $count non-zero entries")
    
    cooEntries
  }
  
  /**
   * Load a dense vector from CSV file
   * Returns RDD
   * 
   * For vector operations, we'll broadcast a Map of (index -> value)
   */
  def loadDenseVectorRDD(
    sc: SparkContext,
    filepath: String
  ): RDD[(Int, Double)] = {
    
    println(s"Loading dense vector from: $filepath")
    
    val rawData = sc.textFile(filepath)
    
    val entries = rawData
      .filter(line => !line.startsWith("index"))  
      .filter(line => line.trim.nonEmpty)
      .map { line =>
        val parts = line.split(",")
        val index = parts(0).trim.toInt
        val value = parts(1).trim.toDouble
        (index, value)
      }
    
    entries.cache()
    
    val count = entries.count()
    println(s"Loaded dense vector with $count entries")
    
    entries
  }

  def getVectorSize(vectorRDD: RDD[(Int, Double)]): Int = {
    val maxIndex = vectorRDD.map(_._1).max()
    maxIndex + 1
  }

  def loadSparseVector(
    sc: SparkContext,
    filepath: String
  ): RDD[SparseVectorEntry] = {
    
    println(s"Loading sparse vector from: $filepath")
    
    val rawData = sc.textFile(filepath)
    
    val entries = rawData
      .filter(line => !line.startsWith("index"))
      .filter(line => line.trim.nonEmpty)
      .flatMap { line =>
        try {
          val parts = line.split(",")
          val index = parts(0).trim.toInt
          val value = parts(1).trim.toDouble
          val entry = SparseVectorEntry(index, value)
          if (entry.isZero) None else Some(entry)
        } catch {
          case e: Exception => None
        }
      }
    
    entries.cache()
    
    val count = entries.count()
    println(s"Loaded sparse vector: $count non-zeros")
    
    entries
  }

  def getSparseVectorSize(vectorRDD: RDD[SparseVectorEntry]): Int = {
    val maxIndex = vectorRDD.map(_.index).max()
    maxIndex + 1
  }

  def loadDenseMatrix(
    sc: SparkContext,
    filepath: String
  ): RDD[(Int, Int, Double)] = {
    
    println(s"Loading dense matrix from: $filepath")
    
    val rawData = sc.textFile(filepath)
    
    val entries = rawData
      .filter(line => !line.startsWith("row"))
      .filter(line => line.trim.nonEmpty)
      .map { line =>
        val parts = line.split(",")
        val row = parts(0).trim.toInt
        val col = parts(1).trim.toInt
        val value = parts(2).trim.toDouble
        (row, col, value)
      }
    
    entries.cache()
    
    val count = entries.count()
    println(s"Loaded dense matrix with $count entries")
    
    entries
  }

  def getMatrixDimensions(cooEntries: RDD[COOEntry]): (Int, Int) = {
    val (maxRow, maxCol) = cooEntries
      .map(entry => (entry.row, entry.col))
      .reduce { case ((r1, c1), (r2, c2)) =>
        (math.max(r1, r2), math.max(c1, c2))
      }
    
    val numRows = maxRow + 1
    val numCols = maxCol + 1
    
    println(s"Matrix dimensions: $numRows x $numCols")
    
    (numRows, numCols)
  }

  def getDimensions(entries: RDD[(Int, Int, Double)]): (Int, Int) = {
    val (maxRow, maxCol) = entries
      .map { case (row, col, _) => (row, col) }
      .reduce { case ((r1, c1), (r2, c2)) =>
        (math.max(r1, r2), math.max(c1, c2))
      }
    
    (maxRow + 1, maxCol + 1)
  }

  def calculateSparsity(
    numNonZeros: Long,
    numRows: Int,
    numCols: Int
  ): Double = {
    
    val totalElements = numRows.toLong * numCols
    val sparsity = 1.0 - (numNonZeros.toDouble / totalElements)
    
    println(f"Sparsity: ${sparsity * 100}%.2f%% ($numNonZeros non-zeros out of $totalElements)")
    
    sparsity
  }

  def saveCOOMatrix(
    entries: RDD[COOEntry],
    filepath: String
  ): Unit = {
    
    println(s"Saving matrix to: $filepath")
    
    entries
      .map(e => s"${e.row},${e.col},${e.value}")
      .saveAsTextFile(filepath)
    
    println(s"Matrix saved successfully")
  }

  def saveVector(
    entries: RDD[(Int, Double)],
    filepath: String
  ): Unit = {
    
    println(s"Saving vector to: $filepath")
    
    entries
      .sortByKey()
      .map { case (idx, value) => s"$idx,$value" }
      .saveAsTextFile(filepath)
    
    println(s"Vector saved successfully")
  }
}