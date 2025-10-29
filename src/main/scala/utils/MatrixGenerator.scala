package utils

import scala.util.Random
import java.io.PrintWriter
import java.io.File

object MatrixGenerator {
  
  /**
   * Generate a random sparse matrix and save to CSV
   * 
   * @param numRows Number of rows in matrix
   * @param numCols Number of columns in matrix
   * @param sparsity Fraction of zeros (0.9 = 90% zeros)
   * @param filename Output file path
   * @param seed Random seed for reproducibility
   */

  def generateSparseMatrix(
    numRows: Int, 
    numCols: Int, 
    sparsity: Double,
    filename: String,
    seed: Long = 42
  ): Unit = {
    
    println(s"Generating ${numRows}x${numCols} matrix with ${sparsity*100}% sparsity...")
    
    val random = new Random(seed)
    
    // Calculate how many non-zero entries we want
    val totalEntries = numRows.toLong * numCols
    val numNonZeros = ((1.0 - sparsity) * totalEntries).toInt
    
    println(s"Creating $numNonZeros non-zero entries...")
    
    // Use a Set to avoid duplicate (row, col) pairs
    val entries = scala.collection.mutable.Set[(Int, Int)]()
    
    // Generate random positions for non-zero values
    while (entries.size < numNonZeros) {
      val row = random.nextInt(numRows)
      val col = random.nextInt(numCols)
      entries.add((row, col))
    }
    
    // Write to CSV file: row,col,value
    val writer = new PrintWriter(new File(filename))
    writer.println("row,col,value")  // Header
    
    entries.foreach { case (row, col) =>
      // Generate random value (Gaussian distribution, mean=0, std=1)
      val value = random.nextGaussian() * 10  // Scale by 10
      writer.println(s"$row,$col,$value")
    }
    
    writer.close()
    println(s"Matrix saved to $filename")
  }
  
  /**
   * Generate a dense vector and save to CSV
   * 
   * @param size Vector length
   * @param filename Output file path
   * @param seed Random seed
   */
  def generateDenseVector(
    size: Int,
    filename: String,
    seed: Long = 42
  ): Unit = {
    
    println(s"Generating dense vector of size $size...")
    
    val random = new Random(seed)
    val writer = new PrintWriter(new File(filename))
    writer.println("index,value")  // Header
    
    for (i <- 0 until size) {
      val value = random.nextGaussian() * 10
      writer.println(s"$i,$value")
    }
    
    writer.close()
    println(s"Vector saved to $filename")
  }
  
  /**
   * Generate a sparse vector and save to CSV
   * Only stores non-zero entries
   */
  def generateSparseVector(
    size: Int,
    sparsity: Double,
    filename: String,
    seed: Long = 42
  ): Unit = {
    
    println(s"Generating sparse vector of size $size with ${sparsity*100}% sparsity...")
    
    val random = new Random(seed)
    val numNonZeros = ((1.0 - sparsity) * size).toInt
    
    // Generate random indices for non-zero values
    val indices = random.shuffle((0 until size).toList).take(numNonZeros).sorted
    
    val writer = new PrintWriter(new File(filename))
    writer.println("index,value")  // Header
    
    indices.foreach { idx =>
      val value = random.nextGaussian() * 10
      writer.println(s"$idx,$value")
    }
    
    writer.close()
    println(s"Sparse vector saved to $filename")
  }
  
  /**
   * Generate a dense matrix and save to CSV
   */
  def generateDenseMatrix(
    numRows: Int,
    numCols: Int,
    filename: String,
    seed: Long = 42
  ): Unit = {
    
    println(s"Generating dense ${numRows}x${numCols} matrix...")
    
    val random = new Random(seed)
    val writer = new PrintWriter(new File(filename))
    writer.println("row,col,value")
    
    // Generate ALL entries (dense)
    for (row <- 0 until numRows; col <- 0 until numCols) {
      val value = random.nextGaussian() * 10
      writer.println(s"$row,$col,$value")
    }
    
    writer.close()
    println(s"Dense matrix saved to $filename")
  }
  
  /**
   * Create a simple identity matrix for testing
   */
  def generateIdentityMatrix(
    size: Int,
    filename: String
  ): Unit = {
    
    println(s"Generating ${size}x${size} identity matrix...")
    
    val writer = new PrintWriter(new File(filename))
    writer.println("row,col,value")
    
    for (i <- 0 until size) {
      writer.println(s"$i,$i,1.0")  // Diagonal entries = 1.0
    }
    
    writer.close()
    println(s"Identity matrix saved to $filename")
  }
  
  /**
   * Main method to generate all test files
   */
  def main(args: Array[String]): Unit = {
    
    println("Generating Test Data Files")
    
    // Small matrices for unit testing
    generateSparseMatrix(10, 10, 0.7, "data/test/small_sparse_10x10.csv")
    generateDenseVector(10, "data/test/dense_vector_10.csv")
    generateSparseVector(10, 0.6, "data/test/sparse_vector_10.csv")
    generateIdentityMatrix(10, "data/test/identity_10x10.csv")
    
    // Medium matrices for benchmarking
    generateSparseMatrix(1000, 1000, 0.95, "data/test/medium_sparse_1000x1000.csv")
    generateDenseVector(1000, "data/test/dense_vector_1000.csv")
    generateDenseMatrix(1000, 100, "data/test/dense_matrix_1000x100.csv")
    
    // Large matrices for performance testing
    generateSparseMatrix(10000, 10000, 0.99, "data/test/large_sparse_10kx10k.csv")
    generateDenseVector(10000, "data/test/dense_vector_10k.csv")
    
    println("All test files generated successfully!")
  }
}