package benchmarks

import org.apache.spark.SparkContext 
import engine.storage._
import engine.operations._

object EndToEndBenchmarks {
  
  // Use Case 1: Iterative Algorithm (PageRank-like)
  def benchmarkIterativeAlgorithm(
    sc: SparkContext,
    matrixSize: Int,
    iterations: Int = 10
  ): (Double, Double) = {
    
    println("=== Iterative Algorithm Benchmark ===")
    
    val matrix = SmartLoader.loadMatrix(
      sc,
      s"data/benchmarks/matrix_${matrixSize}_sp0.99.csv"
    )
    
    // Initialize vector
    var vector = sc.parallelize(
      (0 until matrixSize).map(i => (i, 1.0 / matrixSize))
    )
    
    // Custom implementation
    val customStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      val result = matrix * DenseVectorRDD(vector, matrixSize)
      vector = result.entries
    }
    vector.count()
    val customTime = (System.nanoTime() - customStart) / 1000000.0
    
    // DataFrame implementation (baseline)
    val dfTime = runDataFrameIterative(sc, matrix, iterations)
    
    println(f"Custom: ${customTime}%.2f ms")
    println(f"DataFrame: ${dfTime}%.2f ms")
    println(f"Speedup: ${dfTime / customTime}%.2fx")
    
    (customTime, dfTime)
  }
  
  // Use Case 2: Matrix Chain Multiplication
  def benchmarkMatrixChain(
    sc: SparkContext,
    size: Int
  ): Map[String, Double] = {
    
    println("=== Matrix Chain Benchmark ===")
    
    val A = SmartLoader.loadMatrix(
      sc, s"data/benchmarks/matrix_${size}_sp0.95.csv"
    )
    val B = SmartLoader.loadMatrix(
      sc, s"data/benchmarks/matrix_${size}_sp0.95.csv"
    )
    val x = SmartLoader.loadVector(
      sc, s"data/benchmarks/vector_${size}.csv"
    )
    
    // Compute (A * B) * x
    val start = System.nanoTime()
    
    val AB = A * B
    val AB_Matrix = AB.toCOO
    val result = AB_Matrix * x
    result.entries.count()
    
    val elapsed = (System.nanoTime() - start) / 1000000.0
    
    println(f"Total time: ${elapsed}%.2f ms")
    
    Map("time" -> elapsed)
  }
  
  private def runDataFrameIterative(
    sc: SparkContext,
    matrix: Matrix,
    iterations: Int
  ): Double = {
    // Implementation using DataFrame API
    // Similar to runDataFrameSpMV but in a loop
    0.0 // Placeholder
  }
}