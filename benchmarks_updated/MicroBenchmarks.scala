package benchmarks_updated

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import engine.storage._
import engine.operations.MultiplicationOps
import engine.optimization.OptimizedOps
import scala.collection.mutable.ArrayBuffer

case class BenchmarkResult(
  operation: String,
  matrixSize: Int,
  sparsity: Double,
  implementation: String,
  executionTimeMs: Long,
  throughput: Double, // operations per second
  memoryUsedMB: Double
)

object MicroBenchmarks {
  
  /**
   * Run comprehensive SpMV (Sparse Matrix × Vector) benchmarks
   * Compares your custom implementation against Spark DataFrame baseline
   */
  def runSpMVBenchmarks(
    sc: SparkContext,
    spark: SparkSession,
    dataDir: String = "synthetic-data",
    sizes: Seq[Int] = Seq(10, 100, 1000),
    iterations: Int = 5
  ): Seq[BenchmarkResult] = {
    
    val results = ArrayBuffer[BenchmarkResult]()
    val sparsity = 0.85 // Your data has 15% density = 85% sparsity
    
    println("SPARSE MATRIX × VECTOR (SpMV) BENCHMARKS")
    
    for (size <- sizes) {
      println(s"\n${"="*80}")
      println(s"Matrix Size: ${size}x${size}, Sparsity: ${sparsity*100}%")
        
      val matrixPath = s"$dataDir/sparse_matrix_${size}x${size}.csv"
      val vectorPath = s"$dataDir/dense_vector_${size}.csv"
      
      println(s"Loading data:")
      println(s"  Matrix: $matrixPath")
      println(s"  Vector: $vectorPath")
      
      try {
        val matrix = SmartLoader.loadMatrix(sc, matrixPath)
        val vector = SmartLoader.loadVector(sc, vectorPath)
        
        println(s"\nMatrix info: $matrix")
        println(s"Vector info: $vector")
        
        // Warmup runs
        println("\n--- Warmup (2 iterations) ---")
        for (i <- 1 to 2) {
          val result = matrix * vector
          val count = result.entries.count()
          println(f"  Warmup $i: result has $count entries")
        }
        
        // Benchmark 1: Your Custom Implementation
        println(s"\n--- Custom Implementation ($iterations iterations) ---")
        val customTimes = ArrayBuffer[Long]()
        val customMemory = ArrayBuffer[Double]()
        
        for (i <- 1 to iterations) {
          System.gc()
          Thread.sleep(500)
          
          val memBefore = getMemoryUsage()
          val start = System.nanoTime()
          
          val result = matrix * vector
          val count = result.entries.count()
          
          val elapsed = (System.nanoTime() - start) / 1000000 // ms
          val memAfter = getMemoryUsage()
          val memUsed = memAfter - memBefore
          
          customTimes += elapsed
          customMemory += memUsed
          
          println(f"  Iteration $i: ${elapsed}%,6d ms | Memory: ${memUsed}%,8.2f MB | Result: $count entries")
        }
        
        val avgCustomTime = customTimes.sum / iterations.toDouble
        val stdDevCustom = calculateStdDev(customTimes.map(_.toDouble), avgCustomTime)
        val avgCustomMemory = customMemory.sum / customMemory.length
        val throughput = (size.toLong * size) / (avgCustomTime / 1000.0) // ops/sec
        
        println(f"\n  Average: ${avgCustomTime}%,.2f ms ± ${stdDevCustom}%.2f ms")
        println(f"  Throughput: ${throughput}%,.0f ops/sec")
        println(f"  Avg Memory: ${avgCustomMemory}%,.2f MB")
        
        results += BenchmarkResult(
          "SpMV", size, sparsity, "Custom", 
          avgCustomTime.toLong, throughput, avgCustomMemory
        )
        
        // Benchmark 2: Spark DataFrame Baseline
        println(s"\n--- DataFrame Baseline ($iterations iterations) ---")
        val dfTimes = runDataFrameSpMV(spark, matrix, vector, iterations)
        val avgDFTime = dfTimes.sum / iterations.toDouble
        val stdDevDF = calculateStdDev(dfTimes, avgDFTime)
        val dfThroughput = (size.toLong * size) / (avgDFTime / 1000.0)
        
        println(f"\n  Average: ${avgDFTime}%,.2f ms ± ${stdDevDF}%.2f ms")
        println(f"  Throughput: ${dfThroughput}%,.0f ops/sec")
        
        results += BenchmarkResult(
          "SpMV", size, sparsity, "DataFrame",
          avgDFTime.toLong, dfThroughput, 0.0
        )
        
        // Calculate speedup
        val speedup = avgDFTime / avgCustomTime
        val speedupPercent = ((avgDFTime - avgCustomTime) / avgDFTime) * 100
        
        println(f"\n  *** SPEEDUP: ${speedup}%.2fx (${speedupPercent}%+.1f%%) ***")
        
        if (speedup > 1.0) {
          println(f"  ✓ Custom implementation is ${speedup}%.2fx FASTER")
        } else {
          println(f"  ✗ Custom implementation is ${1.0/speedup}%.2fx SLOWER")
        }
        
      } catch {
        case e: Exception =>
          println(s"\n✗ ERROR: Could not complete benchmark for size=$size")
          println(s"  ${e.getMessage}")
          e.printStackTrace()
      }
    }
    
    // Summary
    printSpMVSummary(results)
    
    results.toSeq
  }
  
  /**
   * DataFrame-based SpMV implementation (baseline for comparison)
   */
  def runDataFrameSpMV(
    spark: SparkSession,
    matrix: Matrix,
    vector: Vector,
    iterations: Int
  ): Seq[Double] = {
    import spark.implicits._
    
    val times = ArrayBuffer[Double]()
    
    // Convert to DataFrame once
    val matrixDF = matrix.toCOO.entries
      .map(e => (e.row, e.col, e.value))
      .toDF("row", "col", "matValue")
      .cache()
    
    val vectorDF = vector.toIndexValueRDD
      .toDF("col", "vecValue")
      .cache()
    
    // Force caching
    matrixDF.count()
    vectorDF.count()
    
    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(500)
      
      val start = System.nanoTime()
      
      val result = matrixDF
        .join(vectorDF, "col")
        .selectExpr("row", "matValue * vecValue as product")
        .groupBy("row")
        .sum("product")
        .count()
      
      val elapsed = (System.nanoTime() - start) / 1000000.0
      times += elapsed
      
      println(f"  Iteration $i: ${elapsed}%,6.0f ms")
    }
    
    matrixDF.unpersist()
    vectorDF.unpersist()
    
    times.toSeq
  }
  
  /**
   * Compare baseline vs optimized implementations
   */
  def runOptimizationComparison(
    sc: SparkContext,
    dataDir: String = "synthetic-data",
    size: Int = 1000,
    iterations: Int = 5
  ): Map[String, Double] = {
    
    println("OPTIMIZATION COMPARISON: Baseline vs Optimized")
    
    val matrixPath = s"$dataDir/sparse_matrix_${size}x${size}.csv"
    val vectorPath = s"$dataDir/dense_vector_${size}.csv"
    
    println(s"\nMatrix Size: ${size}x${size}")
    println(s"Loading from: $matrixPath")
    
    val matrix = SmartLoader.loadMatrix(sc, matrixPath).toCOO
    val vector = SmartLoader.loadVector(sc, vectorPath)
    
    // Warmup
    println("\nWarmup...")
    for (_ <- 1 to 2) {
      MultiplicationOps.sparseMatrixDenseVector(
        matrix.entries,
        vector.toIndexValueRDD
      ).count()
    }
    
    // Baseline (no optimization)
    println(s"\n--- Baseline Implementation ($iterations iterations) ---")
    val baselineTimes = ArrayBuffer[Long]()
    
    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(500)
      
      val start = System.nanoTime()
      
      val result = MultiplicationOps.sparseMatrixDenseVector(
        matrix.entries,
        vector.toIndexValueRDD
      )
      result.count()
      
      val elapsed = (System.nanoTime() - start) / 1000000
      baselineTimes += elapsed
      
      println(f"  Iteration $i: ${elapsed}%,6d ms")
    }
    
    val avgBaseline = baselineTimes.sum / iterations.toDouble
    println(f"\n  Average: ${avgBaseline}%,.2f ms")
    
    // Optimized version
    println(s"\n--- Optimized Implementation ($iterations iterations) ---")
    val optimizedTimes = ArrayBuffer[Long]()
    
    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(500)
      
      val start = System.nanoTime()
      
      val result = OptimizedOps.optimizedSpMV(
        matrix.entries,
        vector.toIndexValueRDD,
        Some(sc.defaultParallelism)
      )
      result.count()
      
      val elapsed = (System.nanoTime() - start) / 1000000
      optimizedTimes += elapsed
      
      println(f"  Iteration $i: ${elapsed}%,6d ms")
    }
    
    val avgOptimized = optimizedTimes.sum / iterations.toDouble
    val speedup = avgBaseline / avgOptimized
    val improvement = ((avgBaseline - avgOptimized) / avgBaseline) * 100
    
    println(f"\n  Average: ${avgOptimized}%,.2f ms")
    println(f"\n  *** SPEEDUP: ${speedup}%.2fx (${improvement}%+.1f%% improvement) ***")
    
    Map(
      "baseline" -> avgBaseline,
      "optimized" -> avgOptimized,
      "speedup" -> speedup,
      "improvement_percent" -> improvement
    )
  }
  
  /**
   * Benchmark Sparse Matrix × Sparse Matrix multiplication
   */
  def runSpMMBenchmarks(
    sc: SparkContext,
    spark: SparkSession,
    dataDir: String = "synthetic-data",
    sizes: Seq[Int] = Seq(10, 100, 500),
    iterations: Int = 3
  ): Seq[BenchmarkResult] = {
    
    val results = ArrayBuffer[BenchmarkResult]()
    val sparsity = 0.85
    
    println("SPARSE MATRIX × SPARSE MATRIX (SpMM) BENCHMARKS")
    
    for (size <- sizes) {
      println(s"\n${"="*80}")
      println(s"Matrix Size: ${size}x${size}")
        
      val matrixPath = s"$dataDir/sparse_matrix_${size}x${size}.csv"
      
      try {
        val matrixA = SmartLoader.loadMatrix(sc, matrixPath).toCOO
        val matrixB = SmartLoader.loadMatrix(sc, matrixPath).toCOO
        
        println(s"Matrix A: ${matrixA.numNonZeros} non-zeros")
        println(s"Matrix B: ${matrixB.numNonZeros} non-zeros")
        
        // Warmup
        println("\n--- Warmup ---")
        (matrixA * matrixB).toCOO.entries.count()
        
        // Benchmark
        println(s"\n--- SpMM Benchmark ($iterations iterations) ---")
        val times = ArrayBuffer[Long]()
        
        for (i <- 1 to iterations) {
          System.gc()
          Thread.sleep(1000)
          
          val start = System.nanoTime()
          
          val result = matrixA * matrixB
          val count = result.toCOO.entries.count()
          
          val elapsed = (System.nanoTime() - start) / 1000000
          times += elapsed
          
          println(f"  Iteration $i: ${elapsed}%,6d ms | Result: $count non-zeros")
        }
        
        val avgTime = times.sum / iterations.toDouble
        val stdDev = calculateStdDev(times.map(_.toDouble), avgTime)
        val throughput = (size.toLong * size * size) / (avgTime / 1000.0)
        
        println(f"\n  Average: ${avgTime}%,.2f ms ± ${stdDev}%.2f ms")
        println(f"  Throughput: ${throughput}%,.0f ops/sec")
        
        results += BenchmarkResult(
          "SpMM", size, sparsity, "Custom",
          avgTime.toLong, throughput, 0.0
        )
        
      } catch {
        case e: Exception =>
          println(s"\n✗ ERROR: Could not complete SpMM benchmark for size=$size")
          println(s"  ${e.getMessage}")
      }
    }
    
    results.toSeq
  }
  
  /**
   * Benchmark CSR vs COO format performance
   */
  def runFormatComparisonBenchmark(
    sc: SparkContext,
    dataDir: String = "synthetic-data",
    size: Int = 1000,
    iterations: Int = 5
  ): Map[String, Double] = {
    
    println("FORMAT COMPARISON: COO vs CSR")
    
    val matrixPath = s"$dataDir/sparse_matrix_${size}x${size}.csv"
    val vectorPath = s"$dataDir/dense_vector_${size}.csv"
    
    val matrixCOO = SmartLoader.loadMatrix(sc, matrixPath).toCOO
    val matrixCSR = matrixCOO.toCSR
    val vector = SmartLoader.loadVector(sc, vectorPath)
    
    println(s"\nMatrix: ${size}x${size}")
    println(s"Non-zeros: ${matrixCOO.numNonZeros}")
    
    // Benchmark COO format
    println(s"\n--- COO Format ($iterations iterations) ---")
    val cooTimes = ArrayBuffer[Long]()
    
    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(500)
      
      val start = System.nanoTime()
      val result = matrixCOO * vector
      result.entries.count()
      val elapsed = (System.nanoTime() - start) / 1000000
      
      cooTimes += elapsed
      println(f"  Iteration $i: ${elapsed}%,6d ms")
    }
    
    val avgCOO = cooTimes.sum / iterations.toDouble
    println(f"\n  Average: ${avgCOO}%,.2f ms")
    
    // Benchmark CSR format
    println(s"\n--- CSR Format ($iterations iterations) ---")
    val csrTimes = ArrayBuffer[Long]()
    
    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(500)
      
      val start = System.nanoTime()
      val result = matrixCSR * vector
      result.entries.count()
      val elapsed = (System.nanoTime() - start) / 1000000
      
      csrTimes += elapsed
      println(f"  Iteration $i: ${elapsed}%,6d ms")
    }
    
    val avgCSR = csrTimes.sum / iterations.toDouble
    val speedup = avgCOO / avgCSR
    
    println(f"\n  Average: ${avgCSR}%,.2f ms")
    println(f"\n  CSR is ${speedup}%.2fx vs COO")
    
    Map(
      "COO" -> avgCOO,
      "CSR" -> avgCSR,
      "speedup" -> speedup
    )
  }
  
  // Helper functions
  
  private def getMemoryUsage(): Double = {
    val runtime = Runtime.getRuntime
    (runtime.totalMemory() - runtime.freeMemory()) / (1024.0 * 1024.0)
  }
  
  private def calculateStdDev(values: Seq[Double], mean: Double): Double = {
    if (values.length <= 1) return 0.0
    val variance = values.map(v => math.pow(v - mean, 2)).sum / (values.length - 1)
    math.sqrt(variance)
  }
  
  private def printSpMVSummary(results: Seq[BenchmarkResult]): Unit = {
    println("SUMMARY: SpMV Benchmarks")
    
    val grouped = results.groupBy(_.matrixSize)
    
    for ((size, group) <- grouped.toSeq.sortBy(_._1)) {
      val custom = group.find(_.implementation == "Custom")
      val baseline = group.find(_.implementation == "DataFrame")
      
      if (custom.isDefined && baseline.isDefined) {
        val speedup = baseline.get.executionTimeMs.toDouble / custom.get.executionTimeMs
        println(f"\nSize ${size}x${size}:")
        println(f"  Custom:     ${custom.get.executionTimeMs}%,6d ms")
        println(f"  DataFrame:  ${baseline.get.executionTimeMs}%,6d ms")
        println(f"  Speedup:    ${speedup}%.2fx")
      }
    }
    
    // Overall average speedup
    val speedups = grouped.values.flatMap { group =>
      for {
        custom <- group.find(_.implementation == "Custom")
        baseline <- group.find(_.implementation == "DataFrame")
      } yield baseline.executionTimeMs.toDouble / custom.executionTimeMs
    }.toSeq
    
    if (speedups.nonEmpty) {
      val avgSpeedup = speedups.sum / speedups.length
      println(f"\nAverage Speedup: ${avgSpeedup}%.2fx")
    }
  }
}