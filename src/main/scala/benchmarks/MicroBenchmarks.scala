package benchmarks

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
  throughput: Double,
  memoryUsedMB: Double
)

object MicroBenchmarks {
  
  def runAllSpMVBenchmarks(
    sc: SparkContext,
    spark: SparkSession,
    dataDir: String = "synthetic-data",
    iterations: Int = 5
  ): Seq[BenchmarkResult] = {
    
    val results = ArrayBuffer[BenchmarkResult]()
    
    println("\n" + "="*80)
    println("SPARSE MATRIX x VECTOR (SpMV) BENCHMARKS")
    println("Testing ALL available datasets")
    println("="*80)
    
    val datasets = DatasetDiscovery.discoverAllDatasets(dataDir)
    
    if (datasets.isEmpty) {
      println("ERROR: No datasets found in " + dataDir)
      return Seq.empty
    }
    
    val byCategory = datasets.groupBy(_.category)
    
    def getIterations(category: String): Int = category match {
      case "Small" => iterations
      case "Medium" => math.max(3, iterations - 2)
      case "Large" => 3
      case "Extra-Large" => 2
      case _ => 3
    }
    
    for ((category, categoryDatasets) <- byCategory.toSeq.sortBy(_._1)) {
      val iters = getIterations(category)
      
      println("\n" + "="*80)
      println(s"CATEGORY: $category (${categoryDatasets.size} datasets, $iters iterations)")
      println("="*80)
      
      for (ds <- categoryDatasets) {
        println(s"\n--- Dataset: ${ds.size}x${ds.size} (${ds.fileSizeMB}%.1f MB) ---")
        
        val matrixPath = s"$dataDir/${ds.matrixFile}"
        val vectorPath = s"$dataDir/${ds.vectorFile}"
        
        try {
          println(s"Loading ${ds.size}x${ds.size} matrix...")
          val loadStart = System.nanoTime()
          
          val matrix = SmartLoader.loadMatrix(sc, matrixPath)
          val vector = SmartLoader.loadVector(sc, vectorPath)
          
          val loadTime = (System.nanoTime() - loadStart) / 1000000.0
          println(f"Loaded in ${loadTime}%.2f ms")
          println(s"  Matrix: $matrix")
          println(s"  Vector: $vector")
          
          println("\nWarmup...")
          val warmupResult = matrix * vector
          val warmupCount = warmupResult.entries.count()
          println(s"Warmup complete: $warmupCount entries")
          
          println(s"\nBenchmarking ($iters iterations)...")
          val customTimes = ArrayBuffer[Long]()
          val customMemory = ArrayBuffer[Double]()
          
          for (i <- 1 to iters) {
            System.gc()
            Thread.sleep(if (ds.category == "Extra-Large") 2000 else 500)
            
            val memBefore = getMemoryUsage()
            val start = System.nanoTime()
            
            val result = matrix * vector
            val count = result.entries.count()
            
            val elapsed = (System.nanoTime() - start) / 1000000
            val memAfter = getMemoryUsage()
            val memUsed = memAfter - memBefore
            
            customTimes += elapsed
            customMemory += memUsed
            
            println(f"  Iteration $i: ${elapsed}%,6d ms | Memory: ${memUsed}%,8.2f MB | Result: $count entries")
          }
          
          val avgCustomTime = customTimes.sum / iters.toDouble
          val stdDevCustom = calculateStdDev(customTimes.map(_.toDouble), avgCustomTime)
          val avgCustomMemory = customMemory.sum / customMemory.length
          val throughput = (ds.size.toLong * ds.size) / (avgCustomTime / 1000.0)
          
          println(f"\nAverage: ${avgCustomTime}%,.2f ms +/- ${stdDevCustom}%.2f ms")
          println(f"Throughput: ${throughput}%,.0f ops/sec")
          println(f"Avg Memory: ${avgCustomMemory}%,.2f MB")
          
          results += BenchmarkResult(
            "SpMV", ds.size, 0.95, "Custom", 
            avgCustomTime.toLong, throughput, avgCustomMemory
          )
          
          val shouldTestDataFrame = ds.category match {
            case "Small" => true
            case "Medium" => true
            case "Large" if ds.size <= 12000 => {
              println("\nWARNING: Testing DataFrame on Large dataset - may be slow")
              true
            }
            case _ => false
          }
          
          if (shouldTestDataFrame) {
            println(s"\nDataFrame baseline ($iters iterations)...")
            try {
              val dfTimes = runDataFrameSpMV(spark, matrix, vector, iters)
              val avgDFTime = dfTimes.sum / iters.toDouble
              val stdDevDF = calculateStdDev(dfTimes, avgDFTime)
              val dfThroughput = (ds.size.toLong * ds.size) / (avgDFTime / 1000.0)
              
              println(f"DataFrame Average: ${avgDFTime}%,.2f ms +/- ${stdDevDF}%.2f ms")
              
              results += BenchmarkResult(
                "SpMV", ds.size, 0.95, "DataFrame",
                avgDFTime.toLong, dfThroughput, 0.0
              )
              
              val speedup = avgDFTime / avgCustomTime
              val speedupPercent = ((avgDFTime - avgCustomTime) / avgDFTime) * 100
              
              println(f"\nSpeedup: ${speedup}%.2fx (${speedupPercent}%+.1f%%)")
              
              if (speedup > 1.0) {
                println(f"Custom implementation is ${speedup}%.2fx FASTER")
              } else {
                println(f"Custom implementation is ${1.0/speedup}%.2fx SLOWER")
              }
            } catch {
              case e: OutOfMemoryError =>
                println("DataFrame ran out of memory - skipping")
              case e: Exception =>
                println(s"DataFrame failed: ${e.getMessage}")
            }
          }
          
        } catch {
          case e: OutOfMemoryError =>
            println(s"\nOUT OF MEMORY for ${ds.size}x${ds.size} dataset")
            println("  Try: Increase spark.driver.memory")
            println(s"  Error: ${e.getMessage}")
            
          case e: Exception =>
            println(s"\nERROR: Could not complete benchmark for ${ds.size}x${ds.size}")
            println(s"  ${e.getMessage}")
            if (ds.category != "Extra-Large") {
              e.printStackTrace()
            }
        }
      }
    }
    
    printSpMVSummary(results)
    results.toSeq
  }
  
  def runDataFrameSpMV(
    spark: SparkSession,
    matrix: Matrix,
    vector: Vector,
    iterations: Int
  ): Seq[Double] = {
    import spark.implicits._
    
    val times = ArrayBuffer[Double]()
    
    val matrixDF = matrix.toCOO.entries
      .map(e => (e.row, e.col, e.value))
      .toDF("row", "col", "matValue")
      .cache()
    
    val vectorDF = vector.toIndexValueRDD
      .toDF("col", "vecValue")
      .cache()
    
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
  
  def runOptimizationComparison(
    sc: SparkContext,
    dataDir: String = "synthetic-data",
    targetSize: Option[Int] = None,
    iterations: Int = 5
  ): Map[String, Double] = {
    
    println("\n" + "="*80)
    println("OPTIMIZATION COMPARISON: Baseline vs Optimized")
    println("="*80)
    
    val datasets = DatasetDiscovery.discoverAllDatasets(dataDir)
    
    val selectedDataset = targetSize match {
      case Some(size) => datasets.find(_.size == size)
      case None => {
        val large = datasets.filter(ds => ds.category == "Large" || ds.category == "Extra-Large")
        if (large.nonEmpty) {
          println("Using LARGE dataset for optimization comparison")
          Some(large.maxBy(_.size))
        } else {
          println("No large datasets found, using Medium")
          datasets.find(_.category == "Medium")
        }
      }
    }
    
    if (selectedDataset.isEmpty) {
      println("ERROR: No suitable dataset found for optimization comparison")
      return Map.empty
    }
    
    val ds = selectedDataset.get
    
    val matrixPath = s"$dataDir/${ds.matrixFile}"
    val vectorPath = s"$dataDir/${ds.vectorFile}"
    
    println(s"\nDataset: ${ds.size}x${ds.size} (${ds.fileSizeMB}%.1f MB, ${ds.category})")
    println(s"Loading from: $matrixPath")
    
    val matrix = SmartLoader.loadMatrix(sc, matrixPath).toCOO
    val vector = SmartLoader.loadVector(sc, vectorPath)
    
    println("\nWarmup...")
    for (_ <- 1 to 2) {
      MultiplicationOps.sparseMatrixDenseVector(
        matrix.entries,
        vector.toIndexValueRDD
      ).count()
    }
    
    val actualIterations = if (ds.size > 20000) 3 else iterations
    
    println(s"\n--- Baseline Implementation ($actualIterations iterations) ---")
    val baselineTimes = ArrayBuffer[Long]()
    
    for (i <- 1 to actualIterations) {
      System.gc()
      Thread.sleep(1000)
      
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
    
    val avgBaseline = baselineTimes.sum / actualIterations.toDouble
    println(f"\nBaseline Average: ${avgBaseline}%,.2f ms")
    
    println(s"\n--- Optimized Implementation ($actualIterations iterations) ---")
    val optimizedTimes = ArrayBuffer[Long]()
    
    for (i <- 1 to actualIterations) {
      System.gc()
      Thread.sleep(1000)
      
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
    
    val avgOptimized = optimizedTimes.sum / actualIterations.toDouble
    val speedup = avgBaseline / avgOptimized
    val improvement = ((avgBaseline - avgOptimized) / avgBaseline) * 100
    
    println(f"\nOptimized Average: ${avgOptimized}%,.2f ms")
    println(f"\nSPEEDUP: ${speedup}%.2fx (${improvement}%+.1f%% improvement)")
    
    if (speedup > 1.0) {
      println(f"SUCCESS: Optimizations are EFFECTIVE at scale ${ds.size}x${ds.size}")
    } else {
      println(f"NOTE: Optimization overhead still dominates at ${ds.size}x${ds.size}")
    }
    
    Map(
      "baseline" -> avgBaseline,
      "optimized" -> avgOptimized,
      "speedup" -> speedup,
      "improvement_percent" -> improvement,
      "dataset_size" -> ds.size.toDouble
    )
  }
  
  def runSpMMBenchmarks(
    sc: SparkContext,
    spark: SparkSession,
    dataDir: String = "synthetic-data",
    maxSize: Int = 500,
    iterations: Int = 3
  ): Seq[BenchmarkResult] = {
    
    val results = ArrayBuffer[BenchmarkResult]()
    
    println("\n" + "="*80)
    println("SPARSE MATRIX x SPARSE MATRIX (SpMM) BENCHMARKS")
    println("="*80)
    
    val datasets = DatasetDiscovery.discoverAllDatasets(dataDir)
      .filter(_.size <= maxSize)
      .filter(ds => ds.category == "Small" || ds.category == "Medium")
    
    if (datasets.isEmpty) {
      println("No suitable datasets found for SpMM (need size <= " + maxSize + ")")
      return Seq.empty
    }
    
    for (ds <- datasets) {
      println(s"\n--- Dataset: ${ds.size}x${ds.size} ---")
      
      val matrixPath = s"$dataDir/${ds.matrixFile}"
      
      try {
        val matrixA = SmartLoader.loadMatrix(sc, matrixPath).toCOO
        val matrixB = SmartLoader.loadMatrix(sc, matrixPath).toCOO
        
        println(s"Matrix A: ${matrixA.numNonZeros} non-zeros")
        println(s"Matrix B: ${matrixB.numNonZeros} non-zeros")
        
        println("\nWarmup...")
        (matrixA * matrixB).toCOO.entries.count()
        
        println(s"\nBenchmarking ($iterations iterations)...")
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
        val throughput = (ds.size.toLong * ds.size * ds.size) / (avgTime / 1000.0)
        
        println(f"\nAverage: ${avgTime}%,.2f ms +/- ${stdDev}%.2f ms")
        println(f"Throughput: ${throughput}%,.0f ops/sec")
        
        results += BenchmarkResult(
          "SpMM", ds.size, 0.95, "Custom",
          avgTime.toLong, throughput, 0.0
        )
        
      } catch {
        case e: Exception =>
          println(s"\nERROR: Could not complete SpMM benchmark for ${ds.size}x${ds.size}")
          println(s"  ${e.getMessage}")
      }
    }
    
    results.toSeq
  }
  
  def runFormatComparisonBenchmark(
    sc: SparkContext,
    dataDir: String = "synthetic-data",
    targetSize: Option[Int] = None,
    iterations: Int = 5
  ): Map[String, Double] = {
    
    println("\n" + "="*80)
    println("FORMAT COMPARISON: COO vs CSR")
    println("="*80)
    
    val datasets = DatasetDiscovery.discoverAllDatasets(dataDir)
    
    val selectedDataset = targetSize match {
      case Some(size) => datasets.find(_.size == size)
      case None => {
        val large = datasets.filter(ds => ds.category == "Large" || ds.category == "Extra-Large")
        if (large.nonEmpty) {
          println("Using LARGE dataset for format comparison")
          Some(large.maxBy(_.size))
        } else {
          println("No large datasets found, using Medium")
          datasets.find(_.category == "Medium")
        }
      }
    }
    
    if (selectedDataset.isEmpty) {
      println("ERROR: No suitable dataset found")
      return Map.empty
    }
    
    val ds = selectedDataset.get
    
    val matrixPath = s"$dataDir/${ds.matrixFile}"
    val vectorPath = s"$dataDir/${ds.vectorFile}"
    
    println(s"\nDataset: ${ds.size}x${ds.size} (${ds.fileSizeMB}%.1f MB, ${ds.category})")
    
    val matrixCOO = SmartLoader.loadMatrix(sc, matrixPath).toCOO
    val matrixCSR = matrixCOO.toCSR
    val vector = SmartLoader.loadVector(sc, vectorPath)
    
    println(s"Non-zeros: ${matrixCOO.numNonZeros}")
    
    val actualIterations = if (ds.size > 20000) 3 else iterations
    
    println(s"\n--- COO Format ($actualIterations iterations) ---")
    val cooTimes = ArrayBuffer[Long]()
    
    for (i <- 1 to actualIterations) {
      System.gc()
      Thread.sleep(1000)
      
      val start = System.nanoTime()
      val result = matrixCOO * vector
      result.entries.count()
      val elapsed = (System.nanoTime() - start) / 1000000
      
      cooTimes += elapsed
      println(f"  Iteration $i: ${elapsed}%,6d ms")
    }
    
    val avgCOO = cooTimes.sum / actualIterations.toDouble
    println(f"\nCOO Average: ${avgCOO}%,.2f ms")
    
    println(s"\n--- CSR Format ($actualIterations iterations) ---")
    val csrTimes = ArrayBuffer[Long]()
    
    for (i <- 1 to actualIterations) {
      System.gc()
      Thread.sleep(1000)
      
      val start = System.nanoTime()
      val result = matrixCSR * vector
      result.entries.count()
      val elapsed = (System.nanoTime() - start) / 1000000
      
      csrTimes += elapsed
      println(f"  Iteration $i: ${elapsed}%,6d ms")
    }
    
    val avgCSR = csrTimes.sum / actualIterations.toDouble
    val speedup = avgCOO / avgCSR
    
    println(f"\nCSR Average: ${avgCSR}%,.2f ms")
    println(f"\nCSR is ${speedup}%.2fx vs COO")
    
    if (speedup > 1.0) {
      println(f"CSR format shows ${(speedup - 1.0) * 100}%.1f%% improvement at this scale")
    }
    
    Map(
      "COO" -> avgCOO,
      "CSR" -> avgCSR,
      "speedup" -> speedup,
      "dataset_size" -> ds.size.toDouble
    )
  }
  
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
    println("\n" + "="*80)
    println("SUMMARY: SpMV Benchmarks")
    println("="*80)
    
    val grouped = results.groupBy(_.matrixSize)
    
    println("\n| Size      | Custom (ms) | DataFrame (ms) | Speedup |")
    println("|-----------|-------------|----------------|---------|")
    
    for ((size, group) <- grouped.toSeq.sortBy(_._1)) {
      val custom = group.find(_.implementation == "Custom")
      val baseline = group.find(_.implementation == "DataFrame")
      
      if (custom.isDefined && baseline.isDefined) {
        val speedup = baseline.get.executionTimeMs.toDouble / custom.get.executionTimeMs
        println(f"| ${size}x${size}%-9s | ${custom.get.executionTimeMs}%,11d | ${baseline.get.executionTimeMs}%,14d | ${speedup}%7.2fx |")
      } else if (custom.isDefined) {
        println(f"| ${size}x${size}%-9s | ${custom.get.executionTimeMs}%,11d | N/A            | N/A     |")
      }
    }
    
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