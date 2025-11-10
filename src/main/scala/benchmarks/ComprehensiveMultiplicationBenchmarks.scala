package benchmarks

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import engine.storage._
import engine.operations.MultiplicationOps
import engine.optimization.OptimizedOps
import scala.collection.mutable.ArrayBuffer

object ComprehensiveMultiplicationBenchmarks {
  
  /**
   * Test ALL 5 multiplication operations
   */
  def testAllMultiplicationOperations(
    sc: SparkContext,
    dataDir: String = "synthetic-data",
    size: Int = 1000,
    iterations: Int = 5
  ): Map[String, (Double, Long)] = {
    
    println("COMPREHENSIVE MULTIPLICATION OPERATIONS TEST")
    println("Testing ALL 5 multiplication operations")
    
    val results = scala.collection.mutable.Map[String, (Double, Long)]()
    
    // Load data
    val matrixCOO = SmartLoader.loadMatrix(sc, s"$dataDir/sparse_matrix_${size}x${size}.csv").toCOO
    val denseVector = SmartLoader.loadVector(sc, s"$dataDir/dense_vector_${size}.csv", forceDense = true)
    val sparseVector = SmartLoader.loadVector(sc, s"$dataDir/sparse_vector_${size}.csv", forceSparse = true)
    val denseMatrix = SmartLoader.loadMatrix(sc, s"$dataDir/dense_matrix_${size}x100.csv", forceDense = true)
    val sparseMatrix = SmartLoader.loadMatrix(sc, s"$dataDir/sparse_matrix_${size}x${size}.csv").toCOO
    val csrMatrix = matrixCOO.toCSR
    
    println(s"\nLoaded:")
    println(s"  - COO Matrix: $matrixCOO")
    println(s"  - CSR Matrix: $csrMatrix")
    println(s"  - Dense Vector: $denseVector")
    println(s"  - Sparse Vector: $sparseVector")
    println(s"  - Dense Matrix: $denseMatrix")
    println(s"  - Sparse Matrix: $sparseMatrix")
    
    // Operation 1: Sparse Matrix × Dense Vector
    println("\n### Operation 1: Sparse Matrix × Dense Vector ###")
    val op1Times = ArrayBuffer[Long]()
    
    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(500)
      
      val start = System.nanoTime()
      val result = MultiplicationOps.sparseMatrixDenseVector(
        matrixCOO.entries,
        denseVector.toIndexValueRDD
      )
      val count = result.count()
      val elapsed = (System.nanoTime() - start) / 1000000
      
      op1Times += elapsed
      println(f"  Iteration $i: ${elapsed}ms, result: $count entries")
    }
    
    val avgOp1 = op1Times.sum.toDouble / iterations
    println(f"  Average: ${avgOp1}%.2f ms")
    results("SpM × DenseVec") = (avgOp1, op1Times.head)
    
    // Operation 2: Sparse Matrix × Sparse Vector
    println("\n### Operation 2: Sparse Matrix × Sparse Vector ###")
    val op2Times = ArrayBuffer[Long]()
    
    val sparseVecRDD = sparseVector match {
      case sv: SparseVector => sv.entries
      case _ => throw new RuntimeException("Expected sparse vector")
    }
    
    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(500)
      
      val start = System.nanoTime()
      val result = MultiplicationOps.sparseMatrixSparseVector(
        matrixCOO.entries,
        sparseVecRDD
      )
      val count = result.count()
      val elapsed = (System.nanoTime() - start) / 1000000
      
      op2Times += elapsed
      println(f"  Iteration $i: ${elapsed}ms, result: $count entries")
    }
    
    val avgOp2 = op2Times.sum.toDouble / iterations
    println(f"  Average: ${avgOp2}%.2f ms")
    results("SpM × SparseVec") = (avgOp2, op2Times.head)
    
    // Operation 3: Sparse Matrix × Dense Matrix
    println("\n### Operation 3: Sparse Matrix × Dense Matrix ###")
    val op3Times = ArrayBuffer[Long]()
    
    val denseMatRDD = denseMatrix match {
      case dm: DenseMatrixRDD => dm.entries
      case _ => denseMatrix.toCOO.entries.map(e => (e.row, e.col, e.value))
    }
    
    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(500)
      
      val start = System.nanoTime()
      val result = MultiplicationOps.sparseMatrixDenseMatrix(
        matrixCOO.entries,
        denseMatRDD
      )
      val count = result.count()
      val elapsed = (System.nanoTime() - start) / 1000000
      
      op3Times += elapsed
      println(f"  Iteration $i: ${elapsed}ms, result: $count entries")
    }
    
    val avgOp3 = op3Times.sum.toDouble / iterations
    println(f"  Average: ${avgOp3}%.2f ms")
    results("SpM × DenseMat") = (avgOp3, op3Times.head)
    
    // Operation 4: Sparse Matrix × Sparse Matrix
    println("\n### Operation 4: Sparse Matrix × Sparse Matrix ###")
    val op4Times = ArrayBuffer[Long]()
    
    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(500)
      
      val start = System.nanoTime()
      val result = MultiplicationOps.sparseMatrixSparseMatrix(
        matrixCOO.entries,
        sparseMatrix.entries
      )
      val count = result.count()
      val elapsed = (System.nanoTime() - start) / 1000000
      
      op4Times += elapsed
      println(f"  Iteration $i: ${elapsed}ms, result: $count entries")
    }
    
    val avgOp4 = op4Times.sum.toDouble / iterations
    println(f"  Average: ${avgOp4}%.2f ms")
    results("SpM × SparseMat") = (avgOp4, op4Times.head)
    
    // Operation 5: CSR Matrix × Dense Vector (Optimized)
    println("\n### Operation 5: CSR Matrix × Dense Vector ###")
    val op5Times = ArrayBuffer[Long]()
    
    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(500)
      
      val start = System.nanoTime()
      val result = MultiplicationOps.csrMatrixDenseVector(
        csrMatrix.rows,
        denseVector.toIndexValueRDD
      )
      val count = result.count()
      val elapsed = (System.nanoTime() - start) / 1000000
      
      op5Times += elapsed
      println(f"  Iteration $i: ${elapsed}ms, result: $count entries")
    }
    
    val avgOp5 = op5Times.sum.toDouble / iterations
    println(f"  Average: ${avgOp5}%.2f ms")
    results("CSR × DenseVec") = (avgOp5, op5Times.head)
    
    // Summary
    println("SUMMARY: All 5 Multiplication Operations")
    println("\n| Operation | Avg Time (ms) | Relative Speed |")
    println("|-----------|---------------|----------------|")
    
    val fastest = results.values.map(_._1).min
    results.toSeq.sortBy(_._2._1).foreach { case (name, (time, _)) =>
      val relSpeed = fastest / time
      println(f"| $name%-25s | ${time}%8.2f | ${relSpeed}%.2fx |")
    }
    
    // Compare SpMV variants
    println("\n### SpMV Comparison:")
    val cooVsDense = results("SpM × DenseVec")._1
    val cooVsSparse = results("SpM × SparseVec")._1
    val csrVsDense = results("CSR × DenseVec")._1
    
    println(f"  COO × DenseVec:   ${cooVsDense}%.2f ms (baseline)")
    println(f"  COO × SparseVec:  ${cooVsSparse}%.2f ms (${cooVsDense/cooVsSparse}%.2fx vs baseline)")
    println(f"  CSR × DenseVec:   ${csrVsDense}%.2f ms (${cooVsDense/csrVsDense}%.2fx vs COO)")
    
    results.toMap
  }
  
  /**
   * Test ALL optimization strategies
   */
  def testAllOptimizations(
    sc: SparkContext,
    dataDir: String = "synthetic-data",
    size: Int = 1000,
    iterations: Int = 3
  ): Map[String, Double] = {
    
    println("COMPREHENSIVE OPTIMIZATION STRATEGIES TEST")
    println("Testing ALL optimization approaches")
    
    val results = scala.collection.mutable.Map[String, Double]()
    
    val matrixA = SmartLoader.loadMatrix(sc, s"$dataDir/sparse_matrix_${size}x${size}.csv").toCOO
    val matrixB = SmartLoader.loadMatrix(sc, s"$dataDir/sparse_matrix_${size}x${size}.csv").toCOO
    val vector = SmartLoader.loadVector(sc, s"$dataDir/dense_vector_${size}.csv")
    
    println(s"\nMatrix A: ${matrixA.numNonZeros} non-zeros")
    println(s"Matrix B: ${matrixB.numNonZeros} non-zeros")
    
    // Strategy 1: Baseline (No optimization)
    println("\n### Strategy 1: Baseline (No Optimization) ###")
    val baselineTimes = ArrayBuffer[Long]()
    
    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(500)
      
      val start = System.nanoTime()
      val result = MultiplicationOps.sparseMatrixDenseVector(
        matrixA.entries,
        vector.toIndexValueRDD
      )
      result.count()
      val elapsed = (System.nanoTime() - start) / 1000000
      
      baselineTimes += elapsed
      println(f"  Iteration $i: ${elapsed}ms")
    }
    
    val avgBaseline = baselineTimes.sum.toDouble / iterations
    println(f"  Average: ${avgBaseline}%.2f ms")
    results("Baseline") = avgBaseline
    
    // Strategy 2: Partitioning Only
    println("\n### Strategy 2: Partitioning Only ###")
    val partTimes = ArrayBuffer[Long]()
    
    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(500)
      
      val start = System.nanoTime()
      val result = OptimizedOps.optimizedSpMV(
        matrixA.entries,
        vector.toIndexValueRDD,
        Some(sc.defaultParallelism)
      )
      result.count()
      val elapsed = (System.nanoTime() - start) / 1000000
      
      partTimes += elapsed
      println(f"  Iteration $i: ${elapsed}ms")
    }
    
    val avgPart = partTimes.sum.toDouble / iterations
    println(f"  Average: ${avgPart}%.2f ms")
    results("Partitioning") = avgPart
    
    // Strategy 3: Optimized SpMM
    println("\n### Strategy 3: Optimized SpMM (Partitioning + Caching) ###")
    val spmmTimes = ArrayBuffer[Long]()
    
    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(1000)
      
      val start = System.nanoTime()
      val result = OptimizedOps.optimizedSpMM(
        matrixA.entries,
        matrixB.entries,
        Some(16)
      )
      result.count()
      val elapsed = (System.nanoTime() - start) / 1000000
      
      spmmTimes += elapsed
      println(f"  Iteration $i: ${elapsed}ms")
    }
    
    val avgSpMM = spmmTimes.sum.toDouble / iterations
    println(f"  Average: ${avgSpMM}%.2f ms")
    results("Optimized SpMM") = avgSpMM
    
    // Strategy 4: Block-Partitioned SpMM
    println("\n### Strategy 4: Block-Partitioned SpMM ###")
    val blockTimes = ArrayBuffer[Long]()
    
    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(1000)
      
      val start = System.nanoTime()
      val result = OptimizedOps.blockPartitionedSpMM(
        matrixA.entries,
        matrixB.entries,
        blockSize = 100  // 100×100 blocks
      )
      result.count()
      val elapsed = (System.nanoTime() - start) / 1000000
      
      blockTimes += elapsed
      println(f"  Iteration $i: ${elapsed}ms")
    }
    
    val avgBlock = blockTimes.sum.toDouble / iterations
    println(f"  Average: ${avgBlock}%.2f ms")
    results("Block-Partitioned") = avgBlock
    
    // Strategy 5: Adaptive Partitioning
    println("\n### Strategy 5: Adaptive Partitioning ###")
    val optimalParts = OptimizedOps.computeOptimalPartitions(
      numEntries = matrixA.numNonZeros,
      numRows = matrixA.numRows,
      numCols = matrixA.numCols
    )
    
    val adaptiveTimes = ArrayBuffer[Long]()
    
    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(500)
      
      val start = System.nanoTime()
      val result = OptimizedOps.optimizedSpMV(
        matrixA.entries,
        vector.toIndexValueRDD,
        Some(optimalParts)
      )
      result.count()
      val elapsed = (System.nanoTime() - start) / 1000000
      
      adaptiveTimes += elapsed
      println(f"  Iteration $i: ${elapsed}ms")
    }
    
    val avgAdaptive = adaptiveTimes.sum.toDouble / iterations
    println(f"  Average: ${avgAdaptive}%.2f ms")
    results("Adaptive Partitioning") = avgAdaptive
    
    // Summary
    println("SUMMARY: All Optimization Strategies")
    println("\n| Strategy | Time (ms) | Speedup vs Baseline | Improvement |")
    println("|----------|-----------|---------------------|-------------|")
    
    results.toSeq.sortBy(_._2).foreach { case (name, time) =>
      val speedup = avgBaseline / time
      val improvement = ((avgBaseline - time) / avgBaseline) * 100
      println(f"| $name%-25s | ${time}%8.2f | ${speedup}%8.2fx | ${improvement}%6.1f%% |")
    }
    
    println("\n### Key Insights:")
    println(f"  - Best optimization: ${results.minBy(_._2)._1}")
    println(f"  - Best speedup: ${avgBaseline / results.values.min}%.2fx")
    println(f"  - Partitioning impact: ${((avgBaseline - avgPart) / avgBaseline) * 100}%+.1f%%")
    println(f"  - Block vs Standard: ${avgSpMM / avgBlock}%.2fx")
    
    results.toMap
  }
}

object ComprehensiveMultiplicationRunner {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
      .setAppName("Comprehensive Multiplication Tests")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.memory", "4g")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    
    try {
        println("COMPREHENSIVE MULTIPLICATION & OPTIMIZATION TESTS")
        
      // Test all 5 operations
      val opResults = ComprehensiveMultiplicationBenchmarks.testAllMultiplicationOperations(
        sc,
        dataDir = "synthetic-data",
        size = 1000,
        iterations = 5
      )
      
      // Test all optimizations
      val optResults = ComprehensiveMultiplicationBenchmarks.testAllOptimizations(
        sc,
        dataDir = "synthetic-data",
        size = 1000,
        iterations = 3
      )
      
      // Save results
      new java.io.File("results").mkdirs()
      val writer = new java.io.PrintWriter("results/comprehensive_operations_report.md")
      
      writer.println("# Comprehensive Multiplication Operations Test")
      writer.println()
      writer.println("## All 5 Multiplication Operations")
      writer.println()
      writer.println("| Operation | Time (ms) |")
      writer.println("|-----------|-----------|")
      opResults.toSeq.sortBy(_._2._1).foreach { case (name, (time, _)) =>
        writer.println(f"| $name | ${time}%.2f |")
      }
      
      writer.println()
      writer.println("## All Optimization Strategies")
      writer.println()
      writer.println("| Strategy | Time (ms) |")
      writer.println("|----------|-----------|")
      optResults.toSeq.sortBy(_._2).foreach { case (name, time) =>
        writer.println(f"| $name | ${time}%.2f |")
      }
      
      writer.close()
      
        println("✓ COMPREHENSIVE TESTS COMPLETE")
        println("\nReport: results/comprehensive_operations_report.md")
      
    } catch {
      case e: Exception =>
        println(s"\n✗ ERROR: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      sc.stop()
    }
  }
}