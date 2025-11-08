package benchmarks


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import engine.storage._

object RunAllBenchmarks {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
      .setAppName("Sparse Matrix Benchmarks")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.memory", "4g")
    
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
    sc.setLogLevel("WARN")
    
    try {
      // Log setup
      println("\n" + "="*80)
      println("BENCHMARK ENVIRONMENT SETUP")
      println("="*80)
      BenchmarkSetup.logEnvironment(sc)
      
      // Verify data exists (instead of generating)
      println("\n" + "="*80)
      println("VERIFYING DATA FILES")
      println("="*80)
      verifyDataExists("synthetic-data", Seq(10, 100, 1000))
      
      // Run microbenchmarks
      println("\n" + "="*80)
      println("RUNNING MICROBENCHMARKS")
      println("="*80)
      
      val microResults = MicroBenchmarks.runSpMVBenchmarks(
        sc, spark,
        dataDir = "synthetic-data",
        sizes = Seq(10, 100, 1000),  // Use sizes you have
        iterations = 5
      )
      
      // Run optimization comparison
      println("\n" + "="*80)
      println("RUNNING OPTIMIZATION COMPARISON")
      println("="*80)
      
      val optimizationResults = MicroBenchmarks.runOptimizationComparison(
        sc,
        dataDir = "synthetic-data",
        size = 1000,
        iterations = 5
      )
      
      // Run format comparison (COO vs CSR)
      println("\n" + "="*80)
      println("RUNNING FORMAT COMPARISON")
      println("="*80)
      
      val formatResults = MicroBenchmarks.runFormatComparisonBenchmark(
        sc,
        dataDir = "synthetic-data",
        size = 1000,
        iterations = 5
      )
      
      // Run scalability tests
      println("\n" + "="*80)
      println("RUNNING SCALABILITY TESTS")
      println("="*80)
      
val scalabilityResults = ScalabilityBenchmarks.runScalabilityTest(
  conf,
  matrixSize = 1000,
  sparsity = 0.85,
  dataDir = "synthetic-data",
  workerCounts = Seq(2, 4, 8, 16)
)
      
      // Run SpMM benchmarks
      println("\n" + "="*80)
      println("RUNNING SpMM BENCHMARKS")
      println("="*80)
      
      val spmmResults = MicroBenchmarks.runSpMMBenchmarks(
        sc, spark,
        dataDir = "synthetic-data",
        sizes = Seq(10, 100),
        iterations = 3
      )
      
      // Run ablation studies
      println("\n" + "="*80)
      println("RUNNING ABLATION STUDIES")
      println("="*80)
      
      val ablationResults = AblationStudy.runAblationStudy(
        sc,
        matrixSize = 1000,
        sparsity = 0.85,
        dataDir = "synthetic-data",
        iterations = 3
      )
      
      // Run end-to-end benchmarks
      println("\n" + "="*80)
      println("RUNNING END-TO-END BENCHMARKS")
      println("="*80)
      
      EndToEndBenchmarks.benchmarkIterativeAlgorithm(
        sc,
        matrixSize = 1000,
        dataDir = "synthetic-data",
        iterations = 10
      )
      
      EndToEndBenchmarks.benchmarkMatrixChain(
        sc,
        size = 100,
        dataDir = "synthetic-data"
      )
      
      // Generate reports
      println("\n" + "="*80)
      println("GENERATING REPORTS")
      println("="*80)
      
      // Create results directory
      new java.io.File("results").mkdirs()
      
      val allResults = microResults ++ spmmResults
      
      ResultsAnalyzer.generateReport(allResults, "results/benchmark_report.md")
      ResultsAnalyzer.generateCSV(allResults, "results/benchmark_results.csv")
      
      println("\n✓ Reports generated:")
      println("  - results/benchmark_report.md")
      println("  - results/benchmark_results.csv")
      
      println("\n" + "="*80)
      println("ALL BENCHMARKS COMPLETE")
      println("="*80)
      println("\nNext steps:")
      println("  1. Review results/benchmark_report.md")
      println("  2. Analyze results/benchmark_results.csv")
      println("  3. Generate plots using Python/R")
      
    } catch {
      case e: Exception =>
        println("\n" + "="*80)
        println("ERROR: BENCHMARK FAILED")
        println("="*80)
        println(s"Message: ${e.getMessage}")
        println("\nStack trace:")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
      sc.stop()
    }
  }
  
  /**
   * Verify that required data files exist before running benchmarks
   */
  private def verifyDataExists(dataDir: String, sizes: Seq[Int]): Unit = {
    println(s"\nChecking for data files in $dataDir/...")
    
    var allExist = true
    var filesChecked = 0
    var filesFound = 0
    
    for (size <- sizes) {
      val matrixPath = s"$dataDir/sparse_matrix_${size}x${size}.csv"
      val vectorPath = s"$dataDir/dense_vector_${size}.csv"
      
      filesChecked += 2
      
      val matrixFile = new java.io.File(matrixPath)
      val vectorFile = new java.io.File(vectorPath)
      
      val matrixExists = matrixFile.exists()
      val vectorExists = vectorFile.exists()
      
      if (matrixExists) filesFound += 1
      if (vectorExists) filesFound += 1
      
      if (matrixExists && vectorExists) {
        val matrixSize = matrixFile.length() / (1024.0 * 1024.0)
        val vectorSize = vectorFile.length() / (1024.0 * 1024.0)
        println(f"  ✓ ${size}x${size}: matrix (${matrixSize}%.2f MB) and vector (${vectorSize}%.2f MB)")
      } else {
        println(s"  ✗ ${size}x${size}: MISSING FILES")
        if (!matrixExists) println(s"      Missing: $matrixPath")
        if (!vectorExists) println(s"      Missing: $vectorPath")
        allExist = false
      }
    }
    
    println(f"\nFiles found: $filesFound/$filesChecked")
    
    if (!allExist) {
      println("\n" + "="*80)
      println("ERROR: MISSING DATA FILES")
      println("="*80)
      println("\nPlease generate the required data files by running:")
      println("  python synthMatrices.py")
      println("  python synthVectors.py")
      println("\nOr generate specific sizes:")
      println("  python")
      println("  >>> import numpy as np")
      println("  >>> import pandas as pd")
      println("  >>> import scipy.sparse as sp")
      println("  >>> # Generate your matrices here")
      throw new RuntimeException("Missing required data files")
    }
    
    println("\n✓ All required data files verified")
  }
}