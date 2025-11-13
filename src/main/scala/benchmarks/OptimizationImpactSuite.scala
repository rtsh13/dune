package benchmarks

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import engine.storage._
import engine.storage.CSCFormat._
import engine.optimization.OptimizationStrategies
import scala.collection.mutable.ArrayBuffer

object OptimizationImpactSuite {

  case class OptimizationResult(
    format: String,
    operation: String,
    optimization: String,
    size: Int,
    timeMs: Double,
    speedup: Double
  )

  case class OptimizationResults(
    byFormat: Map[String, Seq[OptimizationResult]],
    bestPerOperation: Map[(String, String), (String, Double)],
    scalability: Seq[(Int, Double, Double)],
    bestOverall: String,
    avgEfficiency: Double
  )

  def runOptimizationImpact(
    sc: SparkContext,
    datasets: Seq[DatasetDiscovery.DatasetInfo],
    iterations: Int = 3
  ): OptimizationResults = {

    println("Running Optimization Impact Analysis...")
    

    val allResults = ArrayBuffer[OptimizationResult]()

    // Select appropriate datasets
    val testDatasets = selectTestDatasets(datasets)

    for (dataset <- testDatasets) {
      println(s"\n\nTesting: ${dataset.size}x${dataset.size}")
      println("-" * 80)

      val matrixPath = s"synthetic-data/${dataset.matrixFile}"
      val vectorPath = s"synthetic-data/${dataset.vectorFile}"

      // Load data
      val cooMatrix = SmartLoader.loadMatrix(sc, matrixPath).toCOO
      val vector = SmartLoader.loadVector(sc, vectorPath)

      println(s"Loaded: ${cooMatrix.numNonZeros} non-zeros")

      // Convert formats
      val csrMatrix = cooMatrix.toCSR
      val cscMatrix = CSCFormat.cooToDistributedCSC(
        cooMatrix.entries, cooMatrix.numRows, cooMatrix.numCols
      )

      // Test COO optimizations
      println("\n### COO Format Optimizations ###")
      val cooResults = testCOOOptimizations(cooMatrix, vector, dataset.size, iterations)
      allResults ++= cooResults

      // Test CSR optimizations
      println("\n### CSR Format Optimizations ###")
      val csrResults = testCSROptimizations(csrMatrix, vector, dataset.size, iterations)
      allResults ++= csrResults

      // Test CSC optimizations
      println("\n### CSC Format Optimizations ###")
      val cscResults = testCSCOptimizations(cscMatrix, vector, dataset.size, iterations)
      allResults ++= cscResults
    }

    // NEW: Test SpMM-Dense optimizations
    testSpMMDenseOptimizations(sc, allResults, iterations)

    // NEW: Test MTTKRP optimizations
    testMTTKRPOptimizations(sc, allResults, iterations)

    // Analyze results
    val byFormat = allResults.groupBy(_.format).map { case (format, results) =>
      (format, results.toSeq)
    }

    val bestPerOperation = allResults.groupBy(r => (r.format, r.operation))
      .map { case (key, results) =>
        val best = results.maxBy(_.speedup)
        (key, (best.optimization, best.speedup))
      }

    val bestOverall = allResults.maxBy(_.speedup).optimization

    // Scalability analysis (if we have multiple cores)
    val scalability = runScalabilityTest(sc, testDatasets.headOption, iterations)

    val avgEfficiency = if (scalability.nonEmpty) {
      scalability.map(_._3).sum / scalability.size
    } else 0.8

    OptimizationResults(
      byFormat,
      bestPerOperation,
      scalability,
      bestOverall,
      avgEfficiency
    )
  }

private def selectTestDatasets(
    datasets: Seq[DatasetDiscovery.DatasetInfo]
  ): Seq[DatasetDiscovery.DatasetInfo] = {
    // Select multiple datasets across all categories for comprehensive testing
    val byCategory: Map[String, Seq[DatasetDiscovery.DatasetInfo]] = datasets.groupBy(_.category)
    
    val selected = ArrayBuffer[DatasetDiscovery.DatasetInfo]()
    
    // Small: Take up to 2
    byCategory.get("Small").foreach { small =>
      selected ++= small.take(2)
    }
    
    // Medium: Take all (usually 1-2)
    byCategory.get("Medium").foreach { medium =>
      selected ++= medium
    }
    
    // Large: Take up to 2
    byCategory.get("Large").foreach { large =>
      selected ++= large.take(2)
    }
    
    // Extra-Large: Take at least 2 if exists
    byCategory.get("Extra-Large").foreach { xlarge =>
      selected ++= xlarge.take(2)
    }
    
    println(s"\n  Selected ${selected.size} datasets for optimization testing:")
    selected.foreach { ds =>
      println(s"    - ${ds.category}: ${ds.size}x${ds.size}")
    }
    
    selected.toSeq
  }

  private def testCOOOptimizations(
    cooMatrix: SparseMatrixCOO,
    vector: Vector,
    size: Int,
    iterations: Int
  ): Seq[OptimizationResult] = {

    println("\n  Testing: COO Format Optimizations")
    val results = ArrayBuffer[OptimizationResult]()
    val vectorRDD = vector.toIndexValueRDD

    // Baseline
    println("    Baseline")
    val baselineTime = benchmarkOpt("Baseline", iterations) {
      OptimizationStrategies.cooSpMV_Baseline(cooMatrix.entries, vectorRDD)
    }
    results += OptimizationResult("COO", "SpMV", "Baseline", size, baselineTime, 1.0)

    // Co-Partitioning
    println("    Co-Partitioning")
    val copartTime = benchmarkOpt("Co-Partitioning", iterations) {
      OptimizationStrategies.cooSpMV_CoPartitioning(cooMatrix.entries, vectorRDD)
    }
    results += OptimizationResult("COO", "SpMV", "CoPartitioning", size, copartTime, baselineTime / copartTime)

    // In-Partition Aggregation
    println("    In-Partition Aggregation")
    val inpartTime = benchmarkOpt("In-Partition Agg", iterations) {
      OptimizationStrategies.cooSpMV_InPartitionAgg(cooMatrix.entries, vectorRDD)
    }
    results += OptimizationResult("COO", "SpMV", "InPartitionAgg", size, inpartTime, baselineTime / inpartTime)

    // Balanced
    println("    Balanced")
    val balancedTime = benchmarkOpt("Balanced", iterations) {
      OptimizationStrategies.cooSpMV_Balanced(cooMatrix.entries, vectorRDD)
    }
    results += OptimizationResult("COO", "SpMV", "Balanced", size, balancedTime, baselineTime / balancedTime)

    // Caching
    println("    Caching")
    val cachingTime = benchmarkOpt("Caching", iterations) {
      OptimizationStrategies.cooSpMV_Caching(cooMatrix.entries, vectorRDD)
    }
    results += OptimizationResult("COO", "SpMV", "Caching", size, cachingTime, baselineTime / cachingTime)

    // Print summary
    println("\n  COO Optimization Summary:")
    println(f"    Baseline: ${baselineTime}%.2f ms")
    results.foreach { r =>
      if (r.optimization != "Baseline") {
        val improvement = ((baselineTime - r.timeMs) / baselineTime) * 100
        println(f"    ${r.optimization}: ${r.timeMs}%.2f ms (${r.speedup}%.2fx, ${improvement}%+.1f%%)")
      }
    }

    results.toSeq
  }

  private def testCSROptimizations(
    csrMatrix: SparseMatrixCSR,
    vector: Vector,
    size: Int,
    iterations: Int
  ): Seq[OptimizationResult] = {

    println("\n  Testing: CSR Format Optimizations")
    val results = ArrayBuffer[OptimizationResult]()
    val vectorRDD = vector.toIndexValueRDD

    // Baseline
    println("    Baseline")
    val baselineTime = benchmarkOpt("Baseline", iterations) {
      OptimizationStrategies.csrSpMV_Baseline(csrMatrix.rows, vectorRDD)
    }
    results += OptimizationResult("CSR", "SpMV", "Baseline", size, baselineTime, 1.0)

    // Co-Partitioning
    println("    Co-Partitioning")
    val copartTime = benchmarkOpt("Co-Partitioning", iterations) {
      OptimizationStrategies.csrSpMV_CoPartitioning(csrMatrix.rows, vectorRDD)
    }
    results += OptimizationResult("CSR", "SpMV", "CoPartitioning", size, copartTime, baselineTime / copartTime)

    // In-Partition Aggregation
    println("    In-Partition Aggregation")
    val inpartTime = benchmarkOpt("In-Partition Agg", iterations) {
      OptimizationStrategies.csrSpMV_InPartitionAgg(csrMatrix.rows, vectorRDD)
    }
    results += OptimizationResult("CSR", "SpMV", "InPartitionAgg", size, inpartTime, baselineTime / inpartTime)

    // Row-Wise Optimized (CSR advantage)
    println("    Row-Wise Optimized")
    val rowwiseTime = benchmarkOpt("Row-Wise", iterations) {
      OptimizationStrategies.csrSpMV_RowWiseOptimized(csrMatrix.rows, vectorRDD)
    }
    results += OptimizationResult("CSR", "SpMV", "RowWiseOptimized", size, rowwiseTime, baselineTime / rowwiseTime)

    // Print summary
    println("\n  CSR Optimization Summary:")
    println(f"    Baseline: ${baselineTime}%.2f ms")
    results.foreach { r =>
      if (r.optimization != "Baseline") {
        val improvement = ((baselineTime - r.timeMs) / baselineTime) * 100
        println(f"    ${r.optimization}: ${r.timeMs}%.2f ms (${r.speedup}%.2fx, ${improvement}%+.1f%%)")
      }
    }

    results.toSeq
  }

  private def testCSCOptimizations(
    cscMatrix: RDD[CSCColumn],
    vector: Vector,
    size: Int,
    iterations: Int
  ): Seq[OptimizationResult] = {

    println("\n  Testing: CSC Format Optimizations")
    val results = ArrayBuffer[OptimizationResult]()
    val vectorRDD = vector.toIndexValueRDD

    // Baseline
    println("    Baseline")
    val baselineTime = benchmarkOpt("Baseline", iterations) {
      OptimizationStrategies.cscSpMV_Baseline(cscMatrix, vectorRDD)
    }
    results += OptimizationResult("CSC", "SpMV", "Baseline", size, baselineTime, 1.0)

    // Co-Partitioning
    println("    Co-Partitioning")
    val copartTime = benchmarkOpt("Co-Partitioning", iterations) {
      OptimizationStrategies.cscSpMV_CoPartitioning(cscMatrix, vectorRDD)
    }
    results += OptimizationResult("CSC", "SpMV", "CoPartitioning", size, copartTime, baselineTime / copartTime)

    // In-Partition Aggregation
    println("    In-Partition Aggregation")
    val inpartTime = benchmarkOpt("In-Partition Agg", iterations) {
      OptimizationStrategies.cscSpMV_InPartitionAgg(cscMatrix, vectorRDD)
    }
    results += OptimizationResult("CSC", "SpMV", "InPartitionAgg", size, inpartTime, baselineTime / inpartTime)

    // Column-Wise Optimized (CSC advantage)
    println("    Column-Wise Optimized")
    val colwiseTime = benchmarkOpt("Column-Wise", iterations) {
      OptimizationStrategies.cscSpMV_ColumnWiseOptimized(cscMatrix, vectorRDD)
    }
    results += OptimizationResult("CSC", "SpMV", "ColumnWiseOptimized", size, colwiseTime, baselineTime / colwiseTime)

    // Print summary
    println("\n  CSC Optimization Summary:")
    println(f"    Baseline: ${baselineTime}%.2f ms")
    results.foreach { r =>
      if (r.optimization != "Baseline") {
        val improvement = ((baselineTime - r.timeMs) / baselineTime) * 100
        println(f"    ${r.optimization}: ${r.timeMs}%.2f ms (${r.speedup}%.2fx, ${improvement}%+.1f%%)")
      }
    }

    results.toSeq
  }

  private def testSpMMDenseOptimizations(
    sc: SparkContext,
    allResults: ArrayBuffer[OptimizationResult],
    iterations: Int
  ): Unit = {

    val matrixPath = "synthetic-data/sparse_matrix_100x100.csv"
    val densePath = "synthetic-data/dense_matrix_100x10.csv"

    if (new java.io.File(matrixPath).exists() && new java.io.File(densePath).exists()) {
      println("\n\n### SpMM-Dense Optimizations ###")
      println("-" * 80)

      val cooMatrix = SmartLoader.loadMatrix(sc, matrixPath).toCOO
      val denseMatrix = sc.textFile(densePath)
        .filter(!_.startsWith("row"))
        .map { line =>
          val parts = line.split(",")
          (parts(0).toInt, parts(1).toInt, parts(2).toDouble)
        }

      println("Testing COO SpMM-Dense optimizations...")

      // Baseline
      println("  Baseline")
      val baselineTime = benchmarkOpt("Baseline", iterations) {
        OptimizationStrategies.cooSpMMDense_Baseline(cooMatrix.entries, denseMatrix)
      }
      allResults += OptimizationResult("COO", "SpMM-Dense", "Baseline", 100, baselineTime, 1.0)

      // Co-Partitioning
      println("  CoPartitioning")
      val copartTime = benchmarkOpt("CoPartitioning", iterations) {
        OptimizationStrategies.cooSpMMDense_CoPartitioning(cooMatrix.entries, denseMatrix)
      }
      allResults += OptimizationResult("COO", "SpMM-Dense", "CoPartitioning", 100, copartTime, baselineTime / copartTime)

      // In-Partition Aggregation
      println("  InPartitionAgg")
      val inpartTime = benchmarkOpt("InPartitionAgg", iterations) {
        OptimizationStrategies.cooSpMMDense_InPartitionAgg(cooMatrix.entries, denseMatrix)
      }
      allResults += OptimizationResult("COO", "SpMM-Dense", "InPartitionAgg", 100, inpartTime, baselineTime / inpartTime)

      // Block-Partitioned
      println("  BlockPartitioned")
      val blockTime = benchmarkOpt("BlockPartitioned", iterations) {
        OptimizationStrategies.cooSpMMDense_BlockPartitioned(cooMatrix.entries, denseMatrix, 50)
      }
      allResults += OptimizationResult("COO", "SpMM-Dense", "BlockPartitioned", 100, blockTime, baselineTime / blockTime)

      // Caching
      println("  Caching")
      val cachingTime = benchmarkOpt("Caching", iterations) {
        OptimizationStrategies.cooSpMMDense_Caching(cooMatrix.entries, denseMatrix)
      }
      allResults += OptimizationResult("COO", "SpMM-Dense", "Caching", 100, cachingTime, baselineTime / cachingTime)

      println(f"\n  SpMM-Dense Summary:")
      println(f"    Baseline:         $baselineTime%.2f ms")
      println(f"    CoPartitioning:   $copartTime%.2f ms (${baselineTime/copartTime}%.2fx)")
      println(f"    InPartitionAgg:   $inpartTime%.2f ms (${baselineTime/inpartTime}%.2fx)")
      println(f"    BlockPartitioned: $blockTime%.2f ms (${baselineTime/blockTime}%.2fx)")
      println(f"    Caching:          $cachingTime%.2f ms (${baselineTime/cachingTime}%.2fx)")
    } else {
      println("\n\nSkipping SpMM-Dense optimizations: data files not found")
      println(s"  Expected: $matrixPath and $densePath")
      println(s"  Run: python3 generate_tensor_data.py")
    }
  }

  private def testMTTKRPOptimizations(
    sc: SparkContext,
    allResults: ArrayBuffer[OptimizationResult],
    iterations: Int
  ): Unit = {

    val tensorPath = "synthetic-data/sparse_tensor_50x50x50.csv"

    if (new java.io.File(tensorPath).exists()) {
      println("\n\n### MTTKRP Optimizations ###")
      println("-" * 80)

      val tensorEntries = sc.textFile(tensorPath)
        .filter(!_.startsWith("i"))
        .map { line =>
          val parts = line.split(",")
          engine.tensor.TensorEntry(
            Array(parts(0).toInt, parts(1).toInt, parts(2).toInt),
            parts(3).toDouble
          )
        }

      val factorMatrices = Array(
        sc.parallelize((0 until 50).map(i => (i, Array.fill(10)(scala.util.Random.nextDouble())))),
        sc.parallelize((0 until 50).map(i => (i, Array.fill(10)(scala.util.Random.nextDouble())))),
        sc.parallelize((0 until 50).map(i => (i, Array.fill(10)(scala.util.Random.nextDouble()))))
      )

      println("Testing COO MTTKRP optimizations...")

      // Baseline
      println("  Baseline")
      val baselineTime = benchmarkOpt("Baseline", iterations) {
        OptimizationStrategies.cooMTTKRP_Baseline(tensorEntries, factorMatrices, 0)
      }
      allResults += OptimizationResult("COO", "MTTKRP", "Baseline", 50, baselineTime, 1.0)

      // Co-Partitioning
      println("  CoPartitioning")
      val copartTime = benchmarkOpt("CoPartitioning", iterations) {
        OptimizationStrategies.cooMTTKRP_CoPartitioning(tensorEntries, factorMatrices, 0)
      }
      allResults += OptimizationResult("COO", "MTTKRP", "CoPartitioning", 50, copartTime, baselineTime / copartTime)

      // In-Partition Aggregation
      println("  InPartitionAgg")
      val inpartTime = benchmarkOpt("InPartitionAgg", iterations) {
        OptimizationStrategies.cooMTTKRP_InPartitionAgg(tensorEntries, factorMatrices, 0)
      }
      allResults += OptimizationResult("COO", "MTTKRP", "InPartitionAgg", 50, inpartTime, baselineTime / inpartTime)

      println(f"\n  MTTKRP Summary:")
      println(f"    Baseline:       $baselineTime%.2f ms")
      println(f"    CoPartitioning: $copartTime%.2f ms (${baselineTime/copartTime}%.2fx)")
      println(f"    InPartitionAgg: $inpartTime%.2f ms (${baselineTime/inpartTime}%.2fx)")
    } else {
      println("\n\nSkipping MTTKRP optimizations: tensor data not found")
      println(s"  Expected: $tensorPath")
      println(s"  Run: python3 generate_tensor_data.py")
    }
  }

  private def runScalabilityTest(
    sc: SparkContext,
    datasetOpt: Option[DatasetDiscovery.DatasetInfo],
    iterations: Int
  ): Seq[(Int, Double, Double)] = {

    datasetOpt match {
      case None => Seq.empty
      case Some(dataset: DatasetDiscovery.DatasetInfo) =>
        println("\n\n### Scalability Test ###")
        println(s"Testing with ${dataset.size}x${dataset.size} matrix")

        val matrixPath = s"synthetic-data/${dataset.matrixFile}"
        val vectorPath = s"synthetic-data/${dataset.vectorFile}"

        val cooMatrix = SmartLoader.loadMatrix(sc, matrixPath).toCOO
        val vector = SmartLoader.loadVector(sc, vectorPath)

        val results = ArrayBuffer[(Int, Double, Double)]()
        val parallelismLevels = Seq(1, 2, 4, 8).filter(_ <= sc.defaultParallelism)

        var baselineTime = 0.0

        for (parallelism <- parallelismLevels) {
          println(s"\n  Testing with parallelism: $parallelism")
          
          // Create RDD with specific parallelism
          val repartitionedMatrix = cooMatrix.entries.repartition(parallelism)
          val repartitionedVector = vector.toIndexValueRDD.repartition(parallelism)

          val time = benchmarkOpt(s"Parallelism $parallelism", iterations) {
            OptimizationStrategies.cooSpMV_CoPartitioning(repartitionedMatrix, repartitionedVector)
          }

          if (baselineTime == 0.0) baselineTime = time
          val speedup = baselineTime / time
          val efficiency = speedup / parallelism

          results += ((parallelism, time, efficiency))

          println(f"    Time: ${time}%.2f ms")
          println(f"    Speedup: ${speedup}%.2fx")
          println(f"    Efficiency: ${efficiency * 100}%.1f%%")
        }

        results.toSeq
    }
  }

  private def benchmarkOpt(
    name: String,
    iterations: Int
  )(operation: => RDD[_]): Double = {

    val times = ArrayBuffer[Double]()

    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(500)

      val start = System.nanoTime()
      operation.count()
      val elapsed = (System.nanoTime() - start) / 1000000.0

      times += elapsed
    }

    val avg = times.sum / iterations
    println(f"      Average: ${avg}%.2f ms")
    avg
  }
}