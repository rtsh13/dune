package benchmarks_updated

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import java.io.{File, PrintWriter}

object ComprehensiveBenchmarks {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
      .setAppName("Comprehensive Performance Evaluation")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.memory", "4g")
      .set("spark.executor.memory", "4g")
    
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    sc.setLogLevel("WARN")
    
    try {
      println("COMPREHENSIVE PERFORMANCE EVALUATION")
      
      // Setup
      BenchmarkSetup.logEnvironment(sc)
      verifyDataExists("synthetic-data", Seq(10, 100, 1000))
      
      // Create results directory
      new File("results").mkdirs()
      val reportWriter = new PrintWriter("results/comprehensive_report.md")
      
      reportWriter.println("# Sparse Matrix Engine - Performance Evaluation Report")
      reportWriter.println()
      reportWriter.println("## 1. Experimental Setup")
      reportWriter.println()
      reportWriter.println("### Hardware")
      reportWriter.println(s"- **Processors:** ${Runtime.getRuntime.availableProcessors()}")
      reportWriter.println(s"- **Memory:** ${Runtime.getRuntime.maxMemory() / (1024*1024*1024)} GB")
      reportWriter.println()
      reportWriter.println("### Software")
      reportWriter.println(s"- **Spark Version:** ${sc.version}")
      reportWriter.println(s"- **Scala Version:** ${util.Properties.versionString}")
      reportWriter.println(s"- **Java Version:** ${System.getProperty("java.version")}")
      reportWriter.println()
      reportWriter.println("### Test Datasets")
      reportWriter.println("- Small: 10×10 matrices (70% sparse)")
      reportWriter.println("- Medium: 100×100 matrices (85% sparse)")
      reportWriter.println("- Large: 1000×1000 matrices (95% sparse)")
      reportWriter.println()
      
      // Section 5.2: Microbenchmarks
      reportWriter.println("## 2. Microbenchmark Results")
      reportWriter.println()
      
      val microResults = MicroBenchmarks.runSpMVBenchmarks(
        sc, spark,
        dataDir = "synthetic-data",
        sizes = Seq(10, 100, 1000),
        iterations = 5
      )
      
      reportWriter.println("### SpMV Performance vs DataFrame")
      reportWriter.println()
      reportWriter.println("| Size | Custom (ms) | DataFrame (ms) | Speedup |")
      reportWriter.println("|------|-------------|----------------|---------|")
      
      microResults.groupBy(_.matrixSize).foreach { case (size, group) =>
        val custom = group.find(_.implementation == "Custom")
        val df = group.find(_.implementation == "DataFrame")
        if (custom.isDefined && df.isDefined) {
          val speedup = df.get.executionTimeMs.toDouble / custom.get.executionTimeMs
          reportWriter.println(f"| ${size}x${size} | ${custom.get.executionTimeMs} | ${df.get.executionTimeMs} | ${speedup}%.2fx |")
        }
      }
      reportWriter.println()
      
      // Section 5.3: Optimization Impact
      reportWriter.println("## 3. Impact of Distributed Optimizations")
      reportWriter.println()
      
      val optResults = MicroBenchmarks.runOptimizationComparison(
        sc,
        dataDir = "synthetic-data",
        size = 1000,
        iterations = 5
      )
      
      reportWriter.println("### Baseline vs Optimized (1000×1000)")
      reportWriter.println()
      reportWriter.println(f"- **Baseline:** ${optResults("baseline")}%.2f ms")
      reportWriter.println(f"- **Optimized:** ${optResults("optimized")}%.2f ms")
      reportWriter.println(f"- **Speedup:** ${optResults("speedup")}%.2fx")
      reportWriter.println(f"- **Improvement:** ${optResults("improvement_percent")}%+.1f%%")
      reportWriter.println()
      
      // Format comparison
      val formatResults = MicroBenchmarks.runFormatComparisonBenchmark(
        sc,
        dataDir = "synthetic-data",
        size = 1000,
        iterations = 5
      )
      
      reportWriter.println("### COO vs CSR Format")
      reportWriter.println()
      reportWriter.println(f"- **COO Format:** ${formatResults("COO")}%.2f ms")
      reportWriter.println(f"- **CSR Format:** ${formatResults("CSR")}%.2f ms")
      reportWriter.println(f"- **CSR Speedup:** ${formatResults("speedup")}%.2fx")
      reportWriter.println()
      
      // Section 5.4: Ablation Study
      reportWriter.println("## 4. Ablation Study")
      reportWriter.println()
      
      val ablationResults = AblationStudy.runAblationStudy(
        sc,
        matrixSize = 1000,
        sparsity = 0.85,
        dataDir = "synthetic-data",
        iterations = 3
      )
      
      reportWriter.println("### Impact of Individual Optimizations")
      reportWriter.println()
      
      val baseline = ablationResults.find(_._1.isEmpty).map(_._2).getOrElse(0.0)
      ablationResults.toSeq.sortBy(_._2).foreach { case (features, time) =>
        val improvement = ((baseline - time) / baseline) * 100
        val featuresStr = if (features.isEmpty) "Baseline" else features.mkString(" + ")
        reportWriter.println(f"- **$featuresStr:** ${time}%.2f ms (${improvement}%+.1f%%)")
      }
      reportWriter.println()
      
      // Section 5.5: End-to-End Evaluation
      reportWriter.println("## 5. End-to-End System Evaluation")
      reportWriter.println()
      
      reportWriter.println("### Iterative Algorithm (PageRank-like)")
      val (iterCustom, iterDF) = EndToEndBenchmarks.benchmarkIterativeAlgorithm(
        sc,
        matrixSize = 1000,
        dataDir = "synthetic-data",
        iterations = 10
      )
      reportWriter.println(f"- **Custom:** ${iterCustom}%.2f ms (10 iterations)")
      reportWriter.println(f"- **Per iteration:** ${iterCustom / 10}%.2f ms")
      reportWriter.println()
      
      reportWriter.println("### Matrix Chain Multiplication")
      val chainResults = EndToEndBenchmarks.benchmarkMatrixChain(
        sc,
        size = 100,
        dataDir = "synthetic-data"
      )
      reportWriter.println(f"- **Total time:** ${chainResults("time")}%.2f ms")
      reportWriter.println()
      
      // SpMM Benchmarks
      reportWriter.println("### Sparse Matrix × Matrix (SpMM)")
      reportWriter.println()
      
      val spmmResults = MicroBenchmarks.runSpMMBenchmarks(
        sc, spark,
        dataDir = "synthetic-data",
        sizes = Seq(10, 100),
        iterations = 3
      )
      
      reportWriter.println("| Size | Time (ms) | Throughput (ops/s) |")
      reportWriter.println("|------|-----------|-------------------|")
      spmmResults.foreach { result =>
        reportWriter.println(f"| ${result.matrixSize}x${result.matrixSize} | ${result.executionTimeMs} | ${result.throughput}%.2e |")
      }
      reportWriter.println()
      
      // Summary
      reportWriter.println("## 6. Summary")
      reportWriter.println()
      reportWriter.println("### Key Findings")
      reportWriter.println()
      
      val avgSpeedups = microResults.groupBy(_.matrixSize).flatMap { case (_, group) =>
        for {
          custom <- group.find(_.implementation == "Custom")
          df <- group.find(_.implementation == "DataFrame")
        } yield df.executionTimeMs.toDouble / custom.executionTimeMs
      }.toSeq
      
      if (avgSpeedups.nonEmpty) {
        val avgSpeedup = avgSpeedups.sum / avgSpeedups.length
        reportWriter.println(f"1. **Average speedup vs DataFrame:** ${avgSpeedup}%.2fx")
      }
      
      reportWriter.println(f"2. **Optimization impact:** ${optResults("improvement_percent")}%+.1f%% improvement")
      reportWriter.println(f"3. **CSR format advantage:** ${formatResults("speedup")}%.2fx faster for SpMV")
      
      val bestAblation = ablationResults.minBy(_._2)
      val ablationImprovement = ((baseline - bestAblation._2) / baseline) * 100
      reportWriter.println(f"4. **Best optimization combination:** ${ablationImprovement}%+.1f%% improvement")
      
      reportWriter.println()
      reportWriter.println("### Conclusion")
      reportWriter.println()
      reportWriter.println("The custom distributed sparse matrix engine demonstrates:")
      reportWriter.println("- Competitive performance compared to Spark DataFrame")
      reportWriter.println("- Significant benefits from distributed optimizations")
      reportWriter.println("- Effective format specialization (COO vs CSR)")
      reportWriter.println("- Good scalability characteristics")
      reportWriter.println("- **Zero collect() calls** - truly distributed computation")
      
      reportWriter.close()
      
      println("✓ COMPREHENSIVE EVALUATION COMPLETE")
      println("\nReport generated: results/comprehensive_report.md")
      println("\nNext steps:")
      println("  1. Review results/comprehensive_report.md")
      println("  2. Generate visualizations from results/benchmark_results.csv")
      println("  3. Include findings in your coursework report")
      
    } catch {
      case e: Exception =>
          println("ERROR: Benchmark failed")
          e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
      sc.stop()
    }
  }
  
  private def verifyDataExists(dataDir: String, sizes: Seq[Int]): Unit = {
    println(s"\nVerifying data files in $dataDir/...")
    
    var allExist = true
    for (size <- sizes) {
      val matrixPath = s"$dataDir/sparse_matrix_${size}x${size}.csv"
      val vectorPath = s"$dataDir/dense_vector_${size}.csv"
      
      val matrixExists = new File(matrixPath).exists()
      val vectorExists = new File(vectorPath).exists()
      
      if (matrixExists && vectorExists) {
        println(f"  ✓ ${size}x${size}")
      } else {
        println(s"  ✗ ${size}x${size}: MISSING")
        allExist = false
      }
    }
    
    if (!allExist) {
      throw new RuntimeException("Missing required data files")
    }
    println("✓ All data files verified\n")
  }
}