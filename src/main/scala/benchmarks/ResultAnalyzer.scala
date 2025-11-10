package benchmarks

import java.io.PrintWriter

object ResultsAnalyzer {
  
  def generateReport(
    results: Seq[BenchmarkResult],
    outputFile: String
  ): Unit = {
    
    val writer = new PrintWriter(outputFile)
    
    writer.println("# Sparse Matrix Engine Performance Report\n")
    
    // Summary statistics - FIX HERE
    writer.println("## Summary")
    
    val speedups = results
      .groupBy(r => (r.operation, r.matrixSize, r.sparsity))
      .flatMap { case (key, group) =>
        for {
          custom <- group.find(_.implementation == "Custom")
          baseline <- group.find(_.implementation == "DataFrame")
        } yield baseline.executionTimeMs.toDouble / custom.executionTimeMs
      }
    
    if (speedups.nonEmpty) {
      val avgSpeedup = speedups.sum / speedups.size
      writer.println(f"Average Speedup (vs DataFrame): ${avgSpeedup}%.2fx\n")
    } else {
      writer.println("No DataFrame comparisons available\n")
    }
    
    // Detailed results table
    writer.println("## Detailed Results\n")
    writer.println("| Operation | Size | Sparsity | Implementation | Time (ms) | Throughput (ops/s) |")
    writer.println("|-----------|------|----------|----------------|-----------|-------------------|")
    
    results.sortBy(r => (r.operation, r.matrixSize, r.sparsity, r.implementation))
      .foreach { r =>
        writer.println(f"| ${r.operation} | ${r.matrixSize} | ${r.sparsity}%.3f | ${r.implementation} | ${r.executionTimeMs} | ${r.throughput}%.2e |")
      }
    
    // Add SpMV-specific summary if available
    val spmvResults = results.filter(_.operation == "SpMV")
    if (spmvResults.nonEmpty) {
      writer.println("\n## SpMV Performance Summary\n")
      
      spmvResults.groupBy(r => (r.matrixSize, r.sparsity))
        .toSeq.sortBy(_._1._1)
        .foreach { case ((size, sparsity), group) =>
          val custom = group.find(_.implementation == "Custom")
          val baseline = group.find(_.implementation == "DataFrame")
          
          if (custom.isDefined && baseline.isDefined) {
            val speedup = baseline.get.executionTimeMs.toDouble / custom.get.executionTimeMs
            writer.println(f"**${size}x${size}**: ${custom.get.executionTimeMs}ms (Custom) vs ${baseline.get.executionTimeMs}ms (DataFrame) = ${speedup}%.2fx speedup")
          }
        }
    }
    
    writer.close()
    println(s"✓ Report written to $outputFile")
  }
  
  def generateCSV(results: Seq[BenchmarkResult], outputFile: String): Unit = {
    val writer = new PrintWriter(outputFile)
    writer.println("operation,matrixSize,sparsity,implementation,timeMs,throughput")
    
    results.foreach { r =>
      writer.println(s"${r.operation},${r.matrixSize},${r.sparsity},${r.implementation},${r.executionTimeMs},${r.throughput}")
    }
    
    writer.close()
    println(s"✓ CSV written to $outputFile")
  }
}