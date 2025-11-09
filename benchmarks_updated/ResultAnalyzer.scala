// src/main/scala/benchmarks/ResultsAnalyzer.scala
package benchmarks

import java.io.PrintWriter

object ResultsAnalyzer {
  
  def generateReport(
    results: Seq[BenchmarkResult],
    outputFile: String
  ): Unit = {
    
    val writer = new PrintWriter(outputFile)
    
    writer.println("# Sparse Matrix Engine Performance Report\n")
    
    // Summary statistics
    writer.println("## Summary")
    val avgSpeedup = results
      .groupBy(r => (r.operation, r.matrixSize, r.sparsity))
      .map { case (key, group) =>
        val custom = group.find(_.implementation == "Custom").get
        val baseline = group.find(_.implementation == "DataFrame").get
        baseline.executionTimeMs.toDouble / custom.executionTimeMs
      }
      .sum / results.size
    
    writer.println(f"Average Speedup: ${avgSpeedup}%.2fx\n")
    
    // Detailed results table
    writer.println("## Detailed Results\n")
    writer.println("| Operation | Size | Sparsity | Implementation | Time (ms) | Throughput (ops/s) |")
    writer.println("|-----------|------|----------|----------------|-----------|-------------------|")
    
    results.sortBy(r => (r.operation, r.matrixSize, r.sparsity, r.implementation))
      .foreach { r =>
        writer.println(f"| ${r.operation} | ${r.matrixSize} | ${r.sparsity}%.3f | ${r.implementation} | ${r.executionTimeMs} | ${r.throughput}%.2e |")
      }
    
    writer.close()
    println(s"Report written to $outputFile")
  }
  
  def generateCSV(results: Seq[BenchmarkResult], outputFile: String): Unit = {
    val writer = new PrintWriter(outputFile)
    writer.println("operation,matrixSize,sparsity,implementation,timeMs,throughput")
    
    results.foreach { r =>
      writer.println(s"${r.operation},${r.matrixSize},${r.sparsity},${r.implementation},${r.executionTimeMs},${r.throughput}")
    }
    
    writer.close()
  }
}