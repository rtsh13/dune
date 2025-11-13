package benchmarks

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import engine.storage._
import engine.storage.CSCFormat._
import engine.optimization.OptimizationStrategies
import scala.collection.mutable.ArrayBuffer

/** Comprehensive verification system for all operations and formats
  */
object ComprehensiveVerification {

  val TOLERANCE = 1e-9

  case class VerificationResult(
    operation: String,
    format: String,
    optimization: String,
    passed: Boolean,
    maxError: Double,
    errorDetails: String
  )

  /** Verify all SpMV implementations
    */
  def verifyAllSpMV(
    sc: SparkContext,
    cooMatrix: SparseMatrixCOO,
    vector: Vector
  ): Seq[VerificationResult] = {

    println("\n=== Verifying All SpMV Implementations ===\n")
    val results = ArrayBuffer[VerificationResult]()
    val vectorRDD = vector.toIndexValueRDD

    // Compute reference result (baseline COO)
    println("Computing reference result...")
    val reference = OptimizationStrategies.cooSpMV_Baseline(cooMatrix.entries, vectorRDD)
    reference.cache()
    reference.count()

    // Verify COO implementations
    results += verifySpMVOpt("COO", "CoPartitioning", reference) {
      OptimizationStrategies.cooSpMV_CoPartitioning(cooMatrix.entries, vectorRDD)
    }

    results += verifySpMVOpt("COO", "InPartitionAgg", reference) {
      OptimizationStrategies.cooSpMV_InPartitionAgg(cooMatrix.entries, vectorRDD)
    }

    results += verifySpMVOpt("COO", "Balanced", reference) {
      OptimizationStrategies.cooSpMV_Balanced(cooMatrix.entries, vectorRDD)
    }

    results += verifySpMVOpt("COO", "Caching", reference) {
      OptimizationStrategies.cooSpMV_Caching(cooMatrix.entries, vectorRDD)
    }

    // Verify CSR implementations
    val csrMatrix = cooMatrix.toCSR

    results += verifySpMVOpt("CSR", "Baseline", reference) {
      OptimizationStrategies.csrSpMV_Baseline(csrMatrix.rows, vectorRDD)
    }

    results += verifySpMVOpt("CSR", "CoPartitioning", reference) {
      OptimizationStrategies.csrSpMV_CoPartitioning(csrMatrix.rows, vectorRDD)
    }

    results += verifySpMVOpt("CSR", "InPartitionAgg", reference) {
      OptimizationStrategies.csrSpMV_InPartitionAgg(csrMatrix.rows, vectorRDD)
    }

    results += verifySpMVOpt("CSR", "RowWiseOptimized", reference) {
      OptimizationStrategies.csrSpMV_RowWiseOptimized(csrMatrix.rows, vectorRDD)
    }

    // Verify CSC implementations
    val cscMatrix = CSCFormat.cooToDistributedCSC(
      cooMatrix.entries, cooMatrix.numRows, cooMatrix.numCols
    )

    results += verifySpMVOpt("CSC", "Baseline", reference) {
      OptimizationStrategies.cscSpMV_Baseline(cscMatrix, vectorRDD)
    }

    results += verifySpMVOpt("CSC", "CoPartitioning", reference) {
      OptimizationStrategies.cscSpMV_CoPartitioning(cscMatrix, vectorRDD)
    }

    results += verifySpMVOpt("CSC", "InPartitionAgg", reference) {
      OptimizationStrategies.cscSpMV_InPartitionAgg(cscMatrix, vectorRDD)
    }

    results += verifySpMVOpt("CSC", "ColumnWiseOptimized", reference) {
      OptimizationStrategies.cscSpMV_ColumnWiseOptimized(cscMatrix, vectorRDD)
    }

    reference.unpersist()

    printVerificationSummary("SpMV", results.toSeq)
    results.toSeq
  }

  /** Verify all SpMV-Sparse implementations
    */
  def verifyAllSpMVSparse(
    sc: SparkContext,
    cooMatrix: SparseMatrixCOO,
    sparseVector: SparseVector
  ): Seq[VerificationResult] = {

    println("\n=== Verifying All SpMV-Sparse Implementations ===\n")
    val results = ArrayBuffer[VerificationResult]()
    val sparseVectorRDD = sparseVector.entries

    // Reference result
    println("Computing reference result...")
    val reference = OptimizationStrategies.cooSpMVSparse_Baseline(cooMatrix.entries, sparseVectorRDD)
    reference.cache()
    reference.count()

    // COO implementations
    results += verifySpMVOpt("COO-Sparse", "CoPartitioning", reference) {
      OptimizationStrategies.cooSpMVSparse_CoPartitioning(cooMatrix.entries, sparseVectorRDD)
    }

    results += verifySpMVOpt("COO-Sparse", "InPartitionAgg", reference) {
      OptimizationStrategies.cooSpMVSparse_InPartitionAgg(cooMatrix.entries, sparseVectorRDD)
    }

    // CSR implementations
    val csrMatrix = cooMatrix.toCSR

    results += verifySpMVOpt("CSR-Sparse", "Baseline", reference) {
      OptimizationStrategies.csrSpMVSparse_Baseline(csrMatrix.rows, sparseVectorRDD)
    }

    results += verifySpMVOpt("CSR-Sparse", "CoPartitioning", reference) {
      OptimizationStrategies.csrSpMVSparse_CoPartitioning(csrMatrix.rows, sparseVectorRDD)
    }

    // CSC implementations
    val cscMatrix = CSCFormat.cooToDistributedCSC(
      cooMatrix.entries, cooMatrix.numRows, cooMatrix.numCols
    )

    results += verifySpMVOpt("CSC-Sparse", "Baseline", reference) {
      OptimizationStrategies.cscSpMVSparse_Baseline(cscMatrix, sparseVectorRDD)
    }

    results += verifySpMVOpt("CSC-Sparse", "CoPartitioning", reference) {
      OptimizationStrategies.cscSpMVSparse_CoPartitioning(cscMatrix, sparseVectorRDD)
    }

    reference.unpersist()

    printVerificationSummary("SpMV-Sparse", results.toSeq)
    results.toSeq
  }

  /** Verify all SpMM-Sparse implementations
    */
  def verifyAllSpMMSparse(
    sc: SparkContext,
    cooMatrix: SparseMatrixCOO
  ): Seq[VerificationResult] = {

    println("\n=== Verifying All SpMM-Sparse Implementations ===\n")
    val results = ArrayBuffer[VerificationResult]()

    // Reference result
    println("Computing reference result...")
    val reference = OptimizationStrategies.cooSpMM_Baseline(cooMatrix.entries, cooMatrix.entries)
    reference.cache()
    reference.count()

    // COO implementations
    results += verifySpMMOpt("COO-SpMM", "CoPartitioning", reference) {
      OptimizationStrategies.cooSpMM_CoPartitioning(cooMatrix.entries, cooMatrix.entries)
    }

    results += verifySpMMOpt("COO-SpMM", "InPartitionAgg", reference) {
      OptimizationStrategies.cooSpMM_InPartitionAgg(cooMatrix.entries, cooMatrix.entries)
    }

    results += verifySpMMOpt("COO-SpMM", "BlockPartitioned", reference) {
      OptimizationStrategies.cooSpMM_BlockPartitioned(cooMatrix.entries, cooMatrix.entries, 100)
    }

    // CSR implementations
    val csrMatrix = cooMatrix.toCSR

    results += verifySpMMOpt("CSR-SpMM", "Baseline", reference) {
      OptimizationStrategies.csrSpMM_Baseline(csrMatrix.rows, csrMatrix.rows)
    }

    results += verifySpMMOpt("CSR-SpMM", "CoPartitioning", reference) {
      OptimizationStrategies.csrSpMM_CoPartitioning(csrMatrix.rows, csrMatrix.rows)
    }

    results += verifySpMMOpt("CSR-SpMM", "InPartitionAgg", reference) {
      OptimizationStrategies.csrSpMM_InPartitionAgg(csrMatrix.rows, csrMatrix.rows)
    }

    // CSC implementations
    val cscMatrix = CSCFormat.cooToDistributedCSC(
      cooMatrix.entries, cooMatrix.numRows, cooMatrix.numCols
    )

    results += verifySpMMOpt("CSC-SpMM", "Baseline", reference) {
      OptimizationStrategies.cscSpMM_Baseline(cscMatrix, cscMatrix)
    }

    results += verifySpMMOpt("CSC-SpMM", "CoPartitioning", reference) {
      OptimizationStrategies.cscSpMM_CoPartitioning(cscMatrix, cscMatrix)
    }

    results += verifySpMMOpt("CSC-SpMM", "InPartitionAgg", reference) {
      OptimizationStrategies.cscSpMM_InPartitionAgg(cscMatrix, cscMatrix)
    }

    reference.unpersist()

    printVerificationSummary("SpMM-Sparse", results.toSeq)
    results.toSeq
  }

  /** Verify all SpMM-Dense implementations
    */
  def verifyAllSpMMDense(
    sc: SparkContext,
    cooMatrix: SparseMatrixCOO,
    denseMatrix: RDD[(Int, Int, Double)]
  ): Seq[VerificationResult] = {

    println("\n=== Verifying All SpMM-Dense Implementations ===\n")
    val results = ArrayBuffer[VerificationResult]()

    // Reference result
    println("Computing reference result...")
    val reference = OptimizationStrategies.cooSpMMDense_Baseline(cooMatrix.entries, denseMatrix)
    reference.cache()
    reference.count()

    // COO implementations
    results += verifySpMMDenseOpt("COO-SpMM-Dense", "CoPartitioning", reference) {
      OptimizationStrategies.cooSpMMDense_CoPartitioning(cooMatrix.entries, denseMatrix)
    }

    results += verifySpMMDenseOpt("COO-SpMM-Dense", "InPartitionAgg", reference) {
      OptimizationStrategies.cooSpMMDense_InPartitionAgg(cooMatrix.entries, denseMatrix)
    }

    results += verifySpMMDenseOpt("COO-SpMM-Dense", "BlockPartitioned", reference) {
      OptimizationStrategies.cooSpMMDense_BlockPartitioned(cooMatrix.entries, denseMatrix, 100)
    }

    results += verifySpMMDenseOpt("COO-SpMM-Dense", "Caching", reference) {
      OptimizationStrategies.cooSpMMDense_Caching(cooMatrix.entries, denseMatrix)
    }

    // CSR implementations
    val csrMatrix = cooMatrix.toCSR

    results += verifySpMMDenseOpt("CSR-SpMM-Dense", "Baseline", reference) {
      OptimizationStrategies.csrSpMMDense_Baseline(csrMatrix.rows, denseMatrix)
    }

    results += verifySpMMDenseOpt("CSR-SpMM-Dense", "CoPartitioning", reference) {
      OptimizationStrategies.csrSpMMDense_CoPartitioning(csrMatrix.rows, denseMatrix)
    }

    results += verifySpMMDenseOpt("CSR-SpMM-Dense", "InPartitionAgg", reference) {
      OptimizationStrategies.csrSpMMDense_InPartitionAgg(csrMatrix.rows, denseMatrix)
    }

    results += verifySpMMDenseOpt("CSR-SpMM-Dense", "RowWiseOptimized", reference) {
      OptimizationStrategies.csrSpMMDense_RowWiseOptimized(csrMatrix.rows, denseMatrix)
    }

    // CSC implementations
    val cscMatrix = CSCFormat.cooToDistributedCSC(
      cooMatrix.entries, cooMatrix.numRows, cooMatrix.numCols
    )

    results += verifySpMMDenseOpt("CSC-SpMM-Dense", "Baseline", reference) {
      OptimizationStrategies.cscSpMMDense_Baseline(cscMatrix, denseMatrix)
    }

    results += verifySpMMDenseOpt("CSC-SpMM-Dense", "CoPartitioning", reference) {
      OptimizationStrategies.cscSpMMDense_CoPartitioning(cscMatrix, denseMatrix)
    }

    results += verifySpMMDenseOpt("CSC-SpMM-Dense", "InPartitionAgg", reference) {
      OptimizationStrategies.cscSpMMDense_InPartitionAgg(cscMatrix, denseMatrix)
    }

    results += verifySpMMDenseOpt("CSC-SpMM-Dense", "ColumnWiseOptimized", reference) {
      OptimizationStrategies.cscSpMMDense_ColumnWiseOptimized(cscMatrix, denseMatrix)
    }

    reference.unpersist()

    printVerificationSummary("SpMM-Dense", results.toSeq)
    results.toSeq
  }

  /** Run complete verification suite
    */
  def runCompleteVerification(
    sc: SparkContext,
    dataDir: String = "synthetic-data",
    testSize: Int = 100
  ): Map[String, Seq[VerificationResult]] = {

    
    println("COMPREHENSIVE VERIFICATION SUITE")
    

    val matrixPath = s"$dataDir/sparse_matrix_${testSize}x${testSize}.csv"
    val vectorPath = s"$dataDir/dense_vector_${testSize}.csv"

    if (!new java.io.File(matrixPath).exists()) {
      println(s"ERROR: Test files not found for size $testSize")
      return Map.empty
    }

    println(s"\nLoading test data: ${testSize}x${testSize}")
    val cooMatrix = SmartLoader.loadMatrix(sc, matrixPath).toCOO
    val vector = SmartLoader.loadVector(sc, vectorPath)

    println(s"Matrix: ${cooMatrix.numNonZeros} non-zeros")

    // Run all verifications
    val spmvResults = verifyAllSpMV(sc, cooMatrix, vector)

    val sparseVectorOpt = try {
      val sparseVectorPath = s"$dataDir/sparse_vector_${testSize}.csv"
      if (new java.io.File(sparseVectorPath).exists()) {
        Some(SmartLoader.loadVector(sc, sparseVectorPath, forceSparse = true).asInstanceOf[SparseVector])
      } else None
    } catch {
      case _: Exception => None
    }

    val spmvSparseResults = sparseVectorOpt match {
      case Some(sv) => verifyAllSpMVSparse(sc, cooMatrix, sv)
      case None =>
        println("\nSkipping SpMV-Sparse verification (no sparse vector available)")
        Seq.empty
    }

    val spmmResults = verifyAllSpMMSparse(sc, cooMatrix)

    // Create small dense matrix for SpMM-Dense testing
    val denseMatrix = sc.parallelize(
      (0 until testSize).flatMap { row =>
        (0 until math.min(testSize, 20)).map { col =>
          (row, col, scala.util.Random.nextDouble())
        }
      }
    )

    val spmmDenseResults = verifyAllSpMMDense(sc, cooMatrix, denseMatrix)

    println("VERIFICATION SUMMARY")
    

    val allResults = spmvResults ++ spmvSparseResults ++ spmmResults ++ spmmDenseResults
    val passed = allResults.count(_.passed)
    val failed = allResults.count(!_.passed)

    println(f"\nTotal Tests: ${allResults.size}")
    println(f"Passed: $passed (${passed * 100.0 / allResults.size}%.1f%%)")
    println(f"Failed: $failed")

    if (failed > 0) {
      println("\nFailed Tests:")
      allResults.filter(!_.passed).foreach { r =>
        println(s"  - ${r.operation} ${r.format} ${r.optimization}")
        println(s"    ${r.errorDetails}")
      }
    } else {
      println("\nALL TESTS PASSED!")
    }

    Map(
      "SpMV" -> spmvResults,
      "SpMV-Sparse" -> spmvSparseResults,
      "SpMM-Sparse" -> spmmResults,
      "SpMM-Dense" -> spmmDenseResults
    )
  }

  // Helper functions

  private def verifySpMVOpt(
    format: String,
    optimization: String,
    reference: RDD[(Int, Double)]
  )(operation: => RDD[(Int, Double)]): VerificationResult = {

    print(s"  Verifying $format $optimization... ")

    val result = operation
    val comparison = reference.fullOuterJoin(result)

    val mismatches = comparison.filter { case (key, (refOpt, resOpt)) =>
      (refOpt, resOpt) match {
        case (Some(r), Some(res)) => math.abs(r - res) > TOLERANCE
        case (None, Some(_)) => true
        case (Some(_), None) => true
        case (None, None) => false
      }
    }

    val mismatchCount = mismatches.count()

    if (mismatchCount > 0) {
      val sample = mismatches.take(3)
      val errorMsg = s"$mismatchCount mismatches found. Sample: ${sample.mkString(", ")}"
      println(s"FAILED ($mismatchCount errors)")
      VerificationResult("SpMV", format, optimization, false, 1.0, errorMsg)
    } else {
      val errors = reference.join(result).map { case (key, (r, res)) =>
        math.abs(r - res)
      }
      val maxError = if (errors.isEmpty()) 0.0 else errors.max()
      println(f"PASSED (max error: $maxError%.2e)")
      VerificationResult("SpMV", format, optimization, true, maxError, "OK")
    }
  }

  private def verifySpMMOpt(
    format: String,
    optimization: String,
    reference: RDD[COOEntry]
  )(operation: => RDD[COOEntry]): VerificationResult = {

    print(s"  Verifying $format $optimization... ")

    val result = operation
    val refKV = reference.map(e => ((e.row, e.col), e.value))
    val resKV = result.map(e => ((e.row, e.col), e.value))

    val comparison = refKV.fullOuterJoin(resKV)

    val mismatches = comparison.filter { case (key, (refOpt, resOpt)) =>
      (refOpt, resOpt) match {
        case (Some(r), Some(res)) => math.abs(r - res) > TOLERANCE
        case (None, Some(_)) => true
        case (Some(_), None) => true
        case (None, None) => false
      }
    }

    val mismatchCount = mismatches.count()

    if (mismatchCount > 0) {
      println(s"FAILED ($mismatchCount errors)")
      VerificationResult("SpMM", format, optimization, false, 1.0, s"$mismatchCount mismatches")
    } else {
      val errors = refKV.join(resKV).map { case (key, (r, res)) =>
        math.abs(r - res)
      }
      val maxError = if (errors.isEmpty()) 0.0 else errors.max()
      println(f"PASSED (max error: $maxError%.2e)")
      VerificationResult("SpMM", format, optimization, true, maxError, "OK")
    }
  }

  private def verifySpMMDenseOpt(
    format: String,
    optimization: String,
    reference: RDD[(Int, Int, Double)]
  )(operation: => RDD[(Int, Int, Double)]): VerificationResult = {

    print(s"  Verifying $format $optimization... ")

    val result = operation
    val refKV = reference.map { case (i, j, v) => ((i, j), v) }
    val resKV = result.map { case (i, j, v) => ((i, j), v) }

    val comparison = refKV.fullOuterJoin(resKV)

    val mismatches = comparison.filter { case (key, (refOpt, resOpt)) =>
      (refOpt, resOpt) match {
        case (Some(r), Some(res)) => math.abs(r - res) > TOLERANCE
        case (None, Some(_)) => true
        case (Some(_), None) => true
        case (None, None) => false
      }
    }

    val mismatchCount = mismatches.count()

    if (mismatchCount > 0) {
      println(s"FAILED ($mismatchCount errors)")
      VerificationResult("SpMM-Dense", format, optimization, false, 1.0, s"$mismatchCount mismatches")
    } else {
      val errors = refKV.join(resKV).map { case (key, (r, res)) =>
        math.abs(r - res)
      }
      val maxError = if (errors.isEmpty()) 0.0 else errors.max()
      println(f"PASSED (max error: $maxError%.2e)")
      VerificationResult("SpMM-Dense", format, optimization, true, maxError, "OK")
    }
  }

  private def printVerificationSummary(
    operation: String,
    results: Seq[VerificationResult]
  ): Unit = {

    println(s"\n--- $operation Verification Summary ---")
    val passed = results.count(_.passed)
    val failed = results.count(!_.passed)
    println(f"Passed: $passed/${results.size} (${passed * 100.0 / results.size}%.1f%%)")
    if (failed > 0) {
      println(s"Failed: $failed")
      results.filter(!_.passed).foreach { r =>
        println(s"  - ${r.format} ${r.optimization}")
      }
    }
  }
}

object VerificationRunner {
  def main(args: Array[String]): Unit = {
    val conf = new org.apache.spark.SparkConf()
      .setAppName("Comprehensive Verification")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.memory", "4g")

    val sc = new org.apache.spark.SparkContext(conf)
    sc.setLogLevel("WARN")

    try {
      val results = ComprehensiveVerification.runCompleteVerification(
        sc,
        dataDir = "synthetic-data",
        testSize = 100
      )

      // Save verification report
      new java.io.File("results").mkdirs()
      val writer = new java.io.PrintWriter("results/verification_report.md")

      writer.println("# Comprehensive Verification Report")
      writer.println()

      results.foreach { case (operation, opResults) =>
        writer.println(s"## $operation")
        writer.println()
        writer.println("| Format | Optimization | Status | Max Error |")
        writer.println("|--------|--------------|--------|-----------|")

        opResults.foreach { r =>
          val status = if (r.passed) "PASS" else "FAIL"
          writer.println(f"| ${r.format} | ${r.optimization} | $status | ${r.maxError}%.2e |")
        }
        writer.println()
      }

      writer.close()

      println("\n\nVerification report saved to: results/verification_report.md")

    } catch {
      case e: Exception =>
        println(s"\nERROR: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      sc.stop()
    }
  }
}