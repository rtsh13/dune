package benchmarks

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import engine.storage._
import engine.storage.CSCFormat._
import engine.optimization.OptimizationStrategies
import scala.collection.mutable.ArrayBuffer

object AblationStudySuite {

  case class IndividualImpact(
    format: String,
    optimization: String,
    timeMs: Double,
    speedup: Double
  )

  case class CumulativeEffect(
    format: String,
    optimizationsAdded: String,
    timeMs: Double,
    cumulativeSpeedup: Double
  )

  case class AblationResults(
    individualImpact: Seq[IndividualImpact],
    cumulativeEffect: Seq[CumulativeEffect],
    formatAdvantage: Map[String, (Double, Double, Double)]
  )

def runAblationStudies(
    sc: SparkContext,
    datasets: Seq[DatasetDiscovery.DatasetInfo],
    iterations: Int = 3
  ): AblationResults = {

    println("Running Ablation Studies...")
    

    // Select medium-sized dataset for ablation
    val testDataset: Option[DatasetDiscovery.DatasetInfo] = datasets
      .filter(d => d.category == "Medium" || d.category == "Small")
      .sortBy(_.size)(Ordering.Int)  // Explicit ordering
      .reverse
      .headOption

    testDataset match {
      case None =>
        println("No suitable dataset found for ablation study")
        AblationResults(Seq.empty, Seq.empty, Map.empty)

      case Some(dataset: DatasetDiscovery.DatasetInfo) =>
        println(s"Using dataset: ${dataset.size}x${dataset.size}")
        println("-" * 80)

        val matrixPath = s"synthetic-data/${dataset.matrixFile}"
        val vectorPath = s"synthetic-data/${dataset.vectorFile}"
        
        val cooMatrix = SmartLoader.loadMatrix(sc, matrixPath).toCOO
        val vector = SmartLoader.loadVector(sc, vectorPath)

        println(s"Loaded: ${cooMatrix.numNonZeros} non-zeros")

        val csrMatrix = cooMatrix.toCSR
        val cscMatrix = CSCFormat.cooToDistributedCSC(
          cooMatrix.entries, cooMatrix.numRows, cooMatrix.numCols
        )

        // Test individual optimizations
        println("\n### Individual Optimization Impact ###")
        val individualResults = ArrayBuffer[IndividualImpact]()

        individualResults ++= testCOOIndividual(cooMatrix, vector, iterations)
        individualResults ++= testCSRIndividual(csrMatrix, vector, iterations)
        individualResults ++= testCSCIndividual(cscMatrix, vector, iterations)

        // Test cumulative effect
        println("\n### Cumulative Effect of Optimizations ###")
        val cumulativeResults = ArrayBuffer[CumulativeEffect]()

        cumulativeResults ++= testCOOCumulative(cooMatrix, vector, iterations)
        cumulativeResults ++= testCSRCumulative(csrMatrix, vector, iterations)
        cumulativeResults ++= testCSCCumulative(cscMatrix, vector, iterations)

        // Test format-specific advantages
        println("\n### Format-Specific Optimization Advantage ###")
        val formatAdvantage = testFormatAdvantage(
          cooMatrix, csrMatrix, cscMatrix, vector, iterations
        )

        AblationResults(
          individualResults.toSeq,
          cumulativeResults.toSeq,
          formatAdvantage
        )
    }
  }

  private def testCOOIndividual(
    cooMatrix: SparseMatrixCOO,
    vector: Vector,
    iterations: Int
  ): Seq[IndividualImpact] = {

    println("\n  COO Format - Individual Optimizations:")
    val results = ArrayBuffer[IndividualImpact]()
    val vectorRDD = vector.toIndexValueRDD

    val baseline = benchmarkAblation("Baseline", iterations) {
      OptimizationStrategies.cooSpMV_Baseline(cooMatrix.entries, vectorRDD)
    }
    results += IndividualImpact("COO", "Baseline", baseline, 1.0)

    val copart = benchmarkAblation("CoPartitioning", iterations) {
      OptimizationStrategies.cooSpMV_CoPartitioning(cooMatrix.entries, vectorRDD)
    }
    results += IndividualImpact("COO", "CoPartitioning", copart, baseline / copart)

    val inpart = benchmarkAblation("InPartitionAgg", iterations) {
      OptimizationStrategies.cooSpMV_InPartitionAgg(cooMatrix.entries, vectorRDD)
    }
    results += IndividualImpact("COO", "InPartitionAgg", inpart, baseline / inpart)

    val balanced = benchmarkAblation("Balanced", iterations) {
      OptimizationStrategies.cooSpMV_Balanced(cooMatrix.entries, vectorRDD)
    }
    results += IndividualImpact("COO", "Balanced", balanced, baseline / balanced)

    val caching = benchmarkAblation("Caching", iterations) {
      OptimizationStrategies.cooSpMV_Caching(cooMatrix.entries, vectorRDD)
    }
    results += IndividualImpact("COO", "Caching", caching, baseline / caching)

    results.toSeq
  }

  private def testCSRIndividual(
    csrMatrix: SparseMatrixCSR,
    vector: Vector,
    iterations: Int
  ): Seq[IndividualImpact] = {

    println("\n  CSR Format - Individual Optimizations:")
    val results = ArrayBuffer[IndividualImpact]()
    val vectorRDD = vector.toIndexValueRDD

    val baseline = benchmarkAblation("Baseline", iterations) {
      OptimizationStrategies.csrSpMV_Baseline(csrMatrix.rows, vectorRDD)
    }
    results += IndividualImpact("CSR", "Baseline", baseline, 1.0)

    val copart = benchmarkAblation("CoPartitioning", iterations) {
      OptimizationStrategies.csrSpMV_CoPartitioning(csrMatrix.rows, vectorRDD)
    }
    results += IndividualImpact("CSR", "CoPartitioning", copart, baseline / copart)

    val inpart = benchmarkAblation("InPartitionAgg", iterations) {
      OptimizationStrategies.csrSpMV_InPartitionAgg(csrMatrix.rows, vectorRDD)
    }
    results += IndividualImpact("CSR", "InPartitionAgg", inpart, baseline / inpart)

    val rowwise = benchmarkAblation("RowWiseOptimized", iterations) {
      OptimizationStrategies.csrSpMV_RowWiseOptimized(csrMatrix.rows, vectorRDD)
    }
    results += IndividualImpact("CSR", "RowWiseOptimized", rowwise, baseline / rowwise)

    results.toSeq
  }

  private def testCSCIndividual(
    cscMatrix: RDD[CSCColumn],
    vector: Vector,
    iterations: Int
  ): Seq[IndividualImpact] = {

    println("\n  CSC Format - Individual Optimizations:")
    val results = ArrayBuffer[IndividualImpact]()
    val vectorRDD = vector.toIndexValueRDD

    val baseline = benchmarkAblation("Baseline", iterations) {
      OptimizationStrategies.cscSpMV_Baseline(cscMatrix, vectorRDD)
    }
    results += IndividualImpact("CSC", "Baseline", baseline, 1.0)

    val copart = benchmarkAblation("CoPartitioning", iterations) {
      OptimizationStrategies.cscSpMV_CoPartitioning(cscMatrix, vectorRDD)
    }
    results += IndividualImpact("CSC", "CoPartitioning", copart, baseline / copart)

    val inpart = benchmarkAblation("InPartitionAgg", iterations) {
      OptimizationStrategies.cscSpMV_InPartitionAgg(cscMatrix, vectorRDD)
    }
    results += IndividualImpact("CSC", "InPartitionAgg", inpart, baseline / inpart)

    val colwise = benchmarkAblation("ColumnWiseOptimized", iterations) {
      OptimizationStrategies.cscSpMV_ColumnWiseOptimized(cscMatrix, vectorRDD)
    }
    results += IndividualImpact("CSC", "ColumnWiseOptimized", colwise, baseline / colwise)

    results.toSeq
  }

  private def testCOOCumulative(
    cooMatrix: SparseMatrixCOO,
    vector: Vector,
    iterations: Int
  ): Seq[CumulativeEffect] = {

    println("\n  COO Format - Cumulative Effect:")
    val results = ArrayBuffer[CumulativeEffect]()
    val vectorRDD = vector.toIndexValueRDD

    val baseline = benchmarkAblation("None", iterations) {
      OptimizationStrategies.cooSpMV_Baseline(cooMatrix.entries, vectorRDD)
    }
    results += CumulativeEffect("COO", "None", baseline, 1.0)

    val step1 = benchmarkAblation("CoPartitioning", iterations) {
      OptimizationStrategies.cooSpMV_CoPartitioning(cooMatrix.entries, vectorRDD)
    }
    results += CumulativeEffect("COO", "CoPartitioning", step1, baseline / step1)

    val step2 = benchmarkAblation("CoPartitioning + InPartitionAgg", iterations) {
      OptimizationStrategies.cooSpMV_InPartitionAgg(cooMatrix.entries, vectorRDD)
    }
    results += CumulativeEffect("COO", "CoPartitioning + InPartitionAgg", step2, baseline / step2)

    results.toSeq
  }

  private def testCSRCumulative(
    csrMatrix: SparseMatrixCSR,
    vector: Vector,
    iterations: Int
  ): Seq[CumulativeEffect] = {

    println("\n  CSR Format - Cumulative Effect:")
    val results = ArrayBuffer[CumulativeEffect]()
    val vectorRDD = vector.toIndexValueRDD

    val baseline = benchmarkAblation("None", iterations) {
      OptimizationStrategies.csrSpMV_Baseline(csrMatrix.rows, vectorRDD)
    }
    results += CumulativeEffect("CSR", "None", baseline, 1.0)

    val step1 = benchmarkAblation("CoPartitioning", iterations) {
      OptimizationStrategies.csrSpMV_CoPartitioning(csrMatrix.rows, vectorRDD)
    }
    results += CumulativeEffect("CSR", "CoPartitioning", step1, baseline / step1)

    val step2 = benchmarkAblation("CoPartitioning + InPartitionAgg", iterations) {
      OptimizationStrategies.csrSpMV_InPartitionAgg(csrMatrix.rows, vectorRDD)
    }
    results += CumulativeEffect("CSR", "CoPartitioning + InPartitionAgg", step2, baseline / step2)

    val step3 = benchmarkAblation("All + RowWise", iterations) {
      OptimizationStrategies.csrSpMV_RowWiseOptimized(csrMatrix.rows, vectorRDD)
    }
    results += CumulativeEffect("CSR", "All + RowWise", step3, baseline / step3)

    results.toSeq
  }

  private def testCSCCumulative(
    cscMatrix: RDD[CSCColumn],
    vector: Vector,
    iterations: Int
  ): Seq[CumulativeEffect] = {

    println("\n  CSC Format - Cumulative Effect:")
    val results = ArrayBuffer[CumulativeEffect]()
    val vectorRDD = vector.toIndexValueRDD

    val baseline = benchmarkAblation("None", iterations) {
      OptimizationStrategies.cscSpMV_Baseline(cscMatrix, vectorRDD)
    }
    results += CumulativeEffect("CSC", "None", baseline, 1.0)

    val step1 = benchmarkAblation("CoPartitioning", iterations) {
      OptimizationStrategies.cscSpMV_CoPartitioning(cscMatrix, vectorRDD)
    }
    results += CumulativeEffect("CSC", "CoPartitioning", step1, baseline / step1)

    val step2 = benchmarkAblation("CoPartitioning + InPartitionAgg", iterations) {
      OptimizationStrategies.cscSpMV_InPartitionAgg(cscMatrix, vectorRDD)
    }
    results += CumulativeEffect("CSC", "CoPartitioning + InPartitionAgg", step2, baseline / step2)

    val step3 = benchmarkAblation("All + ColumnWise", iterations) {
      OptimizationStrategies.cscSpMV_ColumnWiseOptimized(cscMatrix, vectorRDD)
    }
    results += CumulativeEffect("CSC", "All + ColumnWise", step3, baseline / step3)

    results.toSeq
  }

  private def testFormatAdvantage(
    cooMatrix: SparseMatrixCOO,
    csrMatrix: SparseMatrixCSR,
    cscMatrix: RDD[CSCColumn],
    vector: Vector,
    iterations: Int
  ): Map[String, (Double, Double, Double)] = {

    println("\n  Format-Specific Advantage:")
    val vectorRDD = vector.toIndexValueRDD

    // CSR advantage: Row-wise vs generic
    val csrGeneric = benchmarkAblation("CSR Generic", iterations) {
      OptimizationStrategies.csrSpMV_InPartitionAgg(csrMatrix.rows, vectorRDD)
    }
    val csrSpecific = benchmarkAblation("CSR Specific", iterations) {
      OptimizationStrategies.csrSpMV_RowWiseOptimized(csrMatrix.rows, vectorRDD)
    }

    // CSC advantage: Column-wise vs generic
    val cscGeneric = benchmarkAblation("CSC Generic", iterations) {
      OptimizationStrategies.cscSpMV_InPartitionAgg(cscMatrix, vectorRDD)
    }
    val cscSpecific = benchmarkAblation("CSC Specific", iterations) {
      OptimizationStrategies.cscSpMV_ColumnWiseOptimized(cscMatrix, vectorRDD)
    }

    Map(
      "CSR" -> (csrSpecific, csrGeneric, csrGeneric / csrSpecific),
      "CSC" -> (cscSpecific, cscGeneric, cscGeneric / cscSpecific)
    )
  }

  private def benchmarkAblation(
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
    println(f"    $name%-30s: ${avg}%.2f ms")
    avg
  }
}