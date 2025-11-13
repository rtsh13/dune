package benchmarks

import org.apache.spark.SparkContext
import engine.storage._
import engine.storage.CSCFormat._
import engine.operations.COOOperations
import engine.operations.CSROperations
import engine.operations.CSCOperations
import engine.operations.CSROperations
import scala.collection.mutable.ArrayBuffer

object EndToEndSuite {

  case class IterativeResult(
    implementation: String,
    totalTime: Double,
    perIteration: Double,
    speedup: Double
  )

  case class MatrixChainResult(
    format: String,
    timeMs: Double,
    speedupVsCOO: Double
  )

  case class ApplicationResult(
    name: String,
    description: String,
    customTime: Double,
    baselineTime: Double,
    speedup: Double
  )

  case class EndToEndResults(
    iterativeResults: Seq[IterativeResult],
    matrixChainResults: Seq[MatrixChainResult],
    applications: Seq[ApplicationResult],
    systemComparison: Map[String, (String, String, String)]
  )

def runEndToEndEvaluation(
    sc: SparkContext,
    datasets: Seq[DatasetDiscovery.DatasetInfo]
  ): EndToEndResults = {

    println("Running End-to-End System Evaluation...")
    println( * 80)

    // Select small/medium dataset for end-to-end
    val testDataset: Option[DatasetDiscovery.DatasetInfo] = datasets
      .filter(d => d.category == "Small" || d.category == "Medium")
      .sortBy(_.size)(Ordering.Int)  // Explicit ordering
      .headOption

    testDataset match {
      case None =>
        println("No suitable dataset found")
        EndToEndResults(Seq.empty, Seq.empty, Seq.empty, Map.empty)

      case Some(dataset: DatasetDiscovery.DatasetInfo) =>
        println(s"Using dataset: ${dataset.size}x${dataset.size}")
        println("-" * 80)

        val matrixPath = s"synthetic-data/${dataset.matrixFile}"
        val vectorPath = s"synthetic-data/${dataset.vectorFile}"

        val cooMatrix = SmartLoader.loadMatrix(sc, matrixPath).toCOO
        val vector = SmartLoader.loadVector(sc, vectorPath)

        // Test 1: Iterative Algorithm
        println("\n### Test 1: Iterative Algorithm (PageRank) ###")
        val iterativeResults = testIterativeAlgorithm(sc, cooMatrix, dataset.size)

        // Test 2: Matrix Chain
        println("\n### Test 2: Matrix Chain Multiplication ###")
        val chainResults = testMatrixChain(sc, cooMatrix)

        // Test 3: Real-world applications
        println("\n### Test 3: Real-World Applications ###")
        val applications = testRealWorldApplications(sc, cooMatrix, vector)

        // System comparison
        val systemComparison = generateSystemComparison(
          iterativeResults, chainResults, applications
        )

        EndToEndResults(
          iterativeResults,
          chainResults,
          applications,
          systemComparison
        )
    }
  }

  private def testIterativeAlgorithm(
    sc: SparkContext,
    matrix: SparseMatrixCOO,
    size: Int
  ): Seq[IterativeResult] = {

    val iterations = 10

    var vector = sc.parallelize((0 until size).map(i => (i, 1.0 / size)))

    println("\nCustom Implementation:")
    val customStart = System.nanoTime()
    for (i <- 1 to iterations) {
      val result = COOOperations.spMV(matrix.entries, vector)
      vector = result
      if (i % 2 == 0) {
        vector.count()
        println(f"Iteration $i complete")
      }
    }
    vector.count()
    val customTotal = (System.nanoTime() - customStart) / 1000000.0
    val customPer = customTotal / iterations

    println(f"Total time: ${customTotal}%.2f ms")
    println(f"Per iteration: ${customPer}%.2f ms")

    vector = sc.parallelize((0 until size).map(i => (i, 1.0 / size)))

    println("\nBaseline Implementation:")
    val baselineStart = System.nanoTime()
    for (i <- 1 to iterations) {
      val matrixByCol = matrix.entries.map(e => (e.col, (e.row, e.value)))
      val joined = matrixByCol.join(vector)
      val result = joined.map { case (col, ((row, mVal), vVal)) => (row, mVal * vVal) }
      vector = result.reduceByKey(_ + _)
      if (i % 2 == 0) {
        vector.count()
        println(f"Iteration $i complete")
      }
    }
    vector.count()
    val baselineTotal = (System.nanoTime() - baselineStart) / 1000000.0
    val baselinePer = baselineTotal / iterations

    println(f"Total time: ${baselineTotal}%.2f ms")
    println(f"Per iteration: ${baselinePer}%.2f ms")

    val speedup = baselineTotal / customTotal
    println(f"\nSpeedup: ${speedup}%.2fx")

    Seq(
      IterativeResult("Custom", customTotal, customPer, speedup),
      IterativeResult("Baseline", baselineTotal, baselinePer, 1.0)
    )
  }

private def testMatrixChain(
    sc: SparkContext,
    matrix: SparseMatrixCOO
  ): Seq[MatrixChainResult] = {

    println("Computing (A x B) x C using different formats")

    val results = ArrayBuffer[MatrixChainResult]()

    // Create a small sparse vector from matrix entries
    val sparseVectorEntries = sc.parallelize(
      matrix.entries.take(100).map(e => SparseVectorEntry(e.row, e.value))
    )

    println("\nCOO Format:")
    val cooStart = System.nanoTime()
    val AB_COO = COOOperations.spMMSparse(matrix.entries, matrix.entries)
    val ABC_COO = COOOperations.spMVSparse(AB_COO, sparseVectorEntries)
    ABC_COO.count()
    val cooTime = (System.nanoTime() - cooStart) / 1000000.0
    println(f"Time: ${cooTime}%.2f ms")
    results += MatrixChainResult("COO", cooTime, 1.0)

    println("\nCSR Format:")
    val csrMatrix = matrix.toCSR
    val csrStart = System.nanoTime()
    val AB_CSR = CSROperations.spMMSparse(csrMatrix.rows, csrMatrix.rows)
    val ABC_CSR = CSROperations.spMVSparse(csrMatrix.rows, sparseVectorEntries)
    ABC_CSR.count()
    val csrTime = (System.nanoTime() - csrStart) / 1000000.0
    println(f"Time: ${csrTime}%.2f ms")
    println(f" vs COO: ${cooTime / csrTime}%.2fx")
    results += MatrixChainResult("CSR", csrTime, cooTime / csrTime)

    println("\nCSC Format:")
    val cscMatrix = CSCFormat.cooToDistributedCSC(
      matrix.entries, matrix.numRows, matrix.numCols
    )
    val cscStart = System.nanoTime()
    val AB_CSC = CSCOperations.spMMSparse(cscMatrix, cscMatrix)
    val ABC_CSC = CSCOperations.spMVSparse(cscMatrix, sparseVectorEntries)
    ABC_CSC.count()
    val cscTime = (System.nanoTime() - cscStart) / 1000000.0
    println(f"    Time: ${cscTime}%.2f ms")
    println(f"    vs COO: ${cooTime / cscTime}%.2fx")
    results += MatrixChainResult("CSC", cscTime, cooTime / cscTime)

    results.toSeq
  }

  private def testRealWorldApplications(
    sc: SparkContext,
    matrix: SparseMatrixCOO,
    vector: Vector
  ): Seq[ApplicationResult] = {

    val results = ArrayBuffer[ApplicationResult]()

    println("\nApplication 1: Graph Analysis (Centrality)")
    val vectorRDD = vector.toIndexValueRDD

    val customStart1 = System.nanoTime()
    var result1 = COOOperations.spMV(matrix.entries, vectorRDD)
    for (i <- 1 to 5) {
      result1 = COOOperations.spMV(matrix.entries, result1)
    }
    result1.count()
    val customTime1 = (System.nanoTime() - customStart1) / 1000000.0

    val baselineStart1 = System.nanoTime()
    var result1b = vectorRDD
    for (i <- 1 to 5) {
      val matrixByCol = matrix.entries.map(e => (e.col, (e.row, e.value)))
      val joined = matrixByCol.join(result1b)
      result1b = joined.map { case (col, ((row, mVal), vVal)) => (row, mVal * vVal) }
        .reduceByKey(_ + _)
    }
    result1b.count()
    val baselineTime1 = (System.nanoTime() - baselineStart1) / 1000000.0

    println(f"Custom: ${customTime1}%.2f ms")
    println(f"Baseline: ${baselineTime1}%.2f ms")
    println(f"Speedup: ${baselineTime1 / customTime1}%.2fx")

    results += ApplicationResult(
      "Graph Centrality",
      "5 iterations of matrix-vector multiplication for centrality computation",
      customTime1,
      baselineTime1,
      baselineTime1 / customTime1
    )

    println("\nApplication 2: Collaborative Filtering")

    val customStart2 = System.nanoTime()
    val similarity = COOOperations.spMMSparse(matrix.entries, matrix.entries)
    val recommendations = COOOperations.spMV(similarity, vectorRDD)
    recommendations.count()
    val customTime2 = (System.nanoTime() - customStart2) / 1000000.0

    val baselineStart2 = System.nanoTime()
    val matrixByCol = matrix.entries.map(e => (e.col, (e.row, e.value)))
    val matrixByRow = matrix.entries.map(e => (e.row, (e.col, e.value)))
    val simJoined = matrixByCol.join(matrixByRow)
    val simPartial = simJoined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }
    val simResult = simPartial.reduceByKey(_ + _)
    val recJoined = simResult.map { case ((i, k), v) => (k, (i, v)) }.join(vectorRDD)
    val recResult = recJoined.map { case (k, ((i, simVal), vVal)) =>
      (i, simVal * vVal)
    }.reduceByKey(_ + _)
    recResult.count()
    val baselineTime2 = (System.nanoTime() - baselineStart2) / 1000000.0

    println(f"Custom: ${customTime2}%.2f ms")
    println(f"Baseline: ${baselineTime2}%.2f ms")
    println(f"Speedup: ${baselineTime2 / customTime2}%.2fx")

    results += ApplicationResult(
      "Collaborative Filtering",
      "Matrix multiplication for similarity followed by recommendation generation",
      customTime2,
      baselineTime2,
      baselineTime2 / customTime2
    )

    results.toSeq
  }

  private def generateSystemComparison(
    iterative: Seq[IterativeResult],
    chain: Seq[MatrixChainResult],
    apps: Seq[ApplicationResult]
  ): Map[String, (String, String, String)] = {

    val avgIterativeSpeedup = if (iterative.nonEmpty) {
      iterative.filter(_.implementation == "Custom").head.speedup
    } else 1.0

    val avgChainSpeedup = if (chain.nonEmpty) {
      chain.map(_.speedupVsCOO).max
    } else 1.0

    val avgAppSpeedup = if (apps.nonEmpty) {
      apps.map(_.speedup).sum / apps.size
    } else 1.0

    Map(
      "Iterative Performance" -> (
        f"${avgIterativeSpeedup}%.2fx faster",
        "1.0x (baseline)",
        f"${avgIterativeSpeedup}%.2fx"
      ),
      "Matrix Chain" -> (
        f"Best: ${avgChainSpeedup}%.2fx",
        "COO: 1.0x",
        f"${avgChainSpeedup}%.2fx"
      ),
      "Application Speedup" -> (
        f"${avgAppSpeedup}%.2fx average",
        "1.0x (baseline)",
        f"${avgAppSpeedup}%.2fx"
      ),
      "Zero collect() calls" -> (
        "Yes",
        "N/A (DataFrame uses actions)",
        "Fully distributed"
      )
    )
  }
}