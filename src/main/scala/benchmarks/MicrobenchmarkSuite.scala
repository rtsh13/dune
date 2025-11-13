package benchmarks

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import engine.storage._
import engine.storage.CSCFormat._
import engine.operations.CSCOperations
import engine.operations.CSROperations
import engine.operations.COOOperations
import engine.optimization.OptimizationStrategies
import scala.collection.mutable.ArrayBuffer

object MicrobenchmarkSuite {

  case class FormatResult(format: String, operation: String, size: Int, timeMs: Double, verified: Boolean)
  case class DataFrameResult(size: Int, implementation: String, timeMs: Double, speedup: Double)
  case class OperationResult(operation: String, avgTimeMs: Double, relativeSpeed: Double)

  case class MicroResults(
    formatComparison: Seq[FormatResult],
    dataframeComparison: Seq[DataFrameResult],
    operationComparison: Seq[OperationResult],
    scalingAnalysis: Seq[(Int, Double)],
    bestFormat: String,
    bestSpeedup: Double,
    worstCase: Double
  )

  def runMicrobenchmarks(
    sc: SparkContext,
    spark: SparkSession,
    datasets: Seq[DatasetDiscovery.DatasetInfo],
    iterations: Int = 5
  ): MicroResults = {

    println("Running Microbenchmark Suite...")
    println( * 80)

    val formatResults = ArrayBuffer[FormatResult]()
    val dataframeResults = ArrayBuffer[DataFrameResult]()
    val operationResults = ArrayBuffer[OperationResult]()
    val scalingResults = ArrayBuffer[(Int, Double)]()

    // Test each dataset
    for (dataset <- datasets.sortBy(_.size)(Ordering.Int)) {
      println(s"\n\nTesting: ${dataset.size}x${dataset.size} (${dataset.category})")
      println("-" * 80)

      val matrixPath = s"synthetic-data/${dataset.matrixFile}"
      val vectorPath = s"synthetic-data/${dataset.vectorFile}"

      // Load data
      val cooMatrix = SmartLoader.loadMatrix(sc, matrixPath).toCOO
      val vector = SmartLoader.loadVector(sc, vectorPath)

      println(s"Loaded: ${cooMatrix.numNonZeros} non-zeros")

      // Convert formats (measure conversion time separately)
      println("Converting to CSR...")
      val csrStart = System.nanoTime()
      val csrMatrix = cooMatrix.toCSR
      val csrConversionTime = (System.nanoTime() - csrStart) / 1000000.0
      println(f"CSR conversion: ${csrConversionTime}%.2f ms")

      println("Converting to CSC...")
      val cscStart = System.nanoTime()
      val cscMatrix = CSCFormat.cooToDistributedCSC(
        cooMatrix.entries, cooMatrix.numRows, cooMatrix.numCols
      )
      val cscConversionTime = (System.nanoTime() - cscStart) / 1000000.0
      println(f"CSC conversion: ${cscConversionTime}%.2f ms")

      // Test SpMV for all formats
      println("\n### Testing SpMV ###")
      
      val cooTime = benchmarkOperation("COO SpMV", iterations) {
        COOOperations.spMV(cooMatrix.entries, vector.toIndexValueRDD)
      }
      formatResults += FormatResult("COO", "SpMV", dataset.size, cooTime, true)
      scalingResults += ((dataset.size, cooTime))

      val csrTime = benchmarkOperation("CSR SpMV", iterations) {
        CSROperations.spMV(csrMatrix.rows, vector.toIndexValueRDD)
      }
      formatResults += FormatResult("CSR", "SpMV", dataset.size, csrTime, true)

      val cscTime = benchmarkOperation("CSC SpMV", iterations) {
        CSCOperations.spMV(cscMatrix, vector.toIndexValueRDD)
      }
      formatResults += FormatResult("CSC", "SpMV", dataset.size, cscTime, true)

      val bestCustomTime = Seq(cooTime, csrTime, cscTime).min
      val bestFormat = Seq(("COO", cooTime), ("CSR", csrTime), ("CSC", cscTime)).minBy(_._2)._1

      // Compare against DataFrame (for small/medium sizes)
      //if (dataset.category == "Small" || dataset.category == "Medium") {
        println("\n### DataFrame Comparison ###")
        println(s"Best custom format: $bestFormat")

        
        val dfTime = benchmarkDataFrame(spark, cooMatrix, vector, iterations)
        
        dataframeResults += DataFrameResult(dataset.size, "Custom", bestCustomTime, dfTime / bestCustomTime)
        dataframeResults += DataFrameResult(dataset.size, "DataFrame", dfTime, 1.0)
        
        println(f"Custom (Best=$bestFormat): ${bestCustomTime}%.2f ms")
        println(f"DataFrame: ${dfTime}%.2f ms")
        println(f"Speedup: ${dfTime / bestCustomTime}%.2fx")
      //}

      // Test other operations for small/medium matrices
      if (dataset.size <= 1000) {
        // Test SpMV-Sparse if available
        try {
          val sparseVectorPath = s"synthetic-data/sparse_vector_${dataset.size}.csv"
          if (new java.io.File(sparseVectorPath).exists()) {
            println("\n### Testing SpMV-Sparse ###")
            val sparseVector = SmartLoader.loadVector(sc, sparseVectorPath, forceSparse = true)
            val sparseVectorRDD = sparseVector match {
              case sv: SparseVector => sv.entries
              case _ => null
            }
            
            if (sparseVectorRDD != null) {
              val cooSparseTime = benchmarkOperation("COO SpMV-Sparse", iterations) {
                COOOperations.spMVSparse(cooMatrix.entries, sparseVectorRDD)
              }
              formatResults += FormatResult("COO", "SpMV-Sparse", dataset.size, cooSparseTime, true)
            }
          }
        } catch {
          case e: Exception => println(s"Skipping SpMV-Sparse: ${e.getMessage}")
        }

        // Test SpMM
        if (dataset.size <= 500) {
          println("\n### Testing SpMM ###")
          val cooSpmmTime = benchmarkOperation("COO SpMM", iterations) {
            COOOperations.spMMSparse(cooMatrix.entries, cooMatrix.entries)
          }
          formatResults += FormatResult("COO", "SpMM", dataset.size, cooSpmmTime, true)
        }
      }
    }

    // NEW: Test SpMM-Dense
    println("\n\n" +  * 80)
    println("TESTING: Sparse Matrix × Dense Matrix (SpMM-Dense)")
    println( * 80)

    testSpMMDense(sc, formatResults, iterations)

    // NEW: Test MTTKRP
    println("\n\n" +  * 80)
    println("TESTING: MTTKRP (Tensor Operations)")
    println( * 80)

    testMTTKRP(sc, formatResults, iterations)

    // Compute operation averages
    val opTypes = formatResults.map(_.operation).distinct
    opTypes.foreach { opType =>
      val opResults = formatResults.filter(_.operation == opType)
      val avgTime = opResults.map(_.timeMs).sum / opResults.size
      val fastest = formatResults.filter(_.operation == opType).map(_.timeMs).min
      operationResults += OperationResult(opType, avgTime, fastest / avgTime)
    }

    // Determine best format
    val formatAvgs = Seq("COO", "CSR", "CSC").map { format =>
      val times = formatResults.filter(_.format == format).map(_.timeMs)
      val avg = if (times.nonEmpty) times.sum / times.size else Double.MaxValue
      (format, avg)
    }
    val bestFormat = formatAvgs.minBy(_._2)._1

    // Compute speedup stats
    val speedups = dataframeResults.map(_.speedup)
    val bestSpeedup = if (speedups.nonEmpty) speedups.max else 1.0
    val worstCase = if (speedups.nonEmpty) speedups.min else 1.0

    MicroResults(
      formatResults.toSeq,
      dataframeResults.toSeq,
      operationResults.toSeq,
      scalingResults.toSeq,
      bestFormat,
      bestSpeedup,
      worstCase
    )
  }

  private def testSpMMDense(
    sc: SparkContext,
    formatResults: ArrayBuffer[FormatResult],
    iterations: Int
  ): Unit = {

    val testSizes = Seq(
      (100, 10, "small"),
      (1000, 20, "medium")
    )

    for ((matrixSize, denseSize, label) <- testSizes) {
      val matrixPath = s"synthetic-data/sparse_matrix_${matrixSize}x${matrixSize}.csv"
      val densePath = s"synthetic-data/dense_matrix_${matrixSize}x${denseSize}.csv"

      if (new java.io.File(matrixPath).exists() && new java.io.File(densePath).exists()) {
        println(s"\n\nTesting SpMM-Dense: ${matrixSize}x${matrixSize} × ${matrixSize}x${denseSize} ($label)")
        println("-" * 80)

        val cooMatrix = SmartLoader.loadMatrix(sc, matrixPath).toCOO
        val denseMatrix = loadDenseMatrix(sc, densePath)

        println(s"Sparse matrix: ${cooMatrix.numNonZeros} non-zeros")
        println(s"Dense matrix: ${matrixSize * denseSize} entries")

        // COO SpMM-Dense
        println("\n  Testing COO SpMM-Dense")
        val cooTime = benchmarkOperation("COO SpMM-Dense", iterations) {
          OptimizationStrategies.cooSpMMDense_Baseline(cooMatrix.entries, denseMatrix)
        }
        formatResults += FormatResult("COO", "SpMM-Dense", matrixSize, cooTime, true)

        // CSR SpMM-Dense
        println("\n  Testing CSR SpMM-Dense")
        val csrMatrix = cooMatrix.toCSR
        val csrTime = benchmarkOperation("CSR SpMM-Dense", iterations) {
          OptimizationStrategies.csrSpMMDense_Baseline(csrMatrix.rows, denseMatrix)
        }
        formatResults += FormatResult("CSR", "SpMM-Dense", matrixSize, csrTime, true)

        // CSC SpMM-Dense
        println("\n  Testing CSC SpMM-Dense")
        val cscMatrix = CSCFormat.cooToDistributedCSC(
          cooMatrix.entries, cooMatrix.numRows, cooMatrix.numCols
        )
        val cscTime = benchmarkOperation("CSC SpMM-Dense", iterations) {
          OptimizationStrategies.cscSpMMDense_Baseline(cscMatrix, denseMatrix)
        }
        formatResults += FormatResult("CSC", "SpMM-Dense", matrixSize, cscTime, true)

        println(f"\n  Summary:")
        println(f"    COO: ${cooTime}%.2f ms")
        println(f"    CSR: ${csrTime}%.2f ms (${cooTime/csrTime}%.2fx vs COO)")
        println(f"    CSC: ${cscTime}%.2f ms (${cooTime/cscTime}%.2fx vs COO)")
      } else {
        println(s"\nSkipping SpMM-Dense $label: data files not found")
      }
    }
  }

  private def testMTTKRP(
    sc: SparkContext,
    formatResults: ArrayBuffer[FormatResult],
    iterations: Int
  ): Unit = {

    val tensorSizes = Seq(
      (10, 10, 10, "tiny"),
      (50, 50, 50, "medium")
    )

    for ((d1, d2, d3, label) <- tensorSizes) {
      val tensorPath = s"synthetic-data/sparse_tensor_${d1}x${d2}x${d3}.csv"

      if (new java.io.File(tensorPath).exists()) {
        println(s"\n\nTesting MTTKRP: ${d1}x${d2}x${d3} tensor ($label)")
        println("-" * 80)

        val tensorEntries = loadTensor(sc, tensorPath)
        val factorMatrices = createFactorMatrices(sc, d1, d2, d3, rank = 10)

        val nnz = tensorEntries.count()
        println(s"Tensor: $nnz non-zeros")

        // COO MTTKRP
        println("\n  Testing COO MTTKRP")
        val cooTime = benchmarkOperation("COO MTTKRP", iterations) {
          OptimizationStrategies.cooMTTKRP_Baseline(tensorEntries, factorMatrices, mode = 0)
        }
        formatResults += FormatResult("COO", "MTTKRP", d1, cooTime, true)

        // CSR MTTKRP
        println("\n  Testing CSR MTTKRP")
        val csrRows = convertTensorToCSR(tensorEntries)
        val csrTime = benchmarkOperation("CSR MTTKRP", iterations) {
          OptimizationStrategies.csrMTTKRP_Baseline(csrRows, factorMatrices, mode = 0)
        }
        formatResults += FormatResult("CSR", "MTTKRP", d1, csrTime, true)

        // CSC MTTKRP
        println("\n  Testing CSC MTTKRP")
        val cscColumns = convertTensorToCSC(tensorEntries)
        val cscTime = benchmarkOperation("CSC MTTKRP", iterations) {
          OptimizationStrategies.cscMTTKRP_Baseline(cscColumns, factorMatrices, mode = 0)
        }
        formatResults += FormatResult("CSC", "MTTKRP", d1, cscTime, true)

        println(f"\n  Summary:")
        println(f"    COO: ${cooTime}%.2f ms")
        println(f"    CSR: ${csrTime}%.2f ms (${cooTime/csrTime}%.2fx vs COO)")
        println(f"    CSC: ${cscTime}%.2f ms (${cooTime/cscTime}%.2fx vs COO)")
      } else {
        println(s"\nSkipping MTTKRP $label: tensor data not found")
        println(s"  Expected: $tensorPath")
        println(s"  Run: python3 generate_tensor_data.py")
      }
    }
  }

  // Helper functions

  private def loadDenseMatrix(
    sc: SparkContext,
    path: String
  ): RDD[(Int, Int, Double)] = {
    sc.textFile(path)
      .filter(!_.startsWith("row"))
      .map { line =>
        val parts = line.split(",")
        (parts(0).toInt, parts(1).toInt, parts(2).toDouble)
      }
  }

  private def loadTensor(
    sc: SparkContext,
    path: String
  ): RDD[engine.tensor.TensorEntry] = {
    sc.textFile(path)
      .filter(!_.startsWith("i"))
      .map { line =>
        val parts = line.split(",")
        engine.tensor.TensorEntry(
          Array(parts(0).toInt, parts(1).toInt, parts(2).toInt),
          parts(3).toDouble
        )
      }
  }

  private def createFactorMatrices(
    sc: SparkContext,
    d1: Int,
    d2: Int,
    d3: Int,
    rank: Int
  ): Array[RDD[(Int, Array[Double])]] = {
    
    val random = new scala.util.Random(42)
    
    Array(
      sc.parallelize((0 until d1).map { i =>
        (i, Array.fill(rank)(random.nextDouble()))
      }),
      sc.parallelize((0 until d2).map { i =>
        (i, Array.fill(rank)(random.nextDouble()))
      }),
      sc.parallelize((0 until d3).map { i =>
        (i, Array.fill(rank)(random.nextDouble()))
      })
    )
  }

  private def convertTensorToCSR(
    tensorEntries: RDD[engine.tensor.TensorEntry]
  ): RDD[FormatConverter.CSRRow] = {
    // Treat first two dimensions as matrix (flatten third dimension)
    tensorEntries
      .map(e => (e.indices(0), (e.indices(1), e.value)))
      .groupByKey()
      .map { case (row, entries) =>
        val sorted = entries.toArray.sortBy(_._1)
        val colIndices = sorted.map(_._1)
        val values = sorted.map(_._2)
        
        // Determine numCols from the entries
        val numCols = if (colIndices.nonEmpty) colIndices.max + 1 else 0
        
        FormatConverter.CSRRow(
          row,
          values,
          colIndices,
          numCols
        )
      }
  }

  private def convertTensorToCSC(
    tensorEntries: RDD[engine.tensor.TensorEntry]
  ): RDD[CSCColumn] = {
    // Treat first two dimensions as matrix (flatten third dimension)
    tensorEntries
      .map(e => (e.indices(1), (e.indices(0), e.value)))
      .groupByKey()
      .map { case (col, entries) =>
        val sorted = entries.toArray.sortBy(_._1)
        val rowIndices = sorted.map(_._1)
        val values = sorted.map(_._2)
        
        // Determine numRows from the entries
        val numRows = if (rowIndices.nonEmpty) rowIndices.max + 1 else 0
        
        CSCFormat.CSCColumn(
          col,
          values,
          rowIndices,
          numRows
        )
      }
  }

  private def benchmarkOperation(
    name: String,
    iterations: Int
  )(operation: => RDD[_]): Double = {

    println(s"\n  Benchmarking: $name")
    val times = ArrayBuffer[Double]()

    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(500)

      val start = System.nanoTime()
      operation.count()
      val elapsed = (System.nanoTime() - start) / 1000000.0

      times += elapsed
      println(f"    Iteration $i: ${elapsed}%.2f ms")
    }

    val avg = times.sum / iterations
    println(f"    Average: ${avg}%.2f ms")
    avg
  }

  private def benchmarkDataFrame(
    spark: SparkSession,
    matrix: SparseMatrixCOO,
    vector: Vector,
    iterations: Int
  ): Double = {

    import spark.implicits._

    val matrixDF = matrix.entries
      .map(e => (e.row, e.col, e.value))
      .toDF("row", "col", "matValue")
      .cache()

    val vectorDF = vector.toIndexValueRDD
      .toDF("col", "vecValue")
      .cache()

    matrixDF.count()
    vectorDF.count()

    val times = ArrayBuffer[Double]()

    for (i <- 1 to iterations) {
      System.gc()
      Thread.sleep(500)

      val start = System.nanoTime()
      matrixDF
        .join(vectorDF, "col")
        .selectExpr("row", "matValue * vecValue as product")
        .groupBy("row")
        .sum("product")
        .count()
      val elapsed = (System.nanoTime() - start) / 1000000.0

      times += elapsed
    }

    matrixDF.unpersist()
    vectorDF.unpersist()

    times.sum / iterations
  }
}