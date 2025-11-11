package realworldbenchmarks

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, IndexedRowMatrix, IndexedRow, BlockMatrix}
import org.apache.spark.mllib.linalg.{Vectors, Vector => MLVector, DenseVector, DenseMatrix}
import org.apache.spark.rdd.RDD
import engine.storage._
import engine.operations.MultiplicationOps

object ActualMLlibBenchmark {

  case class BenchmarkResult(
      operation: String,
      matrixSize: String,
      customTime: Double,
      mllibTime: Double,
      speedup: Double
  )

  /** Benchmark 1: SpMV using MLlib's IndexedRowMatrix
    * Note: MLlib doesn't have direct vector multiplication, so we convert vector to column matrix
    */
  def benchmarkMLlibSpMV(
      sc: SparkContext,
      matrixSize: Int,
      dataDir: String
  ): BenchmarkResult = {

    println("\n" + "=" * 80)
    println("BENCHMARK: SpMV - Custom Engine vs MLlib")
    println("=" * 80)

    // Load data
    val customMatrix = COOLoader.loadSparseMatrix(
      sc,
      s"$dataDir/sparse_matrix_${matrixSize}x${matrixSize}.csv"
    )
    val customVector = COOLoader.loadDenseVectorRDD(
      sc,
      s"$dataDir/dense_vector_${matrixSize}.csv"
    )

    val nnz = customMatrix.count()
    println(s"Matrix: ${matrixSize}x${matrixSize}, NNZ: $nnz")

    // === CUSTOM ENGINE ===
    println("\n--- Custom Sparse Matrix Engine ---")
    val customStart = System.nanoTime()

    val customResult = MultiplicationOps.sparseMatrixDenseVector(
      customMatrix,
      customVector
    )
    val customCount = customResult.count()

    val customTime = (System.nanoTime() - customStart) / 1000000.0

    println(f"Custom Engine: ${customTime}%.2f ms")
    println(s"Result entries: $customCount")

    // === MLLIB - using IndexedRowMatrix with manual multiplication ===
    println("\n--- MLlib (IndexedRowMatrix with manual dot products) ---")

    // Convert to MLlib IndexedRowMatrix
    val indexedRows = customMatrix
      .groupBy(_.row)
      .map { case (rowId, entries) =>
        val sorted = entries.toArray.sortBy(_.col)
        val indices = sorted.map(_.col)
        val values = sorted.map(_.value)
        IndexedRow(rowId.toLong, Vectors.sparse(matrixSize, indices, values))
      }
    
    val indexedMatrix = new IndexedRowMatrix(indexedRows)

    // Convert vector to array for broadcasting
    val vectorArray = customVector.collect().sortBy(_._1).map(_._2)
    val vectorBC = sc.broadcast(vectorArray)

    val mllibStart = System.nanoTime()

    // Manual SpMV: for each row, compute dot product with vector
    val mllibResult = indexedMatrix.rows.map { indexedRow =>
      val rowVector = indexedRow.vector
      var dotProduct = 0.0
      
      // Compute dot product based on vector type
      rowVector match {
        case sv: org.apache.spark.mllib.linalg.SparseVector =>
          var i = 0
          while (i < sv.indices.length) {
            dotProduct += sv.values(i) * vectorBC.value(sv.indices(i))
            i += 1
          }
        case dv: org.apache.spark.mllib.linalg.DenseVector =>
          var i = 0
          while (i < dv.size) {
            dotProduct += dv.values(i) * vectorBC.value(i)
            i += 1
          }
      }
      
      (indexedRow.index.toInt, dotProduct)
    }
    
    val mllibCount = mllibResult.count()

    val mllibTime = (System.nanoTime() - mllibStart) / 1000000.0

    println(f"MLlib: ${mllibTime}%.2f ms")
    println(s"Result entries: $mllibCount")

    vectorBC.unpersist()

    // === COMPARISON ===
    val speedup = mllibTime / customTime
    println("\n" + "-" * 80)
    println(f"Speedup: ${speedup}%.2fx")
    if (speedup > 1.0) {
      println("✓ Custom engine is FASTER")
    } else {
      println("✗ MLlib is faster")
    }

    BenchmarkResult(
      "SpMV (MLlib IndexedRowMatrix)",
      s"${matrixSize}x${matrixSize}",
      customTime,
      mllibTime,
      speedup
    )
  }

  /** Benchmark 2: SpMM using MLlib's BlockMatrix.multiply()
    */
  def benchmarkMLlibSpMM(
      sc: SparkContext,
      matrixSize: Int,
      dataDir: String
  ): BenchmarkResult = {

    println("\n" + "=" * 80)
    println("BENCHMARK: SpMM - Custom Engine vs MLlib BlockMatrix")
    println("=" * 80)

    val matrixA = COOLoader.loadSparseMatrix(
      sc,
      s"$dataDir/sparse_matrix_${matrixSize}x${matrixSize}.csv"
    )
    val matrixB = COOLoader.loadSparseMatrix(
      sc,
      s"$dataDir/sparse_matrix_${matrixSize}x${matrixSize}.csv"
    )

    println(s"Matrix A: ${matrixSize}x${matrixSize}")
    println(s"Matrix B: ${matrixSize}x${matrixSize}")

    // === CUSTOM ENGINE ===
    println("\n--- Custom Sparse Matrix Engine ---")
    val customStart = System.nanoTime()

    val customResult = MultiplicationOps.sparseMatrixSparseMatrix(
      matrixA,
      matrixB
    )
    val customCount = customResult.count()

    val customTime = (System.nanoTime() - customStart) / 1000000.0

    println(f"Custom Engine: ${customTime}%.2f ms")
    println(s"Result NNZ: $customCount")

    // === MLLIB BlockMatrix ===
    println("\n--- MLlib BlockMatrix.multiply() ---")

    val mllibStart = System.nanoTime()

    // Convert to BlockMatrix
    val coordA = new CoordinateMatrix(
      matrixA.map(e => MatrixEntry(e.row.toLong, e.col.toLong, e.value))
    )
    val coordB = new CoordinateMatrix(
      matrixB.map(e => MatrixEntry(e.row.toLong, e.col.toLong, e.value))
    )

    val blockMatrixA = coordA.toBlockMatrix().cache()
    val blockMatrixB = coordB.toBlockMatrix().cache()

    // Force materialization
    blockMatrixA.blocks.count()
    blockMatrixB.blocks.count()

    // Multiply using MLlib
    val mllibResultBlocks = blockMatrixA.multiply(blockMatrixB)
    val mllibResult = mllibResultBlocks.toCoordinateMatrix()
    val mllibCount = mllibResult.entries.count()

    val mllibTime = (System.nanoTime() - mllibStart) / 1000000.0

    println(f"MLlib: ${mllibTime}%.2f ms")
    println(s"Result NNZ: $mllibCount")

    // Unpersist the cached RDDs inside BlockMatrix
    blockMatrixA.blocks.unpersist()
    blockMatrixB.blocks.unpersist()

    // === COMPARISON ===
    val speedup = mllibTime / customTime
    println("\n" + "-" * 80)
    println(f"Speedup: ${speedup}%.2fx")
    if (speedup > 1.0) {
      println("✓ Custom engine is FASTER")
    } else {
      println("✗ MLlib is faster")
    }

    BenchmarkResult(
      "SpMM (MLlib BlockMatrix)",
      s"${matrixSize}x${matrixSize}",
      customTime,
      mllibTime,
      speedup
    )
  }

  /** Benchmark 3: Matrix Transpose
    */
  def benchmarkMLlibTranspose(
      sc: SparkContext,
      matrixSize: Int,
      dataDir: String
  ): BenchmarkResult = {

    println("\n" + "=" * 80)
    println("BENCHMARK: Transpose - Custom Engine vs MLlib")
    println("=" * 80)

    val matrix = COOLoader.loadSparseMatrix(
      sc,
      s"$dataDir/sparse_matrix_${matrixSize}x${matrixSize}.csv"
    )

    // === CUSTOM ENGINE ===
    println("\n--- Custom Engine ---")
    val customStart = System.nanoTime()

    val customTranspose = matrix.map(e => COOEntry(e.col, e.row, e.value))
    val customCount = customTranspose.count()

    val customTime = (System.nanoTime() - customStart) / 1000000.0

    println(f"Custom Engine: ${customTime}%.2f ms")

    // === MLLIB ===
    println("\n--- MLlib CoordinateMatrix.transpose() ---")

    val coordMatrix = new CoordinateMatrix(
      matrix.map(e => MatrixEntry(e.row.toLong, e.col.toLong, e.value))
    )

    val mllibStart = System.nanoTime()

    val mllibTranspose = coordMatrix.transpose()
    val mllibCount = mllibTranspose.entries.count()

    val mllibTime = (System.nanoTime() - mllibStart) / 1000000.0

    println(f"MLlib: ${mllibTime}%.2f ms")

    // === COMPARISON ===
    val speedup = mllibTime / customTime
    println("\n" + "-" * 80)
    println(f"Speedup: ${speedup}%.2fx")

    BenchmarkResult(
      "Transpose (MLlib)",
      s"${matrixSize}x${matrixSize}",
      customTime,
      mllibTime,
      speedup
    )
  }

  /** Benchmark 4: Gram Matrix using MLlib's RowMatrix.computeGramianMatrix()
    */
  def benchmarkMLlibGramMatrix(
      sc: SparkContext,
      matrixSize: Int,
      dataDir: String
  ): BenchmarkResult = {

    println("\n" + "=" * 80)
    println("BENCHMARK: Gram Matrix (A^T × A) - Custom vs MLlib")
    println("=" * 80)

    val matrix = COOLoader.loadSparseMatrix(
      sc,
      s"$dataDir/sparse_matrix_${matrixSize}x${matrixSize}.csv"
    )

    // === CUSTOM ENGINE ===
    println("\n--- Custom Engine (A^T × A) ---")
    val customStart = System.nanoTime()

    val transpose = matrix.map(e => COOEntry(e.col, e.row, e.value))
    val gram = MultiplicationOps.sparseMatrixSparseMatrix(transpose, matrix)
    val customCount = gram.count()

    val customTime = (System.nanoTime() - customStart) / 1000000.0

    println(f"Custom Engine: ${customTime}%.2f ms")
    println(s"Result NNZ: $customCount")

    // === MLLIB RowMatrix.computeGramianMatrix() ===
    println("\n--- MLlib RowMatrix.computeGramianMatrix() ---")

    val mllibStart = System.nanoTime()

    // Convert to IndexedRowMatrix
    val indexedRows = matrix
      .groupBy(_.row)
      .map { case (rowId, entries) =>
        val sorted = entries.toArray.sortBy(_.col)
        val indices = sorted.map(_.col)
        val values = sorted.map(_.value)
        IndexedRow(rowId.toLong, Vectors.sparse(matrixSize, indices, values))
      }
    
    val indexedMatrix = new IndexedRowMatrix(indexedRows)
    val rowMatrix = new org.apache.spark.mllib.linalg.distributed.RowMatrix(
      indexedMatrix.rows.map(_.vector)
    )

    // Compute Gram matrix
    val gramMatrix = rowMatrix.computeGramianMatrix()
    
    // Count non-zeros
    val mllibNNZ = gramMatrix.toArray.count(math.abs(_) > 1e-10)

    val mllibTime = (System.nanoTime() - mllibStart) / 1000000.0

    println(f"MLlib: ${mllibTime}%.2f ms")
    println(s"Result NNZ: $mllibNNZ")

    // === COMPARISON ===
    val speedup = mllibTime / customTime
    println("\n" + "-" * 80)
    println(f"Speedup: ${speedup}%.2fx")

    BenchmarkResult(
      "Gram Matrix (MLlib)",
      s"${matrixSize}x${matrixSize}",
      customTime,
      mllibTime,
      speedup
    )
  }

  /** Run all MLlib benchmarks
    */
  def runAllBenchmarks(
      sc: SparkContext,
      matrixSize: Int,
      dataDir: String
  ): Unit = {

    println("\n" + "=" * 80)
    println("MLLIB DISTRIBUTED MATRIX COMPARISON")
    println("=" * 80)
    println(s"Matrix Size: ${matrixSize}x${matrixSize}")
    println(s"Data Directory: $dataDir")
    println("\nComparing against:")
    println("  - MLlib IndexedRowMatrix for SpMV")
    println("  - MLlib BlockMatrix.multiply() for SpMM")
    println("  - MLlib CoordinateMatrix.transpose() for Transpose")
    println("  - MLlib RowMatrix.computeGramianMatrix() for Gram Matrix")

    val results = Seq(
      benchmarkMLlibSpMV(sc, matrixSize, dataDir),
      benchmarkMLlibSpMM(sc, matrixSize, dataDir),
      benchmarkMLlibTranspose(sc, matrixSize, dataDir),
      benchmarkMLlibGramMatrix(sc, matrixSize, dataDir)
    )

    // Print summary
    println("\n" + "=" * 80)
    println("MLLIB BENCHMARK SUMMARY")
    println("=" * 80)
    println()
    println(f"${"Operation"}%-35s ${"Custom (ms)"}%15s ${"MLlib (ms)"}%15s ${"Speedup"}%10s")
    println("-" * 80)

    results.foreach { r =>
      val status = if (r.speedup > 1.0) "✓" else "✗"
      println(f"${r.operation}%-35s ${r.customTime}%,15.2f ${r.mllibTime}%,15.2f ${r.speedup}%9.2fx $status")
    }

    println()
    val avgSpeedup = results.map(_.speedup).sum / results.size
    val wins = results.count(_.speedup > 1.0)
    val losses = results.length - wins
    
    println(f"Average Speedup: ${avgSpeedup}%.2fx")
    println(f"Custom Engine Wins: $wins/${results.length}")
    println(f"MLlib Wins: $losses/${results.length}")
    println()
    
    if (avgSpeedup > 1.0) {
      println("✓ Overall: Custom engine outperforms MLlib on average")
    } else {
      println("✗ Overall: MLlib outperforms custom engine on average")
    }
  }
}

object ActualMLlibBenchmarkRunner {

  def main(args: Array[String]): Unit = {

    val conf = new org.apache.spark.SparkConf()
      .setAppName("MLlib Distributed Matrix Comparison")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.memory", "8g")

    val sc = new org.apache.spark.SparkContext(conf)
    sc.setLogLevel("WARN")

    try {
      ActualMLlibBenchmark.runAllBenchmarks(
        sc,
        matrixSize = 1000,
        dataDir = "synthetic-data"
      )

    } catch {
      case e: Exception =>
        println(s"\nERROR: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      sc.stop()
    }
  }
}