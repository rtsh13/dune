package realworldbenchmarks

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, IndexedRowMatrix, IndexedRow}
import org.apache.spark.mllib.linalg.Vectors
import engine.storage._
import engine.operations.MultiplicationOps

object MLlibComparison {

  case class BenchmarkResult(
      operation: String,
      customTime: Double,
      mllibTime: Double,
      speedup: Double
  )

  def benchmarkSpMV(sc: SparkContext, matrixSize: Int, dataDir: String): BenchmarkResult = {
    println("\n" +  * 80)
    println("BENCHMARK: SpMV - Custom vs MLlib")
    println( * 80)

    val matrix = COOLoader.loadSparseMatrix(sc, s"$dataDir/sparse_matrix_${matrixSize}x${matrixSize}.csv")
    val vector = COOLoader.loadDenseVectorRDD(sc, s"$dataDir/dense_vector_${matrixSize}.csv")

    // Custom Engine
    println("\n--- Custom Engine ---")
    val customStart = System.nanoTime()
    val customResult = MultiplicationOps.sparseMatrixDenseVector(matrix, vector)
    customResult.count()
    val customTime = (System.nanoTime() - customStart) / 1000000.0
    println(f"Custom: ${customTime}%.2f ms")

    // MLlib
    println("\n--- MLlib ---")
    val indexedRows = matrix.groupBy(_.row).map { case (rowId, entries) =>
      val sorted = entries.toArray.sortBy(_.col)
      IndexedRow(rowId.toLong, Vectors.sparse(matrixSize, sorted.map(_.col), sorted.map(_.value)))
    }
    val indexedMatrix = new IndexedRowMatrix(indexedRows)
    val vectorArray = vector.collect().sortBy(_._1).map(_._2)
    val vectorBC = sc.broadcast(vectorArray)

    val mllibStart = System.nanoTime()
    val mllibResult = indexedMatrix.rows.map { row =>
      var sum = 0.0
      row.vector match {
        case sv: org.apache.spark.mllib.linalg.SparseVector =>
          var i = 0
          while (i < sv.indices.length) {
            sum += sv.values(i) * vectorBC.value(sv.indices(i))
            i += 1
          }
      }
      (row.index.toInt, sum)
    }
    mllibResult.count()
    val mllibTime = (System.nanoTime() - mllibStart) / 1000000.0
    println(f"MLlib: ${mllibTime}%.2f ms")

    vectorBC.unpersist()

    val speedup = mllibTime / customTime
    println(f"\nSpeedup: ${speedup}%.2fx")

    BenchmarkResult("SpMV", customTime, mllibTime, speedup)
  }

  def benchmarkSpMM(sc: SparkContext, matrixSize: Int, dataDir: String): BenchmarkResult = {
    println("\n" +  * 80)
    println("BENCHMARK: SpMM - Custom vs MLlib BlockMatrix")
    println( * 80)

    val matrixA = COOLoader.loadSparseMatrix(sc, s"$dataDir/sparse_matrix_${matrixSize}x${matrixSize}.csv")
    val matrixB = COOLoader.loadSparseMatrix(sc, s"$dataDir/sparse_matrix_${matrixSize}x${matrixSize}.csv")

    // Custom Engine
    println("\n--- Custom Engine ---")
    val customStart = System.nanoTime()
    val customResult = MultiplicationOps.sparseMatrixSparseMatrix(matrixA, matrixB)
    customResult.count()
    val customTime = (System.nanoTime() - customStart) / 1000000.0
    println(f"Custom: ${customTime}%.2f ms")

    // MLlib BlockMatrix
    println("\n--- MLlib BlockMatrix ---")
    val mllibStart = System.nanoTime()
    val coordA = new CoordinateMatrix(matrixA.map(e => MatrixEntry(e.row.toLong, e.col.toLong, e.value)))
    val coordB = new CoordinateMatrix(matrixB.map(e => MatrixEntry(e.row.toLong, e.col.toLong, e.value)))
    val blockA = coordA.toBlockMatrix().cache()
    val blockB = coordB.toBlockMatrix().cache()
    blockA.blocks.count()
    blockB.blocks.count()
    val result = blockA.multiply(blockB)
    result.toCoordinateMatrix().entries.count()
    val mllibTime = (System.nanoTime() - mllibStart) / 1000000.0
    println(f"MLlib: ${mllibTime}%.2f ms")

    blockA.blocks.unpersist()
    blockB.blocks.unpersist()

    val speedup = mllibTime / customTime
    println(f"\nSpeedup: ${speedup}%.2fx")

    BenchmarkResult("SpMM", customTime, mllibTime, speedup)
  }

  def runAll(sc: SparkContext): Unit = {
    println("\n" +  * 80)
    println("MLLIB COMPARISON BENCHMARK")
    println( * 80)

    val results = Seq(
      benchmarkSpMV(sc, 1000, "synthetic-data"),
      benchmarkSpMM(sc, 1000, "synthetic-data")
    )

    println("\n" +  * 80)
    println("SUMMARY")
    println( * 80)
    results.foreach { r =>
      val status = if (r.speedup > 1.0) "Faster" else "Slower"
      println(f"${r.operation}%-10s: ${r.speedup}%.2fx $status")
    }
    val avg = results.map(_.speedup).sum / results.size
    println(f"\nAverage Speedup: ${avg}%.2fx")
  }
}

object MLlibComparisonRunner {
  def main(args: Array[String]): Unit = {
    val conf = new org.apache.spark.SparkConf()
      .setAppName("MLlib Comparison")
      .setMaster("local[*]")
      .set("spark.driver.memory", "8g")
    
    val sc = new org.apache.spark.SparkContext(conf)
    sc.setLogLevel("WARN")

    try {
      MLlibComparison.runAll(sc)
    } catch {
      case e: Exception =>
        println(s"ERROR: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      sc.stop()
    }
  }
}
