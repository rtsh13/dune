package benchmarks

import org.apache.spark.{SparkContext, HashPartitioner}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import engine.storage._
import engine.operations.MultiplicationOps
import engine.optimization.{OptimizedOps, AdaptiveOps}
import scala.collection.mutable.ArrayBuffer

object AblationStudy {

  def runAblationStudy(
    sc: SparkContext,
    matrixSize: Int,
    sparsity: Double,
    dataDir: String = "synthetic-data",
    iterations: Int = 3
  ): Map[Set[String], Double] = {

    println("\n" + "="*80)
    println("ABLATION STUDY: Impact of Individual Optimizations")
    println("="*80)
    println(s"Matrix: ${matrixSize}x${matrixSize}, Sparsity: ${sparsity*100}%")

    val matrixPath = s"$dataDir/sparse_matrix_${matrixSize}x${matrixSize}.csv"
    val vectorPath = s"$dataDir/dense_vector_${matrixSize}.csv"

    // Check files exist
    if (!new java.io.File(matrixPath).exists()) {
      println(s"ERROR: Matrix file not found: $matrixPath")
      return Map.empty
    }

    val matrixA = SmartLoader.loadMatrix(sc, matrixPath).toCOO
    val vectorX = SmartLoader.loadVector(sc, vectorPath)

    println("\nTesting optimization combinations...")

    // Define optimization features to test
    val features = Seq(
      Set[String](),                                    // Baseline
      Set("CoPartitioning"),                            // Co-partitioning
      Set("InPartitionAgg"),                            // In-partition aggregation
      Set("CoPartitioning", "InPartitionAgg"),          // Both
      Set("Balanced"),                                  // Balanced partitioning
      Set("CSRFormat")                                  // CSR format
    )

    val results = features.map { featureSet =>
      println(s"\n--- Testing: ${if (featureSet.isEmpty) "Baseline" else featureSet.mkString(" + ")} ---")

      val times = (1 to iterations).map { i =>
        System.gc()
        Thread.sleep(1000)

        val start = System.nanoTime()

        val result = applyOptimizations(
          matrixA.entries,
          vectorX.toIndexValueRDD,
          featureSet,
          sc.defaultParallelism * 2
        )

        result.count()

        val elapsed = (System.nanoTime() - start) / 1000000.0
        println(f"  Iteration $i: ${elapsed}%.2f ms")
        elapsed
      }

      val avgTime = times.sum / times.length
      println(f"  Average: ${avgTime}%.2f ms")

      (featureSet, avgTime)
    }.toMap

    printAblationSummary(results)

    results
  }

  private def applyOptimizations(
    matrixA: RDD[COOEntry],
    vectorX: RDD[(Int, Double)],
    features: Set[String],
    numPartitions: Int
  ): RDD[(Int, Double)] = {

    if (features.isEmpty) {
      // Baseline - simple join
      val matrixByCol = matrixA.map(e => (e.col, (e.row, e.value)))
      val joined = matrixByCol.join(vectorX)
      joined.map { case (col, ((row, mVal), vVal)) => (row, mVal * vVal) }
        .reduceByKey(_ + _)

    } else if (features.contains("CSRFormat")) {
      // CSR format
      val csrMatrix = FormatConverter.cooToDistributedCSR(matrixA, 
        matrixA.map(_.row).max() + 1,
        matrixA.map(_.col).max() + 1
      )
      MultiplicationOps.csrMatrixDenseVector(csrMatrix, vectorX)

    } else if (features.contains("Balanced")) {
      // Balanced partitioning
      AdaptiveOps.balancedSpMV(matrixA, vectorX)

    } else if (features.contains("CoPartitioning") && features.contains("InPartitionAgg")) {
      // Both optimizations
      AdaptiveOps.efficientSpMV(matrixA, vectorX)

    } else if (features.contains("CoPartitioning")) {
      // Just co-partitioning
      AdaptiveOps.mapSideJoinSpMV(matrixA, vectorX)

    } else if (features.contains("InPartitionAgg")) {
      // Just in-partition aggregation
      val partitioner = new HashPartitioner(numPartitions)
      val matrixByCol = matrixA.map(e => (e.col, (e.row, e.value))).partitionBy(partitioner)
      val vectorPartitioned = vectorX.partitionBy(partitioner)

      matrixByCol.join(vectorPartitioned).mapPartitions { partition =>
        val accumulator = scala.collection.mutable.HashMap[Int, Double]()
        partition.foreach { case (col, ((row, mVal), vVal)) =>
          accumulator(row) = accumulator.getOrElse(row, 0.0) + mVal * vVal
        }
        accumulator.iterator
      }.reduceByKey(_ + _)

    } else {
      // Fallback
      MultiplicationOps.sparseMatrixDenseVector(matrixA, vectorX)
    }
  }

  private def printAblationSummary(results: Map[Set[String], Double]): Unit = {
    println("\n" + "="*80)
    println("ABLATION STUDY SUMMARY")
    println("="*80)

    val baseline = results.get(Set.empty).getOrElse(0.0)

    println("\n| Configuration | Time (ms) | vs Baseline |")
    println("|---------------|-----------|-------------|")

    results.toSeq.sortBy(_._2).foreach { case (features, time) =>
      val improvement = if (baseline > 0) ((baseline - time) / baseline) * 100 else 0.0
      val name = if (features.isEmpty) "Baseline" else features.mkString(" + ")
      println(f"| $name%-30s | ${time}%9.2f | ${improvement}%+10.1f%% |")
    }

    val best = results.minBy(_._2)
    val bestImprovement = if (baseline > 0) ((baseline - best._2) / baseline) * 100 else 0.0

    println(f"\nBest configuration: ${if (best._1.isEmpty) "Baseline" else best._1.mkString(" + ")}")
    println(f"Improvement: ${bestImprovement}%+.1f%%")
  }
}