// src/main/scala/benchmarks/AblationStudy.scala
package benchmarks

import org.apache.spark.{SparkContext, HashPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import engine.operations.MultiplicationOps
import scala.collection.mutable.ArrayBuffer
import engine.storage._
import engine.operations.MultiplicationOps
import engine.optimization.OptimizedOps
import engine.storage.FormatConverter

object AblationStudy {
  
  sealed trait OptimizationFeature
  case object Partitioning extends OptimizationFeature
  case object Caching extends OptimizationFeature
  case object CSRFormat extends OptimizationFeature
  case object BlockPartitioning extends OptimizationFeature
  
  def runAblationStudy(
    sc: SparkContext,
    matrixSize: Int,
    sparsity: Double,
    dataDir: String = "synthetic-data",
    iterations: Int = 5
  ): Map[Set[OptimizationFeature], Double] = {
    
    println("\n" + "="*80)
    println("ABLATION STUDY")
    println("="*80)
    println(s"Matrix Size: ${matrixSize}x${matrixSize}")
    println(s"Testing all combinations of optimizations...")
    
    val results = scala.collection.mutable.Map[Set[OptimizationFeature], Double]()
    
    // Load data
    val matrixPath = s"$dataDir/sparse_matrix_${matrixSize}x${matrixSize}.csv"
    val vectorPath = s"$dataDir/dense_vector_${matrixSize}.csv"
    
    println(s"\nLoading data from:")
    println(s"  Matrix: $matrixPath")
    println(s"  Vector: $vectorPath")
    
    val matrix = SmartLoader.loadMatrix(sc, matrixPath).toCOO
    val vector = SmartLoader.loadVector(sc, vectorPath)
    
    println(s"\nMatrix: ${matrix.numNonZeros} non-zeros")
    println(s"Vector: ${vector.size} elements")
    
    // Test all combinations of features
    val features: Set[OptimizationFeature] = Set(Partitioning, Caching, CSRFormat)
    val combinations = features.subsets().toSeq.sortBy(_.size)
    
    println(s"\nTesting ${combinations.size} combinations of features...")
    
    for ((featureSet, idx) <- combinations.zipWithIndex) {
      val featureNames = if (featureSet.isEmpty) "Baseline (no optimizations)" 
                        else featureSet.mkString(", ")
      
      println(s"\n[${idx + 1}/${combinations.size}] Testing: $featureNames")
      
      val times = ArrayBuffer[Double]()
      
      for (i <- 1 to iterations) {
        System.gc()
        Thread.sleep(500)
        
        val start = System.nanoTime()
        
        val result = runWithFeatures(matrix, vector, featureSet, sc)
        result.count()
        
        val elapsed = (System.nanoTime() - start) / 1000000.0
        times += elapsed
        
        println(f"  Iteration $i: ${elapsed}%,.2f ms")
      }
      
      val avgTime = times.sum / iterations
      val stdDev = calculateStdDev(times, avgTime)
      
      results(featureSet) = avgTime
      
      println(f"  Average: ${avgTime}%,.2f ms ± ${stdDev}%.2f ms")
    }
    
    // Print summary
    printAblationSummary(results.toMap)
    
    results.toMap
  }
  
  private def runWithFeatures(
    matrix: SparseMatrixCOO,
    vector: Vector,
    features: Set[OptimizationFeature],
    sc: SparkContext
  ): RDD[(Int, Double)] = {
    
    var matrixData = matrix.entries
    var vectorData = vector.toIndexValueRDD
    
    // Apply partitioning
    if (features.contains(Partitioning)) {
      val partitioner = new HashPartitioner(sc.defaultParallelism)
      matrixData = matrixData
        .map(e => (e.col, (e.row, e.value)))
        .partitionBy(partitioner)
        .map { case (col, (row, value)) => COOEntry(row, col, value) }
      
      vectorData = vectorData.partitionBy(partitioner)
    }
    
    // Apply caching
    if (features.contains(Caching)) {
      matrixData = matrixData.persist(StorageLevel.MEMORY_AND_DISK)
      vectorData = vectorData.persist(StorageLevel.MEMORY_AND_DISK)
      matrixData.count() // materialize
      vectorData.count()
    }
    
    // Use CSR format
    val result = if (features.contains(CSRFormat)) {
      val csrMatrix = FormatConverter.cooToDistributedCSR(
        matrixData, matrix.numRows, matrix.numCols
      )
      MultiplicationOps.csrMatrixDenseVector(csrMatrix, vectorData)
    } else {
      MultiplicationOps.sparseMatrixDenseVector(matrixData, vectorData)
    }
    
    // Cleanup
    if (features.contains(Caching)) {
      matrixData.unpersist(blocking = false)
      vectorData.unpersist(blocking = false)
    }
    
    result
  }
  
  private def printAblationSummary(results: Map[Set[OptimizationFeature], Double]): Unit = {
    println("\n" + "="*80)
    println("ABLATION STUDY SUMMARY")
    println("="*80)
    
    val sorted = results.toSeq.sortBy(_._2)
    val baseline = results.find(_._1.isEmpty).map(_._2).getOrElse(sorted.head._2)
    
    println("\n| Rank | Features | Time (ms) | vs Baseline | vs Best |")
    println("|------|----------|-----------|-------------|---------|")
    
    sorted.zipWithIndex.foreach { case ((features, time), idx) =>
      val featureStr = if (features.isEmpty) "None (Baseline)" 
                      else features.mkString(", ")
      val vsBaseline = baseline / time
      val vsBest = sorted.head._2 / time
      
      val rankSymbol = if (idx == 0) "🥇" else if (idx == 1) "🥈" else if (idx == 2) "🥉" else s"${idx + 1}"
      
      println(f"| $rankSymbol | $featureStr%-30s | ${time}%,.2f | ${vsBaseline}%.2fx | ${vsBest}%.2fx |")
    }
    
    println("\n### Key Findings:")
    
    // Find best single optimization
    val singleOptimizations = results.filter(_._1.size == 1)
    if (singleOptimizations.nonEmpty) {
      val bestSingle = singleOptimizations.minBy(_._2)
      val improvement = ((baseline - bestSingle._2) / baseline) * 100
      println(f"- Best single optimization: ${bestSingle._1.head} (${improvement}%+.1f%% improvement)")
    }
    
    // Find best combination
    val bestOverall = sorted.head
    val overallImprovement = ((baseline - bestOverall._2) / baseline) * 100
    val features = if (bestOverall._1.isEmpty) "None" else bestOverall._1.mkString(" + ")
    println(f"- Best overall: $features (${overallImprovement}%+.1f%% improvement)")
    
    // Check for diminishing returns
    val allOptimizations = results.find(_._1 == Set(Partitioning, Caching, CSRFormat))
    if (allOptimizations.isDefined) {
      val allOptTime = allOptimizations.get._2
      val allOptImprovement = ((baseline - allOptTime) / baseline) * 100
      println(f"- All optimizations combined: ${allOptImprovement}%+.1f%% improvement")
    }
  }
  
  private def calculateStdDev(values: Seq[Double], mean: Double): Double = {
    if (values.length <= 1) return 0.0
    val variance = values.map(v => math.pow(v - mean, 2)).sum / (values.length - 1)
    math.sqrt(variance)
  }
}