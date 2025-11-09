package benchmarks

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import engine.tensor._
import engine.storage._
import scala.collection.mutable.ArrayBuffer

object TensorBenchmarks {
  
  /**
   * Benchmark all tensor operations
   */
  def runTensorOperationsBenchmark(
    sc: SparkContext,
    dataDir: String = "synthetic-data",
    tensorSizes: Seq[(Int, Int, Int)] = Seq((10, 10, 10), (50, 50, 50), (100, 100, 100)),
    iterations: Int = 3
  ): Map[String, Map[String, Double]] = {
    
    println("TENSOR OPERATIONS BENCHMARK")
    println("Testing: Unfolding, Tensor-Matrix Product, Khatri-Rao")
    
    val allResults = scala.collection.mutable.Map[String, Map[String, Double]]()
    
    for ((dimI, dimJ, dimK) <- tensorSizes) {
      println(s"\n${"="*80}")
      println(s"Tensor Size: ${dimI}*${dimJ}*${dimK}")
      
      val results = scala.collection.mutable.Map[String, Double]()
      
      // Load or generate tensor
      val tensorPath = s"$dataDir/tensor_${dimI}x${dimJ}x${dimK}.csv"
      val tensor = try {
        TensorOps.loadSparseTensor3D(sc, tensorPath)
      } catch {
        case _: Exception =>
          println(s"  Generating new tensor: ${dimI}*${dimJ}*${dimK}...")
          generateAndSave3DTensor(sc, dimI, dimJ, dimK, 0.85, tensorPath)
          TensorOps.loadSparseTensor3D(sc, tensorPath)
      }
      
      println(s"  Tensor: $tensor")
      
      // Operation 1: Tensor Unfolding (Matricization) - All modes
      println("\n### Operation 1: Tensor Unfolding ###")
      
      for (mode <- 0 until tensor.order) {
        println(s"\n  Mode $mode unfolding:")
        val unfoldTimes = ArrayBuffer[Long]()
        
        for (i <- 1 to iterations) {
          System.gc()
          Thread.sleep(300)
          
          val start = System.nanoTime()
          val unfolded = TensorOps.unfold(tensor, mode)
          val count = unfolded.count()
          val elapsed = (System.nanoTime() - start) / 1000000
          
          unfoldTimes += elapsed
          println(f"    Iteration $i: ${elapsed}ms, result: $count entries")
        }
        
        val avgUnfold = unfoldTimes.sum.toDouble / iterations
        println(f"    Average: ${avgUnfold}%.2f ms")
        results(s"Unfold_mode$mode") = avgUnfold
      }
      
      // Operation 2: Tensor-Matrix Product
      println("\n### Operation 2: Tensor-Matrix Product ###")
      
      // Create factor matrix (mode-1 size * rank)
      val rank = 5
      val factorMatrix = sc.parallelize(
        (0 until dimJ).flatMap { row =>
          (0 until rank).map { col =>
            (row, col, scala.util.Random.nextGaussian())
          }
        }
      )
      
      println(s"  Factor matrix: ${dimJ}*${rank}")
      
      val tmpTimes = ArrayBuffer[Long]()
      
      for (i <- 1 to iterations) {
        System.gc()
        Thread.sleep(500)
        
        val start = System.nanoTime()
        val result = TensorOps.tensorMatrixProduct(
          tensor,
          factorMatrix,
          mode = 1
        )
        val count = result.entries.count()
        val elapsed = (System.nanoTime() - start) / 1000000
        
        tmpTimes += elapsed
        println(f"  Iteration $i: ${elapsed}ms, result: $count entries")
      }
      
      val avgTMP = tmpTimes.sum.toDouble / iterations
      println(f"  Average: ${avgTMP}%.2f ms")
      results("TensorMatrixProduct") = avgTMP
      
      // Operation 3: Khatri-Rao Product
      println("\n### Operation 3: Khatri-Rao Product ###")
      
      // Create two factor matrices
      val factorA = FactorMatrix(
        sc.parallelize((0 until dimI).map { row =>
          (row, Array.fill(rank)(scala.util.Random.nextGaussian()))
        }),
        numRows = dimI,
        rank = rank
      )
      
      val factorB = FactorMatrix(
        sc.parallelize((0 until dimJ).map { row =>
          (row, Array.fill(rank)(scala.util.Random.nextGaussian()))
        }),
        numRows = dimJ,
        rank = rank
      )
      
      println(s"  Factor A: ${dimI}*${rank}")
      println(s"  Factor B: ${dimJ}*${rank}")
      
      val krTimes = ArrayBuffer[Long]()
      
      for (i <- 1 to iterations) {
        System.gc()
        Thread.sleep(500)
        
        val start = System.nanoTime()
        val result = TensorOps.khatriRaoProduct(factorA, factorB)
        val count = result.count()
        val elapsed = (System.nanoTime() - start) / 1000000
        
        krTimes += elapsed
        println(f"  Iteration $i: ${elapsed}ms, result: $count entries")
      }
      
      val avgKR = krTimes.sum.toDouble / iterations
      println(f"  Average: ${avgKR}%.2f ms")
      results("KhatriRao") = avgKR
      
      // Store results for this size
      allResults(s"${dimI}x${dimJ}x${dimK}") = results.toMap
      
      // Summary for this size
      println(s"\n--- Summary for ${dimI}*${dimJ}*${dimK} ---")
      results.toSeq.sortBy(_._2).foreach { case (op, time) =>
        println(f"  $op%-25s: ${time}%8.2f ms")
      }
    }
    
    // Overall Summary
    println("TENSOR OPERATIONS SUMMARY")
    
    println("\n| Tensor Size | Unfold (avg) | Tensor*Matrix | Khatri-Rao |")
    println("|-------------|--------------|---------------|------------|")
    
    allResults.foreach { case (size, ops) =>
      val avgUnfold = ops.filter(_._1.startsWith("Unfold")).values.sum / 3
      val tmp = ops.getOrElse("TensorMatrixProduct", 0.0)
      val kr = ops.getOrElse("KhatriRao", 0.0)
      
      println(f"| $size%-11s | ${avgUnfold}%8.2f ms | ${tmp}%9.2f ms | ${kr}%8.2f ms |")
    }
    
    println("\n### Scalability Analysis:")
    if (allResults.size > 1) {
      val sizes = tensorSizes.map { case (i, j, k) => i * j * k }
      val times = allResults.values.map(_.values.sum).toSeq
      
      println(f"  Smallest tensor: ${sizes.min} elements")
      println(f"  Largest tensor: ${sizes.max} elements")
      println(f"  Size ratio: ${sizes.max.toDouble / sizes.min}%.1fx")
      println(f"  Time ratio: ${times.max / times.min}%.2fx")
      
      val scalingEfficiency = (Math.log(times.max / times.min) / Math.log(sizes.max.toDouble / sizes.min)) * 100
      println(f"  Scaling efficiency: ${scalingEfficiency}%.1f%%")
    }
    
    allResults.toMap
  }
  
  /**
   * Compare tensor unfolding performance across different modes
   */
  def compareTensorUnfoldingModes(
    sc: SparkContext,
    dataDir: String = "synthetic-data",
    tensorSize: (Int, Int, Int) = (100, 100, 100),
    iterations: Int = 5
  ): Map[Int, Double] = {
    
    println("TENSOR UNFOLDING MODE COMPARISON")
    
    val (dimI, dimJ, dimK) = tensorSize
    val tensorPath = s"$dataDir/tensor_${dimI}x${dimJ}x${dimK}.csv"
    
    val tensor = try {
      TensorOps.loadSparseTensor3D(sc, tensorPath)
    } catch {
      case _: Exception =>
        println(s"Generating tensor: ${dimI}*${dimJ}*${dimK}...")
        generateAndSave3DTensor(sc, dimI, dimJ, dimK, 0.85, tensorPath)
        TensorOps.loadSparseTensor3D(sc, tensorPath)
    }
    
    println(s"Tensor: $tensor")
    println(s"Testing unfolding along each of ${tensor.order} modes...\n")
    
    val results = scala.collection.mutable.Map[Int, Double]()
    
    for (mode <- 0 until tensor.order) {
      println(s"### Mode $mode ###")
      val times = ArrayBuffer[Long]()
      
      for (i <- 1 to iterations) {
        System.gc()
        Thread.sleep(300)
        
        val start = System.nanoTime()
        val unfolded = TensorOps.unfold(tensor, mode)
        val count = unfolded.count()
        val elapsed = (System.nanoTime() - start) / 1000000
        
        times += elapsed
        println(f"  Iteration $i: ${elapsed}ms")
      }
      
      val avg = times.sum.toDouble / iterations
      results(mode) = avg
      println(f"  Average: ${avg}%.2f ms\n")
    }
    
    // Summary
    println("UNFOLDING MODE COMPARISON SUMMARY")
    
    val fastest = results.values.min
    val slowest = results.values.max
    
    println(f"\nFastest mode: ${results.minBy(_._2)._1} (${fastest}%.2f ms)")
    println(f"Slowest mode: ${results.maxBy(_._2)._1} (${slowest}%.2f ms)")
    println(f"Performance variance: ${(slowest - fastest) / fastest * 100}%.1f%%")
    
    results.toMap
  }
  
  /**
   * Benchmark tensor operations at scale
   */
  def tensorScalabilityBenchmark(
    sc: SparkContext,
    dataDir: String = "synthetic-data",
    sizes: Seq[Int] = Seq(20, 40, 60, 80, 100),
    iterations: Int = 3
  ): Map[Int, Map[String, Double]] = {
    
    println("TENSOR SCALABILITY BENCHMARK")
    
    val results = scala.collection.mutable.Map[Int, Map[String, Double]]()
    
    for (size <- sizes) {
      println(s"\n### Cubic Tensor: ${size}*${size}*${size} ###")
      
      val tensorPath = s"$dataDir/tensor_${size}x${size}x${size}.csv"
      val tensor = try {
        TensorOps.loadSparseTensor3D(sc, tensorPath)
      } catch {
        case _: Exception =>
          generateAndSave3DTensor(sc, size, size, size, 0.85, tensorPath)
          TensorOps.loadSparseTensor3D(sc, tensorPath)
      }
      
      val opResults = scala.collection.mutable.Map[String, Double]()
      
      // Test unfolding
      val unfoldTimes = (1 to iterations).map { _ =>
        System.gc()
        Thread.sleep(300)
        val start = System.nanoTime()
        val result = TensorOps.unfold(tensor, 0)
        result.count()
        (System.nanoTime() - start) / 1000000.0
      }
      opResults("Unfold") = unfoldTimes.sum / iterations
      
      // Test tensor-matrix product
      val factorMatrix = sc.parallelize(
        (0 until size).flatMap { row =>
          (0 until 5).map { col =>
            (row, col, scala.util.Random.nextGaussian())
          }
        }
      )
      
      val tmpTimes = (1 to iterations).map { _ =>
        System.gc()
        Thread.sleep(500)
        val start = System.nanoTime()
        val result = TensorOps.tensorMatrixProduct(tensor, factorMatrix, 0)
        result.entries.count()
        (System.nanoTime() - start) / 1000000.0
      }
      opResults("TensorMatrixProduct") = tmpTimes.sum / iterations
      
      results(size) = opResults.toMap
      
      println(f"  Unfold: ${opResults("Unfold")}%.2f ms")
      println(f"  Tensor*Matrix: ${opResults("TensorMatrixProduct")}%.2f ms")
    }
    
    // Analyze scaling
    println("SCALABILITY ANALYSIS")
    
    println("\n| Size | Elements | Unfold (ms) | Tensor*Matrix (ms) |")
    println("|------|----------|-------------|-------------------|")
    
    results.toSeq.sortBy(_._1).foreach { case (size, ops) =>
      val elements = size * size * size
      println(f"| ${size}x${size}x${size} | ${elements}%,9d | ${ops("Unfold")}%8.2f | ${ops("TensorMatrixProduct")}%13.2f |")
    }
    
    // Compute scaling factor
    if (results.size > 1) {
      val firstSize = sizes.head
      val lastSize = sizes.last
      
      val unfoldSpeedup = results(lastSize)("Unfold") / results(firstSize)("Unfold")
      val tmpSpeedup = results(lastSize)("TensorMatrixProduct") / results(firstSize)("TensorMatrixProduct")
      val sizeIncrease = (lastSize.toDouble / firstSize) * (lastSize.toDouble / firstSize) * (lastSize.toDouble / firstSize)
      
      println(f"\n### Scaling from ${firstSize}³ to ${lastSize}³:")
      println(f"  Size increase: ${sizeIncrease}%.1fx")
      println(f"  Unfold time increase: ${unfoldSpeedup}%.2fx")
      println(f"  Tensor*Matrix time increase: ${tmpSpeedup}%.2fx")
      println(f"  Unfold scaling efficiency: ${(Math.log(unfoldSpeedup) / Math.log(sizeIncrease)) * 100}%.1f%%")
      println(f"  Tensor*Matrix scaling efficiency: ${(Math.log(tmpSpeedup) / Math.log(sizeIncrease)) * 100}%.1f%%")
    }
    
    results.toMap
  }
  
  /**
   * Generate and save a 3D tensor
   */
  private def generateAndSave3DTensor(
    sc: SparkContext,
    dimI: Int,
    dimJ: Int,
    dimK: Int,
    sparsity: Double,
    filepath: String
  ): Unit = {
    
    println(s"  Generating ${dimI}*${dimJ}*${dimK} tensor (${sparsity*100}% sparse)...")
    
    val totalElements = dimI.toLong * dimJ * dimK
    val numNonZeros = ((1.0 - sparsity) * totalElements).toInt
    
    val entries = scala.collection.mutable.Set[(Int, Int, Int)]()
    val random = new scala.util.Random(42)
    
    while (entries.size < numNonZeros) {
      val i = random.nextInt(dimI)
      val j = random.nextInt(dimJ)
      val k = random.nextInt(dimK)
      entries.add((i, j, k))
    }
    
    val writer = new java.io.PrintWriter(filepath)
    writer.println("i,j,k,value")
    
    entries.foreach { case (i, j, k) =>
      val value = random.nextGaussian() * 10
      writer.println(s"$i,$j,$k,$value")
    }
    
    writer.close()
    println(s"  Saved: $filepath")
  }
}

object TensorBenchmarksRunner {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
      .setAppName("Tensor Operations Benchmark")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.memory", "4g")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    
    try {
      // Create data directory
      new java.io.File("synthetic-data").mkdirs()
      new java.io.File("results").mkdirs()
      
      println("COMPREHENSIVE TENSOR BENCHMARKS")
      
      // Test 1: All tensor operations
      val opResults = TensorBenchmarks.runTensorOperationsBenchmark(
        sc,
        dataDir = "synthetic-data",
        tensorSizes = Seq((10, 10, 10), (50, 50, 50), (100, 100, 100)),
        iterations = 3
      )
      
      // Test 2: Mode comparison
      val modeResults = TensorBenchmarks.compareTensorUnfoldingModes(
        sc,
        dataDir = "synthetic-data",
        tensorSize = (100, 100, 100),
        iterations = 5
      )
      
      // Test 3: Scalability
      val scaleResults = TensorBenchmarks.tensorScalabilityBenchmark(
        sc,
        dataDir = "synthetic-data",
        sizes = Seq(20, 40, 60, 80, 100),
        iterations = 3
      )
      
      // Generate report
      val writer = new java.io.PrintWriter("results/tensor_benchmark_report.md")
      
      writer.println("# Tensor Operations Benchmark Report")
      writer.println()
      writer.println("## 1. Tensor Operations Performance")
      writer.println()
      writer.println("| Tensor Size | Unfold (avg) | Tensor*Matrix | Khatri-Rao |")
      writer.println("|-------------|--------------|---------------|------------|")
      
      opResults.foreach { case (size, ops) =>
        val avgUnfold = ops.filter(_._1.startsWith("Unfold")).values.sum / 3
        val tmp = ops.getOrElse("TensorMatrixProduct", 0.0)
        val kr = ops.getOrElse("KhatriRao", 0.0)
        writer.println(f"| $size | ${avgUnfold}%.2f ms | ${tmp}%.2f ms | ${kr}%.2f ms |")
      }
      
      writer.println()
      writer.println("## 2. Unfolding Mode Comparison")
      writer.println()
      writer.println("| Mode | Time (ms) |")
      writer.println("|------|-----------|")
      
      modeResults.toSeq.sortBy(_._1).foreach { case (mode, time) =>
        writer.println(f"| Mode $mode | ${time}%.2f |")
      }
      
      writer.println()
      writer.println("## 3. Scalability Analysis")
      writer.println()
      writer.println("| Size | Unfold (ms) | Tensor*Matrix (ms) |")
      writer.println("|------|-------------|-------------------|")
      
      scaleResults.toSeq.sortBy(_._1).foreach { case (size, ops) =>
        writer.println(f"| ${size}³ | ${ops("Unfold")}%.2f | ${ops("TensorMatrixProduct")}%.2f |")
      }
      
      writer.close()
      
      println("TENSOR BENCHMARKS COMPLETE")
      println("\nReport: results/tensor_benchmark_report.md")
      
    } catch {
      case e: Exception =>
        println(s"\n✗ ERROR: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      sc.stop()
    }
  }
}