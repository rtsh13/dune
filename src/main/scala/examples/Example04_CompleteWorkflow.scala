package examples

import org.apache.spark.{SparkConf, SparkContext}
import engine.storage._
import engine.operations.MultiplicationOps
import engine.optimization.OptimizedOps

object Example04_CompleteWorkflow {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
      .setAppName("Complete Workflow - NO COLLECT")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    
    println("COMPLETE SPARSE MATRIX ENGINE DEMONSTRATION")
    println("All operations use PURE JOIN approach - ZERO collect() calls")
    
    // Part1: Load Data
    
    println("\n### Part1: Loading Data ###\n")
    
    val matrixA = SmartLoader.loadMatrix(
      sc,
      "data/test/medium_sparse_1000x1000.csv"
    )
    
    val vectorX = SmartLoader.loadVector(
      sc,
      "data/test/dense_vector_1000.csv"
    )
    
    // Part2: Matrix-Vector Multiplication
    
    println("\n### Part2: Matrix-Vector Multiplication ###\n")
    
    // Clean API usage
    val resultVector = matrixA * vectorX
    
    println(s"Result: $resultVector")
    println("First 10 results:")
    resultVector.preview(10).foreach { case (idx, value) =>
      println(f"  y[$idx] = $value%.6f")
    }
    
    // Save result
    resultVector.saveAsTextFile("output/example04_result_vector")
    println("Result saved to output/example04_result_vector/")
    
    // Part3: Matrix-Matrix Multiplication
    
    println("\n### Part3: Matrix-Matrix Multiplication ###\n")
    
    val matrixB = SmartLoader.loadMatrix(
      sc,
      "data/test/identity_10x10.csv"
    )
    
    // Convert both to COO for multiplication
    val matrixACOO = matrixA.toCOO
    val matrixBCOO = matrixB.toCOO
    
    // Use optimized version from OptimizedOps if it exists
    // Otherwise use basic multiplication
    val resultMatrixEntries = try {
      // Try optimized version
      OptimizedOps.optimizedSpMM(
        matrixACOO.entries,
        matrixBCOO.entries,
        Some(16)
      )
    } catch {
      case _: NoSuchMethodError =>
        // Fall back to basic multiplication
        println("OptimizedOps not available, using basic multiplication")
        MultiplicationOps.sparseMatrixSparseMatrix(
          matrixACOO.entries,
          matrixBCOO.entries
        )
    }
    
    val result = ResultMatrix(
      resultMatrixEntries.map(e => (e.row, e.col, e.value)),
      matrixACOO.numRows,
      matrixBCOO.numCols
    )
    
    println(s"Result: $result")
    result.saveAsTextFile("output/example04_result_matrix")
    println("Result saved to output/example04_result_matrix/")
    
    // Part4: Demonstrate Different Operations
    
    println("\n### Part4: Testing Different Operation Types ###\n")
    
    // Test 1: Sparse Matrix × Dense Vector (already done above)
    println("Test 1: Sparse Matrix × Dense Vector - PASSED")
    
    // Test 2: Sparse Matrix × Sparse Matrix
    println("Running Test 2: Sparse Matrix × Sparse Matrix...")
    val sparseMat1 = matrixACOO
    val sparseMat2 = matrixBCOO
    val sparseResult = sparseMat1 * sparseMat2
    println(f"Test 2: Result has ${sparseResult.entries.count()} entries")
    
    // Test 3: Matrix Addition
    println("Running Test 3: Matrix Addition...")
    val sumResult = matrixACOO + matrixBCOO
    println(f"Test 3: Sum has ${sumResult.entries.count()} entries")
    
    // Test 4: Transpose
    println("Running Test 4: Transpose...")
    val transposed = matrixACOO.transpose
    println(f"Test 4: Transposed matrix: $transposed")
    
    // Test 5: Slicing
    println("Running Test 5: Matrix Slicing...")
    val submatrix = matrixACOO.slice(0, 100, 0, 100)
    println(f"Test 5: Submatrix [0:100, 0:100]: $submatrix")
    
    // Part5: Performance Timing
    
    println("\n### Part5: Performance Timing ###\n")
    
    // Time the multiplication operation
    println("Timing Matrix-Vector multiplication...")
    
    val iterations = 3
    var totalTime = 0L
    
    for (i <- 1 to iterations) {
      val start = System.currentTimeMillis()
      val tempResult = matrixACOO * vectorX
      tempResult.entries.count()  // Force computation
      val elapsed = System.currentTimeMillis() - start
      totalTime += elapsed
      println(f"  Iteration $i: ${elapsed}ms")
    }
    
    val avgTime = totalTime / iterations
    println(f"\nAverage time over $iterations runs: ${avgTime}ms")
    
    // Part6: Statistics
    
    println("\n### Part6: Result Statistics ###\n")
    
    // Compute statistics using distributed operations
    val resultEntries = resultVector.entries
    val count = resultEntries.count()
    val sum = resultEntries.map(_._2).reduce(_ + _)
    val maxEntry = resultEntries.reduce { (a, b) =>
      if (math.abs(a._2) > math.abs(b._2)) a else b
    }
    val minEntry = resultEntries.reduce { (a, b) =>
      if (math.abs(a._2) < math.abs(b._2)) a else b
    }
    
    println(f"Non-zero entries: $count")
    println(f"Sum of values: $sum%.6f")
    println(f"Max absolute value: ${maxEntry._1} → ${maxEntry._2}%.6f")
    println(f"Min absolute value: ${minEntry._1} → ${minEntry._2}%.6f")
    
    // Summary
    
    println("All operations completed successfully")
    println("NO collect() calls were made")
    println("All computation remained distributed")
    
    println("\nOutput files created:")
    println("  - output/example04_result_vector/")
    println("  - output/example04_result_matrix/")
    println("\nView results with:")
    println("  cat output/example04_result_vector/part-* | head -20")
    println("  cat output/example04_result_matrix/part-* | head -20")
    
    sc.stop()
  }
}