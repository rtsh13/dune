package examples

import org.apache.spark.{SparkConf, SparkContext}
import engine.storage.{SmartLoader, DenseVectorRDD, SparseVector}

object Example03_TypeDetection {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
      .setAppName("Type Detection - NO COLLECT")
      .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    println("=" * 60)
    println("Example 3: Auto Type Detection")
    println("NO collect(), NO collectAsMap()")
    println("=" * 60)
    
    println("\n--- Loading High-Sparsity Matrix ---")
    val sparseMatrix = SmartLoader.loadMatrix(
      sc,
      "data/test/medium_sparse_1000x1000.csv"
    )
    println(s"Result: $sparseMatrix")
    println(s"Is sparse: ${sparseMatrix.isSparse}")
    
    println("\n--- Loading Identity Matrix ---")
    val identityMatrix = SmartLoader.loadMatrix(
      sc,
      "data/test/identity_10x10.csv"
    )
    println(s"Result: $identityMatrix")
    println(s"Is sparse: ${identityMatrix.isSparse}")
    
    println("\n--- Forcing Dense Representation ---")
    val forcedDense = SmartLoader.loadMatrix(
      sc,
      "data/test/small_sparse_10x10.csv",
      forceDense = true
    )
    println(s"Result: $forcedDense")
    println(s"Is sparse: ${forcedDense.isSparse}")
    
    println("\n--- Loading Dense Vector as RDD ---")
    val denseVec = SmartLoader.loadVector(
      sc,
      "data/test/dense_vector_10.csv"
    )
    println(s"Result: $denseVec")
    
    println("First 5 vector entries (using take):")
    denseVec.toIndexValueRDD.sortByKey().take(5).foreach { case (idx, value) =>
      println(f"  [$idx] = $value%.3f")
    }
    
    println("\n--- Loading Sparse Vector ---")
    val sparseVec = SmartLoader.loadVector(
      sc,
      "data/test/sparse_vector_10.csv",
      forceSparse = true
    )
    println(s"Result: $sparseVec")
    
    println("\n✓ All detection done without ANY collect operations")
    println("✓ All data structures remain as RDDs")
    
    sc.stop()
  }
}