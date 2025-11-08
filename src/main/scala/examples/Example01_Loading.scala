package examples

import org.apache.spark.{SparkConf, SparkContext}
import engine.storage.COOLoader

object Example01_Loading {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Loading Example - NO COLLECT, NO collectAsMap")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println("Example 1: Pure Distributed Loading")

    val cooMatrix = COOLoader.loadSparseMatrix(
      sc,
      "data/test/small_sparse_10x10.csv"
    )

    val (numRows, numCols) = COOLoader.getMatrixDimensions(cooMatrix)

    val numNonZeros = cooMatrix.count()
    val sparsity = COOLoader.calculateSparsity(numNonZeros, numRows, numCols)

    println("\nFirst 10 matrix entries (using take for display):")
    cooMatrix.take(10).foreach(println)

    // Load dense vector as RDD - NO ARRAY, NO collectAsMap
    val vectorRDD = COOLoader.loadDenseVectorRDD(
      sc,
      "data/test/dense_vector_10.csv"
    )

    val vectorSize = COOLoader.getVectorSize(vectorRDD)
    println(s"\nVector size (computed distributed): $vectorSize")

    // Display first few entries using take()
    println("Vector (first 5 entries using take):")
    vectorRDD.sortByKey().take(5).foreach { case (idx, value) =>
      println(f"  [$idx] = $value%.3f")
    }

    // Load sparse vector - STAYS DISTRIBUTED
    val sparseVec = COOLoader.loadSparseVector(
      sc,
      "data/test/sparse_vector_10.csv"
    )

    println(s"\nSparse vector (first 5 using take):")
    sparseVec.take(5).foreach(println)

    println(s"Total sparse vector entries: ${sparseVec.count()}")

    println("\n--- Saving to Files (Distributed Write) ---")

    cooMatrix
      .map(e => s"${e.row},${e.col},${e.value}")
      .saveAsTextFile("output/example01_matrix")
    println("Matrix saved to: output/example01_matrix/")

    vectorRDD
      .sortByKey()
      .map { case (idx, value) => s"$idx,$value" }
      .saveAsTextFile("output/example01_vector")
    println("Vector saved to: output/example01_vector/")

    println("\n--- View Results ---")
    println("cat output/example01_matrix/part-* | head")
    println("cat output/example01_vector/part-* | head")

    println("\nAll operations completed without ANY collect")

    sc.stop()
  }
}
