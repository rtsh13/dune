package examples

import org.apache.spark.{SparkConf, SparkContext}
import engine.tensor._

object Example05_TensorOperations {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Tensor Operations - NO COLLECT")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    println("TENSOR OPERATIONS DEMONSTRATION")

    // Create a sample 3D tensor

    println("\n### Creating Sample 3D Tensor ###\n")

    // Create tensor entries: (i, j, k) → value
    val tensorData = sc.parallelize(
      Seq(
        TensorEntry(Array(0, 0, 0), 1.0),
        TensorEntry(Array(0, 1, 0), 2.0),
        TensorEntry(Array(1, 0, 1), 3.0),
        TensorEntry(Array(1, 1, 1), 4.0),
        TensorEntry(Array(2, 0, 0), 5.0),
        TensorEntry(Array(2, 1, 1), 6.0)
      )
    )

    val tensor = SparseTensor(
      tensorData,
      dimensions = Array(3, 2, 2) // 3×2×2 tensor
    )

    println(s"Tensor: $tensor")
    println(s"Order: ${tensor.order}")
    println(s"Total elements: ${tensor.totalElements}")

    // Tensor Unfolding (Matricization)

    println("\n### Tensor Unfolding ###\n")

    // Unfold along mode 0 (first dimension)
    val unfolded = TensorOps.unfold(tensor, mode = 0)

    println("Unfolded tensor (mode 0):")
    println("First 10 entries:")
    unfolded.take(10).foreach { case (row, col, value) =>
      println(f"  Matrix[$row, $col] = $value%.1f")
    }

    // Demonstrate Tensor-Matrix Product

    println("\n### Tensor-Matrix Product ###\n")

    // Create a factor matrix (2×3)
    val factorMatrix = sc.parallelize(
      Seq(
        (0, 0, 0.5),
        (0, 1, 0.5),
        (1, 0, 1.0),
        (1, 1, 1.0)
      )
    )

    val result = TensorOps.tensorMatrixProduct(
      tensor,
      factorMatrix,
      mode = 1 // Multiply along mode 1
    )

    println(s"Result tensor: $result")

    println("Tensor operations completed")
    println("All operations remained distributed")

    sc.stop()
  }
}
