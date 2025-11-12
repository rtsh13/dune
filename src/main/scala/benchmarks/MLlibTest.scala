package realworldbenchmarks

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}

object MLlibTest {
  def main(args: Array[String]): Unit = {
    println("MLlib is available!")
    val vec = Vectors.dense(1.0, 2.0, 3.0)
    println(s"Test vector: $vec")
  }
}