package engine.operations

import org.apache.spark.rdd.RDD
import engine.storage._

trait MatrixOperations[MatrixType] {
  def spMV(
      matrix: MatrixType,
      vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)]

  def spMVSparse(
      matrix: MatrixType,
      vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)]

  def spMMDense(
      matrix: MatrixType,
      matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)]

  def spMMSparse(
      matrixA: MatrixType,
      matrixB: MatrixType
  ): RDD[COOEntry]
}