package engine.tensor

import org.apache.spark.rdd.RDD

// Tensor data types

/**
 * COO format for tensors
 * Stores non-zero entries as (indices, value)
 */
case class TensorEntry(
  indices: Array[Int],  // [i, j, k, ...] for n-dimensional tensor
  value: Double
) {
  def isZero: Boolean = math.abs(value) < 1e-10
  
  def order: Int = indices.length
  
  override def toString: String = {
    s"Tensor[${indices.mkString(", ")}] = $value"
  }
}

// Sparse Tensor (distributed)
case class SparseTensor(
  entries: RDD[TensorEntry],
  dimensions: Array[Int]  // Size of each mode [I, J, K, ...]
) {
  
  // Order of tensor (number of dimensions)
  def order: Int = dimensions.length
  
  // Number of non-zero entries
  lazy val numNonZeros: Long = entries.count()
  
  // Total number of elements (if dense)
  def totalElements: Long = dimensions.map(_.toLong).product
  
  // Sparsity
  def sparsity: Double = 1.0 - (numNonZeros.toDouble / totalElements)
  
  override def toString: String = {
    f"SparseTensor(${dimensions.mkString(" × ")}, $numNonZeros non-zeros, ${sparsity*100}%.1f%% sparse)"
  }
}

/**
 * Factor matrix for tensor decomposition
 * Used in CP decomposition and similar algorithms
 */
case class FactorMatrix(
  entries: RDD[(Int, Array[Double])],  // (row_index, [factor values])
  numRows: Int,
  rank: Int  // Number of factors
) {
  override def toString: String = {
    s"FactorMatrix($numRows × $rank)"
  }
}