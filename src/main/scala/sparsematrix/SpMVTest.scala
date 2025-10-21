package sparsematrix

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

class SpMVTest extends AnyFunSuite {
  
  test("SpMV with identity matrix") {
    val entries = List(
      MatrixEntry(0, 0, 1.0),
      MatrixEntry(1, 1, 1.0),
      MatrixEntry(2, 2, 1.0)
    )
    val matrix = SparseMatrix(entries, 3, 3)
    val vector = Array(1.0, 2.0, 3.0)
    
    val result = SpMV.multiply(matrix, vector)
    
    assert(result.sameElements(Array(1.0, 2.0, 3.0)))
  }
  
  test("SpMV with 2x2 matrix") {
    val entries = List(
      MatrixEntry(0, 0, 2.0),
      MatrixEntry(0, 1, 3.0),
      MatrixEntry(1, 0, 4.0),
      MatrixEntry(1, 1, 5.0)
    )
    val matrix = SparseMatrix(entries, 2, 2)
    val vector = Array(1.0, 2.0)
    
    val result = SpMV.multiply(matrix, vector)
    
    assert(result(0) == 8.0)  // 2*1 + 3*2
    assert(result(1) == 14.0) // 4*1 + 5*2
  }
  
  test("SpMV with sparse matrix") {
    val entries = List(
      MatrixEntry(0, 2, 5.0),
      MatrixEntry(2, 1, 3.0),
      MatrixEntry(3, 0, 2.0)
    )
    val matrix = SparseMatrix(entries, 4, 3)
    val vector = Array(1.0, 2.0, 3.0)
    
    val result = SpMV.multiply(matrix, vector)
    
    assert(result(0) == 15.0)
    assert(result(1) == 0.0)
    assert(result(2) == 6.0)
    assert(result(3) == 2.0)
  }
}