package engine.operations

import org.apache.spark.rdd.RDD
import engine.storage._

object MultiplicationOps {

  /** Sparse Matrix × Dense Vector: y = A.x Performs a distributed Sparse Matrix
    * Dense Vector (SpMV) multiplication. This function calculates the result
    * vector y as the product of Matrix A and Vector x.
    *
    * The calculation utilizes Spark's distributed key-value operations to
    * parallelize the summation of partial products.
    *
    * Algorithm:
    * \1. Matrix A: (row, col, value) rekey to (col, (row, value)) 2. Vector x:
    * (index, value). it is already keyed by index 3. Join on column = index
    * (the inner dimension) 4. Compute partial product: matrix_value x
    * vector_value, keying the result by row (the output dimension) 5. Group by
    * row and sum all partial products using reduceByKey
    *
    * @param matrixA
    *   The input matrix A, provided in Coordinate Format (COO) RDD[COOEntry].
    * @param vectorX
    *   The input dense vector x, provided as an RDD[(Int, Double)] keyed by
    *   index.
    * @return
    *   RDD[(Int, Double)] A new RDD representing the resulting vector y, keyed
    *   by row index.
    *
    * Time Complexity: O(nnz(A) + nnz(x)) where nnz = non-zeros Space
    * Complexity: O(nnz(A)) distributed
    */
  def sparseMatrixDenseVector(
      matrixA: RDD[COOEntry],
      vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("Computing Sparse Matrix x Dense Vector...")

    // Step 1: Rekey matrix by column for join
    val matrixByCol = matrixA.map { entry =>
      (entry.col, (entry.row, entry.value))
    }

    // Step 2: Vector is already keyed by index

    // Step 3: Join matrix columns with vector indices
    val joined = matrixByCol.join(vectorX)

    // Step 4: Compute partial products
    val partialProducts = joined.map { case (col, ((row, matVal), vecVal)) =>
      (row, matVal * vecVal)
    }

    // Step 5: Sum partial products by row
    val result = partialProducts.reduceByKey(_ + _)

    println(s"SpMV complete. Result has ${result.count()} non-zero entries.")

    result
  }

  // Operation 2: Sparse Matrix × Sparse Vector (SpMV)

  /** Sparse Matrix × Sparse Vector: y = A × x
    *
    * Similar to dense vector version but both operands are sparse Only non-zero
    * entries participate in join
    */
  def sparseMatrixSparseVector(
      matrixA: RDD[COOEntry],
      vectorX: RDD[SparseVectorEntry]
  ): RDD[(Int, Double)] = {

    println("Computing Sparse Matrix x Sparse Vector...")

    // Rekey matrix by column
    val matrixByCol = matrixA.map { entry =>
      (entry.col, (entry.row, entry.value))
    }

    // Rekey vector by index
    val vectorByIndex = vectorX.map { entry =>
      (entry.index, entry.value)
    }

    // Join - only matching non-zeros participate
    val joined = matrixByCol.join(vectorByIndex)

    // Compute partial products
    val partialProducts = joined.map { case (col, ((row, matVal), vecVal)) =>
      (row, matVal * vecVal)
    }

    // Aggregate by row
    val result = partialProducts.reduceByKey(_ + _)

    println(
      s"Sparse SpMV complete. Result has ${result.count()} non-zero entries."
    )

    result
  }

  // Operation 3: Sparse Matrix × Dense Matrix (SpMM)

  /** Sparse Matrix × Dense Matrix: C = A × B
    *
    * Algorithm: For C[i,k] = Σ(j) A[i,j] × B[j,k]
    *
    *   1. Matrix A: (i, j, vA) → rekey to (j, (i, vA)) 2. Matrix B: (j, k, vB)
    *      \- already keyed by j 3. Join on j (inner dimension) 4. For each
    *      match: emit ((i, k), vA × vB) 5. Sum all partial products for each
    *      (i, k)
    */
  def sparseMatrixDenseMatrix(
      matrixA: RDD[COOEntry],
      matrixB: RDD[(Int, Int, Double)]
  ): RDD[(Int, Int, Double)] = {

    println("Computing Sparse Matrix x Dense Matrix...")

    // Step 1: Rekey A by column (inner dimension)
    val matrixAByCol = matrixA.map { entry =>
      (entry.col, (entry.row, entry.value))
    }

    // Step 2: Rekey B by row (inner dimension)
    val matrixBByRow = matrixB.map { case (row, col, value) =>
      (row, (col, value))
    }

    // Step 3: Join on inner dimension j
    val joined = matrixAByCol.join(matrixBByRow)

    // Step 4: Compute partial products for each output cell
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }

    // Step 5: Sum all contributions to each output cell
    val resultCells = partialProducts.reduceByKey(_ + _)

    // Step 6: Convert back to (row, col, value) format
    val result = resultCells.map { case ((i, k), value) =>
      (i, k, value)
    }

    println(s"SpMM (dense) complete. Result has ${result.count()} entries.")

    result
  }

  // Operation 4: Sparse Matrix × Sparse Matrix (SpMM)

  /** Sparse Matrix × Sparse Matrix: C = A × B
    *
    * Same algorithm as dense case but:
    *   - Only non-zero entries participate
    *   - Result is filtered to remove near-zeros
    *   - Returns COOEntry format to maintain sparsity
    */
  def sparseMatrixSparseMatrix(
      matrixA: RDD[COOEntry],
      matrixB: RDD[COOEntry]
  ): RDD[COOEntry] = {

    println("Computing Sparse Matrix x Sparse Matrix...")

    // Rekey A by column
    val matrixAByCol = matrixA.map { entry =>
      (entry.col, (entry.row, entry.value))
    }

    // Rekey B by row
    val matrixBByRow = matrixB.map { entry =>
      (entry.row, (entry.col, entry.value))
    }

    // Join on inner dimension
    val joined = matrixAByCol.join(matrixBByRow)

    // Compute partial products
    val partialProducts = joined.map { case (j, ((i, vA), (k, vB))) =>
      ((i, k), vA * vB)
    }

    // Aggregate
    val resultCells = partialProducts.reduceByKey(_ + _)

    // Convert to COOEntry and filter zeros to maintain sparsity
    val result = resultCells
      .map { case ((i, k), value) => COOEntry(i, k, value) }
      .filter(!_.isZero) // Remove numerical zeros

    println(
      s"SpMM (sparse) complete. Result has ${result.count()} non-zero entries."
    )

    result
  }

  // Operation 5: Matrix-Vector with CSR Format (Optimized)

  /** Sparse Matrix (CSR) × Dense Vector: y = A × x
    *
    * CSR format is optimized for row-wise operations Each CSRRow computes its
    * dot product with the vector independently
    *
    * Algorithm:
    *   1. For each row in CSR matrix 2. Join row's column indices with vector
    *      3. Compute dot product: Σ(row_val × vec_val)
    *
    * This is more efficient than COO for SpMV because:
    *   - Row data is co-located
    *   - Reduces number of join operations
    */
  def csrMatrixDenseVector(
      csrMatrix: RDD[FormatConverter.CSRRow],
      vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {

    println("Computing CSR Matrix x Dense Vector...")

    // Flatten CSR rows to (col, (row, value)) format
    val matrixFlat = csrMatrix.flatMap { row =>
      row.elementsIterator.map { case (col, value) =>
        (col, (row.rowId, value))
      }
    }

    // Join with vector
    val joined = matrixFlat.join(vectorX)

    // Compute products and aggregate by row
    val result = joined
      .map { case (col, ((row, matVal), vecVal)) =>
        (row, matVal * vecVal)
      }
      .reduceByKey(_ + _)

    println(
      s"CSR SpMV complete. Result has ${result.count()} non-zero entries."
    )

    result
  }

  // HELPER: Display operation statistics

  /** Compute and display statistics about an operation Uses distributed
    * aggregations - NO collect()
    */
  def displayStats(
      operationName: String,
      result: RDD[(Int, Double)]
  ): Unit = {
    val count = result.count()
    val sum = result.map(_._2).reduce(_ + _)
    val maxEntry = result.reduce { (a, b) =>
      if (math.abs(a._2) > math.abs(b._2)) a else b
    }

    println(s"\n=== $operationName Statistics ===")
    println(f"Non-zero entries: $count")
    println(f"Sum of values: $sum%.6f")
    println(f"Max absolute value: ${maxEntry._1} → ${maxEntry._2}%.6f")
  }
}
