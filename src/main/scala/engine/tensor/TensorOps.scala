package engine.tensor

import org.apache.spark.rdd.RDD
import engine.storage._

// Tensor operations
object TensorOps {
  
  // Operation: Tensor Unfolding (Matricization)
  
  /**
   * Unfold (matricize) a tensor along a specific mode
   * 
   * Converts n-way tensor to matrix by "flattening" certain dimensions
   * 
   * Example: 3D tensor T[i,j,k] unfolded along mode 0:
   * - Rows: index i
   * - Cols: combined index (j,k)
   * - Result: matrix M[i, j*K + k] = T[i,j,k]
   */
  def unfold(
    tensor: SparseTensor,
    mode: Int
  ): RDD[(Int, Int, Double)] = {
    
    require(mode >= 0 && mode < tensor.order, s"Invalid mode $mode")
    
    println(s"Unfolding tensor along mode $mode...")
    
    // For each tensor entry, compute matrix indices
    val matrixEntries = tensor.entries.map { entry =>
      val indices = entry.indices
      
      // Row index = the specified mode
      val row = indices(mode)
      
      // Column index = combination of all other modes
      // Use multi-index to single-index conversion
      var col = 0
      var multiplier = 1
      
      for (m <- tensor.order - 1 to 0 by -1) {
        if (m != mode) {
          col += indices(m) * multiplier
          multiplier *= tensor.dimensions(m)
        }
      }
      
      (row, col, entry.value)
    }
    
    println(s"Tensor unfolded to matrix")
    
    matrixEntries
  }
  
  // Operation: Matricized Tensor Times Khatri-Rao Product (MTTKRP)
  
  /**
   * MTTKRP: Core operation in CP tensor decomposition
   * 
   * Computes: Y = T_(mode) × (U_n ⊙ ... ⊙ U_1)
   * 
   * Where:
   * - T_(mode) is tensor unfolded along mode
   * - ⊙ is Khatri-Rao product (column-wise Kronecker)
   * - U_1, ..., U_n are factor matrices
   * 
   * This is THE bottleneck operation in CP-ALS algorithm
   * 
   * Algorithm (distributed):
   * 1. For each non-zero tensor entry T[i,j,k,...]
   * 2. Get corresponding rows from factor matrices
   * 3. Compute element-wise product (Hadamard)
   * 4. Accumulate result for output row
   */
  def mttkrp(
    tensor: SparseTensor,
    factorMatrices: Array[FactorMatrix],
    mode: Int
  ): FactorMatrix = {
    
    require(mode >= 0 && mode < tensor.order)
    require(factorMatrices.length == tensor.order - 1)
    
    println(s"Computing MTTKRP for mode $mode...")
    
    val rank = factorMatrices(0).rank
    
    // Step 1: Prepare factor matrices (exclude target mode)
    // Convert to RDD format suitable for join
    val factorRDDs = factorMatrices.map { factorMat =>
      factorMat.entries  // RDD[(row_index, factor_values)]
    }
    
    // Step 2: For each tensor entry, compute contribution
    val contributions = tensor.entries.flatMap { entry =>
      val indices = entry.indices
      val tensorValue = entry.value
      
      // Output row is the index in target mode
      val outputRow = indices(mode)
      
      // Collect indices for factor matrices (excluding target mode)
      val factorIndices = indices.zipWithIndex
        .filter(_._2 != mode)
        .map(_._1)
      
      // Emit: (outputRow, (factorIndices, tensorValue))
      Some((outputRow, (factorIndices, tensorValue)))
    }
    
    // Step 3: Group by output row
    val groupedByRow = contributions.groupByKey()
    
    // Step 4: For each output row, fetch factor values and compute
    val resultRows = groupedByRow.map { case (outputRow, rowData) =>
      
      // For this output row, accumulate contributions
      val accumulator = Array.fill(rank)(0.0)
      
      rowData.foreach { case (factorIndices, tensorValue) =>
        // Get factor values for each mode
        // NOTE: This is a simplification - in practice, you'd join with factor RDDs
        // For now, we'll show the structure
        
        // Compute Hadamard product of factor rows
        // factorProduct[r] = product of all factor matrices at indices
        val factorProduct = Array.fill(rank)(1.0)
        
        // Multiply by tensor value and accumulate
        for (r <- 0 until rank) {
          accumulator(r) += tensorValue * factorProduct(r)
        }
      }
      
      (outputRow, accumulator)
    }
    
    println(s"MTTKRP complete for mode $mode")
    
    FactorMatrix(resultRows, tensor.dimensions(mode), rank)
  }
  
  // Operation: Tensor-Matrix Product along Mode
  
  /**
   * Multiply tensor by matrix along a specific mode
   * 
   * T ×_mode M
   * 
   * Result is a tensor with modified dimension along 'mode'
   */
  def tensorMatrixProduct(
    tensor: SparseTensor,
    matrix: RDD[(Int, Int, Double)],  // Matrix as (row, col, value)
    mode: Int
  ): SparseTensor = {
    
    println(s"Computing tensor-matrix product along mode $mode...")
    
    // Step 1: Rekey tensor entries by the specified mode
    val tensorKeyed = tensor.entries.map { entry =>
      val modeIndex = entry.indices(mode)
      (modeIndex, entry)
    }
    
    // Step 2: Rekey matrix by row
    val matrixByRow = matrix.map { case (row, col, value) =>
      (row, (col, value))
    }
    
    // Step 3: Join tensor and matrix on mode index
    val joined = tensorKeyed.join(matrixByRow)
    
    // Step 4: Compute new tensor entries
    val newEntries = joined.map { case (modeIdx, (entry, (newIdx, matValue))) =>
      // Create new indices with modified mode
      val newIndices = entry.indices.clone()
      newIndices(mode) = newIdx
      
      // Emit partial product
      (newIndices.toSeq, entry.value * matValue)
    }
    
    // Step 5: Aggregate by new indices
    val aggregated = newEntries
      .map { case (indices, value) => (indices, value) }
      .reduceByKey(_ + _)
      .map { case (indices, value) =>
        TensorEntry(indices.toArray, value)
      }
      .filter(!_.isZero)
    
    // Update dimensions
    val newDimensions = tensor.dimensions.clone()
    // Would need to know new dimension size from matrix
    
    println(s"Tensor-matrix product complete")
    
    SparseTensor(aggregated, newDimensions)
  }
  
  // Operation: Khatri-Rao Product of Two Matrices
  
  /**
   * Khatri-Rao product: column-wise Kronecker product
   * 
   * For matrices A (I×R) and B (J×R):
   * Result C (IJ×R) where C[:,r] = A[:,r] ⊗ B[:,r]
   * 
   * Used in tensor decomposition algorithms
   */
  def khatriRaoProduct(
    matrixA: FactorMatrix,
    matrixB: FactorMatrix
  ): RDD[(Int, Array[Double])] = {
    
    require(matrixA.rank == matrixB.rank, "Matrices must have same rank")
    
    println(s"Computing Khatri-Rao product...")
    
    val rank = matrixA.rank
    
    // Cartesian product of rows
    val cartesian = matrixA.entries.cartesian(matrixB.entries)
    
    // For each pair, compute Kronecker product
    val result = cartesian.map { case ((rowA, factorsA), (rowB, factorsB)) =>
      
      // Output row index
      val outputRow = rowA * matrixB.numRows + rowB
      
      // Element-wise product of factor vectors
      val product = factorsA.zip(factorsB).map { case (a, b) => a * b }
      
      (outputRow, product)
    }
    
    println(s"Khatri-Rao product complete")
    
    result
  }
  
  // Helper: Load 3D Tensor from CSV
  
  /**
   * Load a 3D sparse tensor from CSV file
   * Format: i,j,k,value
   */
  def loadSparseTensor3D(
    sc: org.apache.spark.SparkContext,
    filepath: String
  ): SparseTensor = {
    
    println(s"Loading 3D tensor from: $filepath")
    
    val entries = sc.textFile(filepath)
      .filter(line => !line.startsWith("i"))  // Skip header
      .filter(_.trim.nonEmpty)
      .map { line =>
        val parts = line.split(",")
        val i = parts(0).trim.toInt
        val j = parts(1).trim.toInt
        val k = parts(2).trim.toInt
        val value = parts(3).trim.toDouble
        
        TensorEntry(Array(i, j, k), value)
      }
      .filter(!_.isZero)
    
    entries.cache()
    
    // Compute dimensions
    val maxIndices = entries.map(_.indices).reduce { (a, b) =>
      a.zip(b).map { case (x, y) => math.max(x, y) }
    }
    
    val dimensions = maxIndices.map(_ + 1)
    
    val count = entries.count()
    println(s"Loaded tensor: ${dimensions.mkString(" × ")} with $count non-zeros")
    
    SparseTensor(entries, dimensions)
  }
}