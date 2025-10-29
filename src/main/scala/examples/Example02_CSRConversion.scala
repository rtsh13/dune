package examples

import org.apache.spark.{SparkConf, SparkContext}
import engine.storage.{COOLoader, FormatConverter}

object Example02_CSRConversion {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
      .setAppName("CSR Conversion - ZERO COLLECT")
      .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    println("Example 2: Distributed CSR - ZERO COLLECT()")
    
    val cooMatrix = COOLoader.loadSparseMatrix(
      sc,
      "data/test/identity_10x10.csv"
    )
    
    val (numRows, numCols) = COOLoader.getMatrixDimensions(cooMatrix)
    
    println("\nCOO Format (first 10 entries for display):")
    cooMatrix.take(10).foreach(println)
    
    println("\nConverting to Distributed CSR...")
    val csrRows = FormatConverter.cooToDistributedCSR(
      cooMatrix,
      numRows,
      numCols
    )
    
    println(s"\nDistributed CSR Format:")
    println(s"Total rows (distributed count): ${csrRows.count()}")
    
    println("\nFirst 5 CSR rows (using take for display):")
    csrRows.take(5).foreach { row =>
      println(s"  Row ${row.rowId}: ${row.nnz} non-zeros")
      if (row.nnz > 0) {
        println(s"    Columns: ${row.colIndices.mkString(", ")}")
        println(s"    Values:  ${row.values.mkString(", ")}")
      }
    }
    
    val totalNonZeros = csrRows.map(_.nnz.toLong).reduce(_ + _)
    println(s"\nTotal non-zeros (distributed reduce): $totalNonZeros")
    
    println("\nConverting back to COO (distributed)...")
    val cooAgain = FormatConverter.distributedCSRToCOO(csrRows)
    
    println(s"Recovered COO entries: ${cooAgain.count()}")
    
    println("\n--- Saving CSR to file (distributed write) ---")
    csrRows
      .flatMap { row =>
        row.getElements.map { case (col, value) =>
          s"${row.rowId},$col,$value"
        }
      }
      .saveAsTextFile("output/example02_csr")
    
    println("CSR saved to: output/example02_csr/")
    println("View with: cat output/example02_csr/part-*")
    
    sc.stop()
  }
}