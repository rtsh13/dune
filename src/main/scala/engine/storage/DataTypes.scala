package engine.storage

case class COOEntry(row: Int, col: Int, value: Double) {
  def isZero: Boolean = math.abs(value) < 1e-10
  override def toString: String = s"($row, $col) = $value"
}

case class SparseVectorEntry(index: Int, value: Double) {
  def isZero: Boolean = math.abs(value) < 1e-10
  override def toString: String = s"[$index] = $value"
}

case class CSRMatrix(
  values: Array[Double],
  colIndices: Array[Int],
  rowPointers: Array[Int],
  numRows: Int,
  numCols: Int
) {

  def getRow(rowIdx: Int): Seq[(Int, Double)] = {
    require(rowIdx >= 0 && rowIdx < numRows, s"Row index $rowIdx out of bounds")
    
    val start = rowPointers(rowIdx)
    val end = rowPointers(rowIdx + 1)
    
    // Return (column, value) pairs for this row
    (start until end).map { i =>
      (colIndices(i), values(i))
    }
  }
  
  //Get value at specific position
  def apply(row: Int, col: Int): Double = {
    val rowData = getRow(row)
    rowData.find(_._1 == col).map(_._2).getOrElse(0.0)
  }

  //Convert back to COO format
  def toCOO: Seq[COOEntry] = {
    val entries = scala.collection.mutable.ArrayBuffer[COOEntry]()
    
    for (row <- 0 until numRows) {
      val rowData = getRow(row)
      rowData.foreach { case (col, value) =>
        entries += COOEntry(row, col, value)
      }
    }
    
    entries.toSeq
  }
  
  override def toString: String = {
    s"CSRMatrix($numRows x $numCols, ${values.length} non-zeros)"
  }
}