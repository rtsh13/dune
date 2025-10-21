package sparsematrix

case class MatrixEntry(row: Int, col: Int, value: Double)

case class SparseMatrix(
  entries: List[MatrixEntry],
  numRows: Int,
  numCols: Int
) {
  def toCOO: List[(Int, Int, Double)] = 
    entries.map(e => (e.row, e.col, e.value))
}