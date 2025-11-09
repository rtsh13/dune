package benchmarks

import java.io.File
import scala.util.matching.Regex

object DatasetDiscovery {
  
  case class DatasetInfo(
    size: Int,
    matrixFile: String,
    vectorFile: String,
    fileSizeMB: Double,
    category: String
  )
  
  private val matrixPattern: Regex = """sparse_matrix_(\d+)x(\d+).*\.csv""".r
  private val vectorPattern: Regex = """dense_vector_(\d+).*\.csv""".r
  
  def discoverAllDatasets(dataDir: String = "synthetic-data"): Seq[DatasetInfo] = {
    println("\n" + "="*80)
    println("DISCOVERING DATASETS IN " + dataDir)
    println("="*80)
    
    val dir = new File(dataDir)
    if (!dir.exists() || !dir.isDirectory) {
      println(s"ERROR: Directory $dataDir does not exist")
      return Seq.empty
    }
    
    val allFiles = dir.listFiles().filter(_.getName.endsWith(".csv"))
    
    val matrixFiles = allFiles.collect {
      case f if matrixPattern.findFirstIn(f.getName).isDefined =>
        val matrixPattern(rows, cols) = f.getName
        (rows.toInt, f.getName, f.length().toDouble / (1024 * 1024))
    }.toSeq
    
    val vectorFiles = allFiles.collect {
      case f if vectorPattern.findFirstIn(f.getName).isDefined =>
        val vectorPattern(size) = f.getName
        (size.toInt, f.getName)
    }.toMap
    
    val datasets = matrixFiles.flatMap { case (size, matrixFile, sizeMB) =>
      vectorFiles.get(size).map { vectorFile =>
        val category = if (size < 1000) "Small"
                      else if (size < 10000) "Medium"
                      else if (size < 20000) "Large"
                      else "Extra-Large"
        
        DatasetInfo(size, matrixFile, vectorFile, sizeMB, category)
      }
    }.sortBy(_.size)
    
    if (datasets.isEmpty) {
      println("WARNING: No matching matrix-vector pairs found")
      println("Expected format: sparse_matrix_NxN_*.csv and dense_vector_N_*.csv")
    } else {
      println(s"\nFound ${datasets.size} complete datasets:")
      println()
      println("| Category     | Size      | Matrix File                              | Vector File                | Size (MB) |")
      println("|--------------|-----------|------------------------------------------|----------------------------|-----------|")
      
      datasets.foreach { ds =>
        println(f"| ${ds.category}%-12s | ${ds.size}x${ds.size}%-6s | ${ds.matrixFile}%-40s | ${ds.vectorFile}%-26s | ${ds.fileSizeMB}%8.1f |")
      }
    }
    
    datasets
  }
  
  def getDatasetsByCategory(dataDir: String = "synthetic-data"): Map[String, Seq[DatasetInfo]] = {
    val datasets = discoverAllDatasets(dataDir)
    datasets.groupBy(_.category)
  }
  
  def getSmallDatasets(dataDir: String = "synthetic-data"): Seq[DatasetInfo] = {
    discoverAllDatasets(dataDir).filter(_.category == "Small")
  }
  
  def getMediumDatasets(dataDir: String = "synthetic-data"): Seq[DatasetInfo] = {
    discoverAllDatasets(dataDir).filter(_.category == "Medium")
  }
  
  def getLargeDatasets(dataDir: String = "synthetic-data"): Seq[DatasetInfo] = {
    discoverAllDatasets(dataDir).filter(_.category == "Large")
  }
  
  def getExtraLargeDatasets(dataDir: String = "synthetic-data"): Seq[DatasetInfo] = {
    discoverAllDatasets(dataDir).filter(_.category == "Extra-Large")
  }
  
  def main(args: Array[String]): Unit = {
    val datasets = discoverAllDatasets("synthetic-data")
    
    println("\n" + "="*80)
    println("SUMMARY BY CATEGORY")
    println("="*80)
    
    val byCategory = datasets.groupBy(_.category)
    Seq("Small", "Medium", "Large", "Extra-Large").foreach { cat =>
      val count = byCategory.get(cat).map(_.size).getOrElse(0)
      val totalSize = byCategory.get(cat).map(_.map(_.fileSizeMB).sum).getOrElse(0.0)
      println(f"$cat%-12s: $count%2d datasets, ${totalSize}%8.1f MB total")
    }
    
    println()
    println("READY FOR BENCHMARKING: " + (if (datasets.nonEmpty) "YES" else "NO"))
  }
}