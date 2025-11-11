import org.apache.spark.{SparkConf, SparkContext}
import engine.storage._
import engine.operations.MultiplicationOps
import java.io.File

object Main {
  def main(args: Array[String]): Unit = {
    
    val config = parseArgs(args)
    
    if (config.isEmpty) {
      printUsage()
      System.exit(1)
    }
    
    val params = config.get
    
    val conf = new SparkConf()
      .setAppName("Sparse Matrix Multiplication Engine")
      .setMaster(params.master)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.memory", params.memory)
      .set("spark.executor.memory", params.memory)
    
    val sc = new SparkContext(conf)
    sc.setLogLevel(params.logLevel)
    
    try {
      println("="*80)
      println("SPARSE MATRIX MULTIPLICATION ENGINE")
      println("="*80)
      
      verifyFiles(params.inputA, params.inputB)
      
      val operationType = detectOperationType(sc, params.inputA, params.inputB)
      
      println(s"\nDetected operation: $operationType")
      println(s"Input A: ${params.inputA}")
      println(s"Input B: ${params.inputB}")
      if (params.outputPath.isDefined) {
        println(s"Output: ${params.outputPath.get}")
      }
      println()
      
      // Perform the multiplication
      val result = operationType match {
        case OperationType.MatrixVector =>
          performMatrixVectorMultiplication(sc, params)
          
        case OperationType.MatrixMatrix =>
          performMatrixMatrixMultiplication(sc, params)
          
        case OperationType.VectorMatrix =>
          performVectorMatrixMultiplication(sc, params)
      }
      
      println("\n" + "="*80)
      println("OPERATION COMPLETE")
      println("="*80)
      
      if (params.showPreview) {
        println("\nResult preview (first 10 entries):")
        result match {
          case Left(vectorResult) =>
            vectorResult.preview(10).foreach { case (idx, value) =>
              println(f"  [$idx] = $value%.6f")
            }
          case Right(matrixResult) =>
            matrixResult.preview(10).foreach { case (row, col, value) =>
              println(f"  [$row, $col] = $value%.6f")
            }
        }
      }
      
      // Display statistics
      if (params.showStats) {
        println("\nResult statistics:")
        result match {
          case Left(vectorResult) =>
            displayVectorStats(vectorResult)
          case Right(matrixResult) =>
            displayMatrixStats(matrixResult)
        }
      }
      
    } catch {
      case e: Exception =>
        println(s"\nERROR: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      sc.stop()
    }
  }
  
  sealed trait OperationType
  object OperationType {
    case object MatrixVector extends OperationType
    case object MatrixMatrix extends OperationType
    case object VectorMatrix extends OperationType
  }
  
  def detectOperationType(
    sc: SparkContext,
    fileA: String,
    fileB: String
  ): OperationType = {
    
    println("Analyzing input files to detect operation type...")
    
    // Quick peek at structure by reading first few lines
    val structureA = analyzeFileStructure(sc, fileA)
    val structureB = analyzeFileStructure(sc, fileB)
    
    println(s"  File A: $structureA")
    println(s"  File B: $structureB")
    
    (structureA, structureB) match {
      case (FileStructure.Matrix, FileStructure.Vector) =>
        OperationType.MatrixVector
        
      case (FileStructure.Matrix, FileStructure.Matrix) =>
        OperationType.MatrixMatrix
        
      case (FileStructure.Vector, FileStructure.Matrix) =>
        OperationType.VectorMatrix
        
      case (FileStructure.Vector, FileStructure.Vector) =>
        throw new IllegalArgumentException(
          "Vector-Vector multiplication not supported. " +
          "Use dot product or outer product explicitly."
        )
        
      case _ =>
        throw new IllegalArgumentException(
          s"Cannot determine operation type from files: $fileA, $fileB"
        )
    }
  }
  
  sealed trait FileStructure
  object FileStructure {
    case object Matrix extends FileStructure {
      override def toString: String = "Matrix (row,col,value)"
    }
    case object Vector extends FileStructure {
      override def toString: String = "Vector (index,value)"
    }
  }
  
  def analyzeFileStructure(sc: SparkContext, filepath: String): FileStructure = {
    
    val sample = sc.textFile(filepath)
      .filter(line => !line.trim.isEmpty)
      .filter(line => !line.toLowerCase.startsWith("row"))
      .filter(line => !line.toLowerCase.startsWith("index"))
      .filter(line => !line.toLowerCase.startsWith("i,"))
      .take(5)
    
    if (sample.isEmpty) {
      throw new IllegalArgumentException(s"File $filepath is empty or contains only headers")
    }
    
    val numFields = sample.head.split(",").length
    
    numFields match {
      case 2 => FileStructure.Vector
      case 3 => FileStructure.Matrix
      case _ => throw new IllegalArgumentException(
        s"Unexpected file format in $filepath. " +
        s"Expected 2 fields (vector) or 3 fields (matrix), got $numFields"
      )
    }
  }
  
  type MultiplicationResult = Either[ResultVector, ResultMatrix]
  
  def performMatrixVectorMultiplication(
    sc: SparkContext,
    params: Config
  ): MultiplicationResult = {
    
    println("\n--- MATRIX x VECTOR MULTIPLICATION ---\n")
    
    // Load matrix (smart format detection)
    val matrix = SmartLoader.loadMatrix(sc, params.inputA)
    
    // Load vector (smart format detection)
    val vector = SmartLoader.loadVector(sc, params.inputB)
    
    println(s"\nMatrix dimensions: ${matrix.numRows} x ${matrix.numCols}")
    println(s"Vector size: ${vector.size}")
    
    // Verify dimensions
    if (matrix.numCols != vector.size) {
      throw new IllegalArgumentException(
        s"Dimension mismatch: Matrix has ${matrix.numCols} columns " +
        s"but vector has ${vector.size} elements"
      )
    }
    
    // Perform multiplication
    println("\nPerforming multiplication...")
    val start = System.nanoTime()
    
    val result = matrix * vector
    
    // Force computation
    val resultCount = result.entries.count()
    
    val elapsed = (System.nanoTime() - start) / 1000000.0
    
    println(f"\nMultiplication complete in ${elapsed}%.2f ms")
    println(s"Result vector has $resultCount non-zero entries")
    
    // Save if output path specified
    if (params.outputPath.isDefined) {
      result.saveAsTextFile(params.outputPath.get)
      println(s"\nResult saved to: ${params.outputPath.get}")
    }
    
    Left(result)
  }
  
  def performMatrixMatrixMultiplication(
    sc: SparkContext,
    params: Config
  ): MultiplicationResult = {
    
    println("\n--- MATRIX x MATRIX MULTIPLICATION ---\n")
    
    // Load both matrices
    val matrixA = SmartLoader.loadMatrix(sc, params.inputA)
    val matrixB = SmartLoader.loadMatrix(sc, params.inputB)
    
    println(s"\nMatrix A dimensions: ${matrixA.numRows} x ${matrixA.numCols}")
    println(s"Matrix B dimensions: ${matrixB.numRows} x ${matrixB.numCols}")
    
    // Verify dimensions
    if (matrixA.numCols != matrixB.numRows) {
      throw new IllegalArgumentException(
        s"Dimension mismatch: Matrix A has ${matrixA.numCols} columns " +
        s"but Matrix B has ${matrixB.numRows} rows"
      )
    }
    
    // Perform multiplication
    println("\nPerforming multiplication...")
    val start = System.nanoTime()
    
    val result = matrixA * matrixB
    
    // Force computation
    val resultCount = result.entries.count()
    
    val elapsed = (System.nanoTime() - start) / 1000000.0
    
    println(f"\nMultiplication complete in ${elapsed}%.2f ms")
    println(s"Result matrix has $resultCount entries")
    
    // Save if output path specified
    if (params.outputPath.isDefined) {
      result.saveAsTextFile(params.outputPath.get)
      println(s"\nResult saved to: ${params.outputPath.get}")
    }
    
    Right(result)
  }
  
  def performVectorMatrixMultiplication(
    sc: SparkContext,
    params: Config
  ): MultiplicationResult = {
    
    println("\n--- VECTOR x MATRIX MULTIPLICATION ---\n")
    println("Note: Computing as (M^T x v)^T where M = inputB, v = inputA\n")
    
    // Load vector and matrix
    val vector = SmartLoader.loadVector(sc, params.inputA)
    val matrix = SmartLoader.loadMatrix(sc, params.inputB)
    
    println(s"\nVector size: ${vector.size}")
    println(s"Matrix dimensions: ${matrix.numRows} x ${matrix.numCols}")
    
    // Verify dimensions
    if (vector.size != matrix.numRows) {
      throw new IllegalArgumentException(
        s"Dimension mismatch: Vector has ${vector.size} elements " +
        s"but matrix has ${matrix.numRows} rows"
      )
    }
    
    // Perform multiplication: v^T x M = (M^T x v)^T
    println("\nPerforming multiplication...")
    val start = System.nanoTime()
    
    val matrixTransposed = matrix.transpose
    val result = matrixTransposed * vector
    
    // Force computation
    val resultCount = result.entries.count()
    
    val elapsed = (System.nanoTime() - start) / 1000000.0
    
    println(f"\nMultiplication complete in ${elapsed}%.2f ms")
    println(s"Result vector has $resultCount non-zero entries")
    
    // Save if output path specified
    if (params.outputPath.isDefined) {
      result.saveAsTextFile(params.outputPath.get)
      println(s"\nResult saved to: ${params.outputPath.get}")
    }
    
    Left(result)
  }
  
  def displayVectorStats(result: ResultVector): Unit = {
    val entries = result.entries
    
    val count = entries.count()
    val sum = entries.map(_._2).reduce(_ + _)
    
    val maxEntry = entries.reduce { (a, b) =>
      if (math.abs(a._2) > math.abs(b._2)) a else b
    }
    
    val minNonZero = entries
      .filter(e => math.abs(e._2) > 1e-10)
      .reduce { (a, b) =>
        if (math.abs(a._2) < math.abs(b._2)) a else b
      }
    
    println(f"Non-zero entries: $count")
    println(f"Sum of values: $sum%.6f")
    println(f"Max absolute value: [${maxEntry._1}] = ${maxEntry._2}%.6f")
    println(f"Min absolute value (non-zero): [${minNonZero._1}] = ${minNonZero._2}%.6f")
  }
  
  def displayMatrixStats(result: ResultMatrix): Unit = {
    val entries = result.entries
    
    val count = entries.count()
    val sum = entries.map(_._3).reduce(_ + _)
    
    val maxEntry = entries.reduce { (a, b) =>
      if (math.abs(a._3) > math.abs(b._3)) a else b
    }
    
    val minNonZero = entries
      .filter(e => math.abs(e._3) > 1e-10)
      .reduce { (a, b) =>
        if (math.abs(a._3) < math.abs(b._3)) a else b
      }
    
    println(f"Total entries: $count")
    println(f"Sum of values: $sum%.6f")
    println(f"Max absolute value: [${maxEntry._1}, ${maxEntry._2}] = ${maxEntry._3}%.6f")
    println(f"Min absolute value (non-zero): [${minNonZero._1}, ${minNonZero._2}] = ${minNonZero._3}%.6f")
  }
  
  case class Config(
    inputA: String,
    inputB: String,
    outputPath: Option[String] = None,
    master: String = "local[*]",
    memory: String = "4g",
    logLevel: String = "WARN",
    showPreview: Boolean = true,
    showStats: Boolean = true
  )
  
  def parseArgs(args: Array[String]): Option[Config] = {
    
    if (args.length < 2) {
      return None
    }
    
    var inputA: Option[String] = None
    var inputB: Option[String] = None
    var outputPath: Option[String] = None
    var master = "local[*]"
    var memory = "4g"
    var logLevel = "WARN"
    var showPreview = true
    var showStats = true
    
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "-a" | "--input-a" =>
          if (i + 1 < args.length) {
            inputA = Some(args(i + 1))
            i += 2
          } else {
            println("ERROR: Missing value for -a/--input-a")
            return None
          }
          
        case "-b" | "--input-b" =>
          if (i + 1 < args.length) {
            inputB = Some(args(i + 1))
            i += 2
          } else {
            println("ERROR: Missing value for -b/--input-b")
            return None
          }
          
        case "-o" | "--output" =>
          if (i + 1 < args.length) {
            outputPath = Some(args(i + 1))
            i += 2
          } else {
            println("ERROR: Missing value for -o/--output")
            return None
          }
          
        case "--master" =>
          if (i + 1 < args.length) {
            master = args(i + 1)
            i += 2
          } else {
            println("ERROR: Missing value for --master")
            return None
          }
          
        case "--memory" =>
          if (i + 1 < args.length) {
            memory = args(i + 1)
            i += 2
          } else {
            println("ERROR: Missing value for --memory")
            return None
          }
          
        case "--log-level" =>
          if (i + 1 < args.length) {
            logLevel = args(i + 1).toUpperCase
            i += 2
          } else {
            println("ERROR: Missing value for --log-level")
            return None
          }
          
        case "--no-preview" =>
          showPreview = false
          i += 1
          
        case "--no-stats" =>
          showStats = false
          i += 1
          
        case "-h" | "--help" =>
          return None
          
        case other =>
          // Positional arguments
          if (inputA.isEmpty) {
            inputA = Some(other)
          } else if (inputB.isEmpty) {
            inputB = Some(other)
          } else {
            println(s"ERROR: Unexpected argument: $other")
            return None
          }
          i += 1
      }
    }
    
    if (inputA.isEmpty || inputB.isEmpty) {
      println("ERROR: Both input files are required")
      return None
    }
    
    Some(Config(
      inputA = inputA.get,
      inputB = inputB.get,
      outputPath = outputPath,
      master = master,
      memory = memory,
      logLevel = logLevel,
      showPreview = showPreview,
      showStats = showStats
    ))
  }
  
  def printUsage(): Unit = {
    println("""
      |Usage: spark-submit [spark-options] main.jar [options] <input-a> <input-b>
      |
      |Performs matrix/vector multiplication operations on CSV files.
      |Automatically detects operation type based on file structure.
      |
      |Positional Arguments:
      |  <input-a>              First input file (matrix or vector CSV)
      |  <input-b>              Second input file (matrix or vector CSV)
      |
      |Options:
      |  -a, --input-a <file>   First input file (alternative to positional)
      |  -b, --input-b <file>   Second input file (alternative to positional)
      |  -o, --output <path>    Output path for result (optional)
      |  --master <url>         Spark master URL (default: local[*])
      |  --memory <size>        Memory per executor (default: 4g)
      |  --log-level <level>    Spark log level: ERROR, WARN, INFO, DEBUG (default: WARN)
      |  --no-preview           Disable result preview
      |  --no-stats             Disable result statistics
      |  -h, --help             Show this help message
      |
      |Supported Operations:
      |  Matrix x Vector        Detected when: 3-column CSV x 2-column CSV
      |  Matrix x Matrix        Detected when: 3-column CSV x 3-column CSV
      |  Vector x Matrix        Detected when: 2-column CSV x 3-column CSV
      |
      |File Formats:
      |  Matrix CSV:  row,col,value
      |  Vector CSV:  index,value
      |
      |Examples:
      |  # Matrix-vector multiplication
      |  spark-submit main.jar matrix.csv vector.csv
      |  
      |  # Matrix-matrix multiplication with output
      |  spark-submit main.jar -a matrixA.csv -b matrixB.csv -o result/
      |  
      |  # With custom Spark settings
      |  spark-submit main.jar --master spark://host:7077 --memory 8g matrix.csv vector.csv
      |  
      |  # Using synthetic data
      |  spark-submit main.jar synthetic-data/sparse_matrix_1000x1000.csv \
      |                        synthetic-data/dense_vector_1000.csv -o output/result
      |
      |Notes:
      |  - All operations use distributed computation (no collect() calls)
      |  - SmartLoader automatically detects sparse vs dense representation
      |  - Output is written as distributed files in the specified directory
      |""".stripMargin)
  }
  
  def verifyFiles(fileA: String, fileB: String): Unit = {
    
    val files = Seq(fileA, fileB)
    val missing = files.filterNot(f => new File(f).exists())
    
    if (missing.nonEmpty) {
      println("\nERROR: The following input files do not exist:")
      missing.foreach(f => println(s"  - $f"))
      println()
      throw new IllegalArgumentException("Input files not found")
    }
  }
}