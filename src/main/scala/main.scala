import org.apache.spark.{SparkConf, SparkContext}
import engine.storage._
import engine.operations.{MultiplicationOps, FormatSpecificOps}
import engine.optimization.{AdaptiveOps, OptimizedOps}
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
      .set("spark.sql.shuffle.partitions", params.shufflePartitions.toString)
    
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
      if (params.strategy.isDefined) {
        println(s"Strategy: ${params.strategy.get}")
      }
      if (params.format.isDefined) {
        println(s"Format: ${params.format.get}")
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
    
    // Load matrix
    val matrixCOO = COOLoader.loadSparseMatrix(sc, params.inputA)
    val (numRows, numCols) = COOLoader.getMatrixDimensions(matrixCOO)
    
    // Load vector
    val vectorRDD = params.vectorType match {
      case VectorType.Sparse =>
        println("Loading sparse vector...")
        COOLoader.loadSparseVector(sc, params.inputB).map(e => (e.index, e.value))
      case VectorType.Dense =>
        println("Loading dense vector...")
        COOLoader.loadDenseVectorRDD(sc, params.inputB)
    }
    
    val vectorSize = vectorRDD.map(_._1).max() + 1
    
    println(s"\nMatrix dimensions: $numRows x $numCols")
    println(s"Vector size: $vectorSize")
    
    // Verify dimensions
    if (numCols != vectorSize) {
      throw new IllegalArgumentException(
        s"Dimension mismatch: Matrix has $numCols columns " +
        s"but vector has $vectorSize elements"
      )
    }
    
    // Perform multiplication based on strategy and format
    println("\nPerforming multiplication...")
    val start = System.nanoTime()
    
    val resultRDD = (params.format, params.strategy) match {
      // COO Format operations
      case (Some(MatrixFormat.COO), Some(Strategy.Baseline)) =>
        FormatSpecificOps.cooSpMV(matrixCOO, vectorRDD)
      
      case (Some(MatrixFormat.COO), Some(Strategy.Optimized)) =>
        OptimizedOps.optimizedSpMV(matrixCOO, vectorRDD)
      
      case (Some(MatrixFormat.COO), Some(Strategy.Adaptive)) =>
        AdaptiveOps.adaptiveSpMV(matrixCOO, vectorRDD)
      
      case (Some(MatrixFormat.COO), Some(Strategy.Efficient)) =>
        AdaptiveOps.efficientSpMV(matrixCOO, vectorRDD)
      
      case (Some(MatrixFormat.COO), Some(Strategy.Balanced)) =>
        AdaptiveOps.balancedSpMV(matrixCOO, vectorRDD)
      
      case (Some(MatrixFormat.COO), Some(Strategy.MapSideJoin)) =>
        AdaptiveOps.mapSideJoinSpMV(matrixCOO, vectorRDD)
      
      // CSR Format operations
      case (Some(MatrixFormat.CSR), _) =>
        println("Converting to CSR format...")
        val csrMatrix = FormatConverter.cooToDistributedCSR(matrixCOO, numRows, numCols)
        params.strategy match {
          case Some(Strategy.Baseline) =>
            FormatSpecificOps.csrSpMV(csrMatrix, vectorRDD)
          case Some(Strategy.Optimized) =>
            AdaptiveOps.csrSpMV(csrMatrix, vectorRDD)
          case _ =>
            FormatSpecificOps.csrSpMV(csrMatrix, vectorRDD)
        }
      
      // CSC Format operations
      case (Some(MatrixFormat.CSC), _) =>
        println("Converting to CSC format...")
        val cscMatrix = CSCFormat.cooToDistributedCSC(matrixCOO, numRows, numCols)
        FormatSpecificOps.cscSpMV(cscMatrix, vectorRDD)
      
      // Default: Use adaptive strategy
      case _ =>
        MultiplicationOps.sparseMatrixDenseVector(matrixCOO, vectorRDD)
    }
    
    // Force computation
    val resultCount = resultRDD.count()
    
    val elapsed = (System.nanoTime() - start) / 1000000.0
    
    println(f"\nMultiplication complete in ${elapsed}%.2f ms")
    println(s"Result vector has $resultCount non-zero entries")
    
    val result = ResultVector(resultRDD, numRows)
    
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
    val matrixA = COOLoader.loadSparseMatrix(sc, params.inputA)
    val matrixB = COOLoader.loadSparseMatrix(sc, params.inputB)
    
    val (rowsA, colsA) = COOLoader.getMatrixDimensions(matrixA)
    val (rowsB, colsB) = COOLoader.getMatrixDimensions(matrixB)
    
    println(s"\nMatrix A dimensions: $rowsA x $colsA")
    println(s"Matrix B dimensions: $rowsB x $colsB")
    
    // Verify dimensions
    if (colsA != rowsB) {
      throw new IllegalArgumentException(
        s"Dimension mismatch: Matrix A has $colsA columns " +
        s"but Matrix B has $rowsB rows"
      )
    }
    
    // Perform multiplication based on strategy and format
    println("\nPerforming multiplication...")
    val start = System.nanoTime()
    
    val resultCOO = (params.format, params.strategy) match {
      // COO Format operations
      case (Some(MatrixFormat.COO), Some(Strategy.Baseline)) =>
        FormatSpecificOps.cooSpMMSparse(matrixA, matrixB)
      
      case (Some(MatrixFormat.COO), Some(Strategy.Optimized)) =>
        OptimizedOps.optimizedSpMM(matrixA, matrixB)
      
      case (Some(MatrixFormat.COO), Some(Strategy.BlockPartitioned)) =>
        val blockSize = params.blockSize.getOrElse(1000)
        OptimizedOps.blockPartitionedSpMM(matrixA, matrixB, blockSize)
      
      // CSR Format operations
      case (Some(MatrixFormat.CSR), _) =>
        println("Converting to CSR format...")
        val csrA = FormatConverter.cooToDistributedCSR(matrixA, rowsA, colsA)
        val csrB = FormatConverter.cooToDistributedCSR(matrixB, rowsB, colsB)
        FormatSpecificOps.csrSpMMSparse(csrA, csrB)
      
      // CSC Format operations
      case (Some(MatrixFormat.CSC), _) =>
        println("Converting to CSC format...")
        val cscA = CSCFormat.cooToDistributedCSC(matrixA, rowsA, colsA)
        val cscB = CSCFormat.cooToDistributedCSC(matrixB, rowsB, colsB)
        FormatSpecificOps.cscSpMMSparse(cscA, cscB)
      
      // Default
      case _ =>
        MultiplicationOps.sparseMatrixSparseMatrix(matrixA, matrixB)
    }
    
    // Force computation
    val resultCount = resultCOO.count()
    
    val elapsed = (System.nanoTime() - start) / 1000000.0
    
    println(f"\nMultiplication complete in ${elapsed}%.2f ms")
    println(s"Result matrix has $resultCount non-zero entries")
    
    val result = ResultMatrix(
      resultCOO.map(e => (e.row, e.col, e.value)),
      rowsA,
      colsB
    )
    
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
    
    // Load vector
    val vectorRDD = COOLoader.loadDenseVectorRDD(sc, params.inputA)
    val vectorSize = COOLoader.getVectorSize(vectorRDD)
    
    // Load matrix
    val matrix = COOLoader.loadSparseMatrix(sc, params.inputB)
    val (numRows, numCols) = COOLoader.getMatrixDimensions(matrix)
    
    println(s"\nVector size: $vectorSize")
    println(s"Matrix dimensions: $numRows x $numCols")
    
    // Verify dimensions
    if (vectorSize != numRows) {
      throw new IllegalArgumentException(
        s"Dimension mismatch: Vector has $vectorSize elements " +
        s"but matrix has $numRows rows"
      )
    }
    
    // Perform multiplication: v^T x M = (M^T x v)^T
    println("\nPerforming multiplication...")
    val start = System.nanoTime()
    
    // Transpose matrix: swap rows and columns
    val matrixTransposed = matrix.map(e => COOEntry(e.col, e.row, e.value))
    
    // Now perform SpMV with transposed matrix
    val resultRDD = MultiplicationOps.sparseMatrixDenseVector(matrixTransposed, vectorRDD)
    
    // Force computation
    val resultCount = resultRDD.count()
    
    val elapsed = (System.nanoTime() - start) / 1000000.0
    
    println(f"\nMultiplication complete in ${elapsed}%.2f ms")
    println(s"Result vector has $resultCount non-zero entries")
    
    val result = ResultVector(resultRDD, numCols)
    
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
  
  // Strategy options
  sealed trait Strategy
  object Strategy {
    case object Baseline extends Strategy
    case object Optimized extends Strategy
    case object Adaptive extends Strategy
    case object Efficient extends Strategy
    case object Balanced extends Strategy
    case object MapSideJoin extends Strategy
    case object BlockPartitioned extends Strategy
  }
  
  // Matrix format options
  sealed trait MatrixFormat
  object MatrixFormat {
    case object COO extends MatrixFormat
    case object CSR extends MatrixFormat
    case object CSC extends MatrixFormat
  }
  
  // Vector type options
  sealed trait VectorType
  object VectorType {
    case object Dense extends VectorType
    case object Sparse extends VectorType
  }
  
  case class Config(
    inputA: String,
    inputB: String,
    outputPath: Option[String] = None,
    master: String = "local[*]",
    memory: String = "4g",
    logLevel: String = "WARN",
    showPreview: Boolean = true,
    showStats: Boolean = true,
    strategy: Option[Strategy] = None,
    format: Option[MatrixFormat] = None,
    vectorType: VectorType = VectorType.Dense,
    blockSize: Option[Int] = None,
    shufflePartitions: Int = 200
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
    var strategy: Option[Strategy] = None
    var format: Option[MatrixFormat] = None
    var vectorType: VectorType = VectorType.Dense
    var blockSize: Option[Int] = None
    var shufflePartitions = 200
    
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
          
        case "--strategy" =>
          if (i + 1 < args.length) {
            strategy = args(i + 1).toLowerCase match {
              case "baseline" => Some(Strategy.Baseline)
              case "optimized" => Some(Strategy.Optimized)
              case "adaptive" => Some(Strategy.Adaptive)
              case "efficient" => Some(Strategy.Efficient)
              case "balanced" => Some(Strategy.Balanced)
              case "mapsidejoin" => Some(Strategy.MapSideJoin)
              case "blockpartitioned" => Some(Strategy.BlockPartitioned)
              case other =>
                println(s"ERROR: Unknown strategy: $other")
                return None
            }
            i += 2
          } else {
            println("ERROR: Missing value for --strategy")
            return None
          }
          
        case "--format" =>
          if (i + 1 < args.length) {
            format = args(i + 1).toUpperCase match {
              case "COO" => Some(MatrixFormat.COO)
              case "CSR" => Some(MatrixFormat.CSR)
              case "CSC" => Some(MatrixFormat.CSC)
              case other =>
                println(s"ERROR: Unknown format: $other")
                return None
            }
            i += 2
          } else {
            println("ERROR: Missing value for --format")
            return None
          }
          
        case "--vector-type" =>
          if (i + 1 < args.length) {
            vectorType = args(i + 1).toLowerCase match {
              case "dense" => VectorType.Dense
              case "sparse" => VectorType.Sparse
              case other =>
                println(s"ERROR: Unknown vector type: $other")
                return None
            }
            i += 2
          } else {
            println("ERROR: Missing value for --vector-type")
            return None
          }
          
        case "--block-size" =>
          if (i + 1 < args.length) {
            try {
              blockSize = Some(args(i + 1).toInt)
              i += 2
            } catch {
              case _: NumberFormatException =>
                println("ERROR: --block-size must be an integer")
                return None
            }
          } else {
            println("ERROR: Missing value for --block-size")
            return None
          }
          
        case "--shuffle-partitions" =>
          if (i + 1 < args.length) {
            try {
              shufflePartitions = args(i + 1).toInt
              i += 2
            } catch {
              case _: NumberFormatException =>
                println("ERROR: --shuffle-partitions must be an integer")
                return None
            }
          } else {
            println("ERROR: Missing value for --shuffle-partitions")
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
      showStats = showStats,
      strategy = strategy,
      format = format,
      vectorType = vectorType,
      blockSize = blockSize,
      shufflePartitions = shufflePartitions
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
      |  --strategy <name>      Multiplication strategy:
      |                         baseline, optimized, adaptive, efficient, balanced,
      |                         mapsidejoin, blockpartitioned
      |  --format <format>      Matrix format: COO, CSR, CSC (default: COO)
      |  --vector-type <type>   Vector type: dense, sparse (default: dense)
      |  --block-size <size>    Block size for block-partitioned strategy (default: 1000)
      |  --shuffle-partitions <n> Number of shuffle partitions (default: 200)
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
      |Strategies:
      |  baseline               Simple implementation without optimizations
      |  optimized              Standard optimizations (co-partitioning, in-partition agg)
      |  adaptive               Automatically chooses best strategy based on data size
      |  efficient              Efficient SpMV with in-partition aggregation
      |  balanced               Balanced partitioning for better load distribution
      |  mapsidejoin            Map-side join for co-located data
      |  blockpartitioned       Block-based multiplication for large matrices
      |
      |Examples:
      |  # Basic matrix-vector multiplication
      |  spark-submit main.jar matrix.csv vector.csv
      |  
      |  # Matrix-matrix with optimized strategy
      |  spark-submit main.jar --strategy optimized -a matrixA.csv -b matrixB.csv -o result/
      |  
      |  # CSR format with adaptive strategy
      |  spark-submit main.jar --format CSR --strategy adaptive matrix.csv vector.csv
      |  
      |  # Block-partitioned for large matrices
      |  spark-submit main.jar --strategy blockpartitioned --block-size 500 \
      |               largeA.csv largeB.csv -o output/
      |  
      |  # With custom Spark settings
      |  spark-submit main.jar --master spark://host:7077 --memory 8g \
      |               --shuffle-partitions 400 matrix.csv vector.csv
      |
      |Notes:
      |  - All operations use distributed computation (no collect() calls)
      |  - Automatic format detection based on sparsity
      |  - Output is written as distributed files in the specified directory
      |  - Strategies can significantly impact performance on large datasets
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