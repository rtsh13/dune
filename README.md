# Distributed Sparse Matrix Engine

A high-performance, distributed sparse matrix multiplication engine built on Apache Spark and Scala. This system implements multiple sparse matrix formats (COO, CSR, CSC) with optimized distributed algorithms for efficient large-scale matrix operations.

## Overview

This project provides a fully distributed sparse matrix computation framework with zero driver bottlenecks. All operations are performed in a distributed manner across the Spark cluster, with no `collect()` or `collectAsMap()` calls that would bring data to the driver node.

### Key Features

- **Multiple Sparse Formats**: Support for COO (Coordinate), CSR (Compressed Sparse Row), and CSC (Compressed Sparse Column) formats
- **Smart Format Detection**: Automatically chooses optimal representation based on sparsity
- **Distributed Optimizations**: 
  - Co-partitioning strategies
  - In-partition aggregation
  - Format-specific optimizations
  - Block-partitioned computation
  - Caching strategies
- **Tensor Operations**: Support for 3D tensor operations including MTTKRP
- **Zero Driver Bottlenecks**: Fully distributed computation with no collect operations
- **Comprehensive Benchmarking**: End-to-end evaluation suite with detailed performance analysis

## Architecture
![flow](/docs/Flow.png)

## Performance

Benchmark results show **2-10x speedup** compared to Apache Spark's MLlib and DataFrame implementations for sparse matrix operations:

- **SpMV (Sparse Matrix-Vector)**: 2.5x average speedup
- **SpMM (Sparse Matrix-Matrix)**: 3.2x average speedup
- **Iterative Algorithms**: 2.8x speedup for PageRank
- **Parallel Efficiency**: ~85% efficiency up to 8 cores

## Quick Start

### Prerequisites

- Scala 2.12.x
- Apache Spark 3.3.0+
- SBT 1.x
- Java 11+
- Python 3.x (for plotting, optional)

### Installation
```bash
# Clone the repository
git clone <repository-url>
cd sparse-matrix-engine

# Generate synthetic test data
sbt "runMain utils.MatrixGenerator"

# Compile the project
sbt compile
```

### Basic Usage
```bash
# Simple matrix-vector multiplication
sbt "runMain Main \
  synthetic-data/sparse_matrix_1000x1000.csv \
  synthetic-data/dense_vector_1000.csv \
  -o output/"

# Matrix-matrix multiplication with custom settings
sbt "runMain Main \
  -a matrixA.csv \
  -b matrixB.csv \
  -o results/ \
  --memory 8g \
  --log-level WARN"
```

### Interactive Script
```bash
# Run with defaults
./userSimulate.sh

# Run with specific optimizations
./userSimulate.sh \
  --strategy efficient \
  --format CSR \
  --memory 8g

# See all options
./userSimulate.sh --help
```

## Project Structure
```
sparse-matrix-engine/
├── src/main/scala/
│   ├── benchmarks/           # Comprehensive benchmark suite
│   │   ├── BenchmarkController.scala
│   │   ├── MicrobenchmarkSuite.scala
│   │   ├── OptimizationImpactSuite.scala
│   │   ├── AblationStudySuite.scala
│   │   ├── EndToEndSuite.scala
│   │   └── ComprehensiveVerification.scala
│   │   └── ComprehensiveVerification.scala
│   │   └── MLlibTest.scala
│   │   └── RealWorldSuite.scala
│   │   └── MLlibComparison.scala
│   ├── engine/
│   │   ├── operations/       # Matrix operation implementations
│   │   │   ├── COOOperations.scala
│   │   │   ├── CSROperations.scala
│   │   │   └── CSCOperations.scala
│   │   │   └── MatrixOperations.scala
│   │   │   └── MultiplicationOps.scala
│   │   │   └── CSCOperations.scala
│   │   ├── optimization/     # Optimization strategies
│   │   │   └── OptimizationStrategies.scala
│   │   │   └── DataStatistics.scala
│   │   ├── storage/          # Data structures and loaders
│   │   │   ├── COOLoader.scala
│   │   │   └── CSCFormat.scala
│   │   │   ├── DataTypes.scala
│   │   │   ├── FormatConverter.scala
│   │   │   ├── JoinHelpers.scala
│   │   │   ├── MatrixTypes.scala
│   │   │   ├── SmartLoader.scala
│   │   └── tensor/           # Tensor operations
│   │       ├── TensorOps.scala
│   │       └── TensorTypes.scala
│   ├── examples/             # Example programs
│   ├── utils/                # Utilities
│   │   └── MatrixGenerator.scala
│   └── main.scala            # Main entry point
├── synthetic-data/           # Generated test datasets
├── results/                  # Benchmark results and reports
├── readWorldStats.sh         # End-to-end evaluation script
└── userSimulate.sh           # Interactive execution script
```

## Supported Operations

### Matrix Operations
- **SpMV**: Sparse Matrix × Dense Vector
- **SpMV-Sparse**: Sparse Matrix × Sparse Vector
- **SpMM-Sparse**: Sparse Matrix × Sparse Matrix
- **SpMM-Dense**: Sparse Matrix × Dense Matrix
- Matrix transpose, addition

### Tensor Operations
- **MTTKRP**: Matricized Tensor Times Khatri-Rao Product
- Tensor unfolding (matricization)
- Tensor-matrix product

### Optimization Strategies
- Baseline (simple join)
- Co-partitioning (hash partitioning both RDDs)
- In-partition aggregation (local reduce)
- Balanced partitioning (load-balanced)
- Caching (persist intermediate results)
- Format-specific optimizations (row-wise/column-wise)
- Block-partitioned (tile-based computation)

## File Formats

### Matrix CSV Format
```csv
row,col,value
0,0,1.5
0,2,3.2
1,1,2.7
```

### Vector CSV Format
```csv
index,value
0,1.5
1,3.2
2,2.7
```

### Tensor CSV Format
```csv
i,j,k,value
0,0,0,1.5
0,1,0,2.3
1,0,1,3.1
```

## Running Benchmarks

### Complete Benchmark Suite
```bash
# Run comprehensive benchmarks
sbt "runMain benchmarks.BenchmarkRunner"

# View results
cat results/COMPREHENSIVE_BENCHMARK_REPORT.md
```

### End-to-End Evaluation
```bash
# Run full system evaluation
./readWorldStats.sh

# Skip plot generation
./readWorldStats.sh SKIP_PLOTS

# View results
cat results/e2e/results/end_to_end_evaluation.md
```

### Verification Tests
```bash
# Run correctness verification
sbt "runMain benchmarks.VerificationRunner"

# View verification report
cat results/verification_report.md
```

## Benchmark Results

The comprehensive benchmark suite evaluates:

1. **Microbenchmarks**: Format comparison (COO vs CSR vs CSC)
2. **DataFrame Comparison**: Custom engine vs Spark SQL
3. **Optimization Impact**: Effect of each optimization strategy
4. **Ablation Studies**: Individual and cumulative optimization effects
5. **End-to-End Tests**: Real-world application scenarios
6. **Scalability Analysis**: Performance with varying parallelism
7. **RealWorld Analysis**: Performance with real world algorithms


## Examples

### Example 1: Basic Loading
```bash
sbt "runMain examples.Example01_Loading"
```

### Example 2: CSR Conversion
```bash
sbt "runMain examples.Example02_CSRConversion"
```

### Example 3: Smart Type Detection
```bash
sbt "runMain examples.Example03_TypeDetection"
```

### Example 5: Tensor Operations
```bash
sbt "runMain examples.Example05_TensorOperations"
```

## 🔬 Advanced Usage

### Using Different Formats
```scala
import engine.storage._
import engine.operations._

// Load and convert to CSR
val cooMatrix = SmartLoader.loadMatrix(sc, "matrix.csv")
val csrMatrix = cooMatrix.toCSR

// Use format-specific operations
val result = CSROperations.spMV(csrMatrix.rows, vector)
```

### Custom Optimization Strategies
```scala
import engine.optimization.OptimizationStrategies

// Use co-partitioning optimization
val result = OptimizationStrategies.cooSpMV_CoPartitioning(
  matrix.entries, 
  vector
)

// Use in-partition aggregation
val result = OptimizationStrategies.csrSpMV_InPartitionAgg(
  csrMatrix.rows,
  vector
)
```

### Tensor Operations
```scala
import engine.tensor._

// Load 3D tensor
val tensor = TensorOps.loadSparseTensor3D(sc, "tensor.csv")

// Unfold tensor along mode
val matrix = TensorOps.unfold(tensor, mode = 0)

// MTTKRP operation
val result = TensorOps.mttkrp(tensor, factorMatrices, mode = 0)
```

## Performance Tuning

### Memory Configuration
```bash
# Increase memory allocation
sbt "runMain Main matrix.csv vector.csv --memory 16g"
```

### Parallelism Settings
```scala
val conf = new SparkConf()
  .set("spark.default.parallelism", "16")
  .set("spark.sql.shuffle.partitions", "16")
```

### Caching Strategies
```scala
// Cache matrix for iterative algorithms
matrix.entries.cache()
matrix.entries.count() // Trigger caching
```

## Troubleshooting

### Out of Memory Errors
- Increase `--memory` parameter
- Increase number of partitions
- Use more compact formats (CSR/CSC instead of COO)

### Slow Performance
- Check data skew with `.glom().map(_.size).collect()`
- Increase parallelism for large datasets
- Use appropriate format for operation type

### File Not Found
- Verify paths are correct
- Generate test data: `sbt "runMain utils.MatrixGenerator"`
- Check file permissions

## Documentation

- **Architecture**: See `docs/ARCHITECTURE.md` (if available)
- **API Reference**: ScalaDoc generated with `sbt doc`
- **Benchmark Report**: `results/COMPREHENSIVE_BENCHMARK_REPORT.md`
- **Verification Report**: `results/verification_report.md`