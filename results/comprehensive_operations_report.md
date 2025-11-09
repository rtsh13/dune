# Comprehensive Multiplication Operations Test

## All 5 Multiplication Operations

| Operation | Time (ms) |
|-----------|-----------|
| SpM × SparseVec | 96.60 |
| CSR × DenseVec | 109.00 |
| SpM × DenseVec | 153.00 |
| SpM × DenseMat | 1593.60 |
| SpM × SparseMat | 5728.20 |

## All Optimization Strategies

| Strategy | Time (ms) |
|----------|-----------|
| Baseline | 109.67 |
| Adaptive Partitioning | 119.67 |
| Partitioning | 136.67 |
| Optimized SpMM | 5040.00 |
| Block-Partitioned | 7876.67 |
