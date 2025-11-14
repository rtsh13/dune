# Comprehensive Verification Report

## SpMV

| Format | Optimization | Status | Max Error |
|--------|--------------|--------|-----------|
| COO | CoPartitioning | PASS | 7.11e-15 |
| COO | InPartitionAgg | PASS | 7.11e-15 |
| COO | Balanced | PASS | 1.42e-14 |
| COO | Caching | PASS | 7.11e-15 |
| CSR | Baseline | PASS | 3.55e-15 |
| CSR | CoPartitioning | PASS | 7.11e-15 |
| CSR | InPartitionAgg | PASS | 7.11e-15 |
| CSR | RowWiseOptimized | PASS | 7.11e-15 |
| CSC | Baseline | PASS | 3.55e-15 |
| CSC | CoPartitioning | PASS | 7.11e-15 |
| CSC | InPartitionAgg | PASS | 7.11e-15 |
| CSC | ColumnWiseOptimized | PASS | 7.11e-15 |

## SpMV-Sparse

| Format | Optimization | Status | Max Error |
|--------|--------------|--------|-----------|
| COO-Sparse | CoPartitioning | PASS | 1.78e-15 |
| COO-Sparse | InPartitionAgg | PASS | 1.78e-15 |
| CSR-Sparse | Baseline | PASS | 0.00e+00 |
| CSR-Sparse | CoPartitioning | PASS | 1.78e-15 |
| CSC-Sparse | Baseline | PASS | 0.00e+00 |
| CSC-Sparse | CoPartitioning | PASS | 1.78e-15 |

## SpMM-Sparse

| Format | Optimization | Status | Max Error |
|--------|--------------|--------|-----------|
| COO-SpMM | CoPartitioning | PASS | 8.88e-16 |
| COO-SpMM | InPartitionAgg | PASS | 8.88e-16 |
| COO-SpMM | BlockPartitioned | PASS | 4.44e-16 |
| CSR-SpMM | Baseline | PASS | 4.44e-16 |
| CSR-SpMM | CoPartitioning | PASS | 8.88e-16 |
| CSR-SpMM | InPartitionAgg | PASS | 8.88e-16 |
| CSC-SpMM | Baseline | PASS | 4.44e-16 |
| CSC-SpMM | CoPartitioning | PASS | 8.88e-16 |
| CSC-SpMM | InPartitionAgg | PASS | 8.88e-16 |

## SpMM-Dense

| Format | Optimization | Status | Max Error |
|--------|--------------|--------|-----------|
| COO-SpMM-Dense | CoPartitioning | PASS | 2.66e-15 |
| COO-SpMM-Dense | InPartitionAgg | PASS | 2.66e-15 |
| COO-SpMM-Dense | BlockPartitioned | PASS | 3.55e-15 |
| COO-SpMM-Dense | Caching | PASS | 2.66e-15 |
| CSR-SpMM-Dense | Baseline | PASS | 2.22e-16 |
| CSR-SpMM-Dense | CoPartitioning | PASS | 2.66e-15 |
| CSR-SpMM-Dense | InPartitionAgg | PASS | 2.66e-15 |
| CSR-SpMM-Dense | RowWiseOptimized | PASS | 2.66e-15 |
| CSC-SpMM-Dense | Baseline | PASS | 4.44e-16 |
| CSC-SpMM-Dense | CoPartitioning | PASS | 2.66e-15 |
| CSC-SpMM-Dense | InPartitionAgg | PASS | 2.66e-15 |
| CSC-SpMM-Dense | ColumnWiseOptimized | PASS | 2.66e-15 |

