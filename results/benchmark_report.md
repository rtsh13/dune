# Sparse Matrix Engine Performance Report

## Summary
Average Speedup (vs DataFrame): 2.21x

## Detailed Results

| Operation | Size | Sparsity | Implementation | Time (ms) | Throughput (ops/s) |
|-----------|------|----------|----------------|-----------|-------------------|
| SpMM | 10 | 0.850 | Custom | 44 | 2.26e+04 |
| SpMM | 100 | 0.850 | Custom | 79 | 1.27e+07 |
| SpMV | 10 | 0.850 | Custom | 59 | 1.68e+03 |
| SpMV | 10 | 0.850 | DataFrame | 138 | 7.20e+02 |
| SpMV | 100 | 0.850 | Custom | 52 | 1.92e+05 |
| SpMV | 100 | 0.850 | DataFrame | 150 | 6.65e+04 |
| SpMV | 1000 | 0.850 | Custom | 112 | 8.93e+06 |
| SpMV | 1000 | 0.850 | DataFrame | 159 | 6.27e+06 |

## SpMV Performance Summary

**10x10**: 59ms (Custom) vs 138ms (DataFrame) = 2.34x speedup
**100x100**: 52ms (Custom) vs 150ms (DataFrame) = 2.88x speedup
**1000x1000**: 112ms (Custom) vs 159ms (DataFrame) = 1.42x speedup
