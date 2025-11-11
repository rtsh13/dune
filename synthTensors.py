#!/usr/bin/env python3
"""
Generate synthetic tensor and dense matrix data for benchmarking
Saves data in CSV format compatible with Spark loading
"""

import os
import random
import csv
from datetime import datetime

def generate_sparse_tensor(dim1, dim2, dim3, sparsity, output_path):
    """
    Generate a sparse 3D tensor in COO format
    
    Args:
        dim1, dim2, dim3: Dimensions of the tensor
        sparsity: Fraction of zeros (0.95 = 95% sparse)
        output_path: Where to save the CSV file
    """
    print(f"\nGenerating sparse tensor {dim1}x{dim2}x{dim3} with sparsity {sparsity}")
    
    total_elements = dim1 * dim2 * dim3
    nnz = int(total_elements * (1.0 - sparsity))
    
    print(f"  Total possible elements: {total_elements:,}")
    print(f"  Non-zero elements: {nnz:,}")
    
    random.seed(42)  # For reproducibility
    
    # Use a set to avoid duplicates
    entries = set()
    
    with open(output_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['i', 'j', 'k', 'value'])
        
        generated = 0
        attempts = 0
        max_attempts = nnz * 10  # Prevent infinite loops
        
        while generated < nnz and attempts < max_attempts:
            i = random.randint(0, dim1 - 1)
            j = random.randint(0, dim2 - 1)
            k = random.randint(0, dim3 - 1)
            
            key = (i, j, k)
            if key not in entries:
                entries.add(key)
                value = random.uniform(0.1, 10.0)
                writer.writerow([i, j, k, f"{value:.6f}"])
                generated += 1
                
                if generated % 10000 == 0:
                    print(f"    Progress: {generated:,} / {nnz:,} ({generated*100/nnz:.1f}%)")
            
            attempts += 1
        
        if attempts >= max_attempts:
            print(f"  WARNING: Reached max attempts. Generated {generated} of {nnz} entries")
    
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"  Saved to: {output_path}")
    print(f"  File size: {file_size_mb:.2f} MB")
    print(f"  Actual sparsity: {1.0 - (generated / total_elements):.4f}")


def generate_dense_matrix(rows, cols, output_path):
    """
    Generate a dense matrix (all entries populated)
    
    Args:
        rows: Number of rows
        cols: Number of columns
        output_path: Where to save the CSV file
    """
    print(f"\nGenerating dense matrix {rows}x{cols}")
    
    total_elements = rows * cols
    print(f"  Total elements: {total_elements:,}")
    
    random.seed(42)
    
    with open(output_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['row', 'col', 'value'])
        
        written = 0
        for i in range(rows):
            for j in range(cols):
                value = random.uniform(0.1, 10.0)
                writer.writerow([i, j, f"{value:.6f}"])
                written += 1
                
                if written % 50000 == 0:
                    print(f"    Progress: {written:,} / {total_elements:,} ({written*100/total_elements:.1f}%)")
    
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"  Saved to: {output_path}")
    print(f"  File size: {file_size_mb:.2f} MB")


def generate_factor_matrices(dimensions, rank, output_dir):
    """
    Generate factor matrices for MTTKRP (used in tensor decomposition)
    
    Args:
        dimensions: List of mode sizes [d1, d2, d3, ...]
        rank: Rank of the decomposition
        output_dir: Directory to save factor matrices
    """
    print(f"\nGenerating factor matrices for {len(dimensions)}-mode tensor")
    print(f"  Dimensions: {dimensions}")
    print(f"  Rank: {rank}")
    
    random.seed(42)
    
    for mode, dim in enumerate(dimensions):
        output_path = os.path.join(output_dir, f"factor_matrix_mode{mode}_{dim}x{rank}.csv")
        
        print(f"\n  Mode {mode}: {dim}x{rank}")
        
        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Header: index, r0, r1, r2, ..., r{rank-1}
            header = ['index'] + [f'r{r}' for r in range(rank)]
            writer.writerow(header)
            
            for i in range(dim):
                row = [i] + [f"{random.uniform(0.0, 1.0):.6f}" for _ in range(rank)]
                writer.writerow(row)
        
        file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"    Saved to: {output_path}")
        print(f"    File size: {file_size_mb:.2f} MB")


def print_summary(output_dir):
    """Print summary of generated files"""
    print("\n" + "=" * 80)
    print("DATA GENERATION SUMMARY")
    print("=" * 80)
    
    if not os.path.exists(output_dir):
        print(f"Directory {output_dir} does not exist")
        return
    
    files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
    files.sort()
    
    total_size = 0
    
    print("\n{:<60} {:>15}".format("File", "Size (MB)"))
    print("-" * 80)
    
    for filename in files:
        filepath = os.path.join(output_dir, filename)
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        total_size += size_mb
        print(f"{filename:<60} {size_mb:>12.2f} MB")
    
    print("-" * 80)
    print(f"{'TOTAL':<60} {total_size:>12.2f} MB")
    print(f"\nTotal files: {len(files)}")


def main():
    """Main data generation pipeline"""
    
    print("=" * 80)
    print("TENSOR AND DENSE MATRIX DATA GENERATOR")
    print("=" * 80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    output_dir = "synthetic-data"
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    print(f"\nOutput directory: {output_dir}")
    
    # PART 1: SPARSE TENSORS
    print("\n" + "=" * 80)
    print("PART 1: GENERATING SPARSE TENSORS")
    print("=" * 80)
    
    tensor_configs = [
        # (dim1, dim2, dim3, sparsity, label)
        (10, 10, 10, 0.95, "tiny"),
        (20, 20, 20, 0.95, "small"),
        (50, 50, 50, 0.95, "medium"),
        (100, 100, 100, 0.95, "large"),
        (200, 200, 200, 0.98, "xlarge"),  # Higher sparsity for larger
    ]
    
    for dim1, dim2, dim3, sparsity, label in tensor_configs:
        output_path = os.path.join(output_dir, f"sparse_tensor_{dim1}x{dim2}x{dim3}.csv")
        generate_sparse_tensor(dim1, dim2, dim3, sparsity, output_path)
    
    # PART 2: DENSE MATRICES (for SpMM-Dense)
    print("\n" + "=" * 80)
    print("PART 2: GENERATING DENSE MATRICES (for SpMM-Dense)")
    print("=" * 80)
    
    dense_configs = [
        # (rows, cols, label)
        (10, 5, "tiny"),
        (100, 10, "small"),
        (1000, 20, "medium"),
        (10000, 50, "large"),
    ]
    
    for rows, cols, label in dense_configs:
        output_path = os.path.join(output_dir, f"dense_matrix_{rows}x{cols}.csv")
        generate_dense_matrix(rows, cols, output_path)
    
    # PART 3: FACTOR MATRICES (for MTTKRP)
    print("\n" + "=" * 80)
    print("PART 3: GENERATING FACTOR MATRICES (for MTTKRP)")
    print("=" * 80)
    
    factor_configs = [
        # (dimensions, rank, label)
        ([10, 10, 10], 5, "tiny"),
        ([20, 20, 20], 10, "small"),
        ([50, 50, 50], 10, "medium"),
        ([100, 100, 100], 20, "large"),
    ]
    
    for dimensions, rank, label in factor_configs:
        generate_factor_matrices(dimensions, rank, output_dir)
    
    # PART 4: RECTANGULAR TENSORS (for testing different aspect ratios)
    print("\n" + "=" * 80)
    print("PART 4: GENERATING RECTANGULAR TENSORS")
    print("=" * 80)
    
    rect_configs = [
        # (dim1, dim2, dim3, sparsity, label)
        (100, 50, 20, 0.95, "rect1"),
        (50, 100, 50, 0.95, "rect2"),
        (200, 100, 50, 0.95, "rect3"),
    ]
    
    for dim1, dim2, dim3, sparsity, label in rect_configs:
        output_path = os.path.join(output_dir, f"sparse_tensor_{dim1}x{dim2}x{dim3}.csv")
        generate_sparse_tensor(dim1, dim2, dim3, sparsity, output_path)
    
    print_summary(output_dir)
    
    print("\n" + "=" * 80)
    print("DATA GENERATION COMPLETE!")
    print("=" * 80)
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nNext steps:")
    print("  1. Verify data: ls -lh synthetic-data/")
    print("  2. Run verification: sbt 'runMain benchmarks.VerificationRunner'")
    print("  3. Run benchmarks: sbt 'runMain benchmarks.BenchmarkRunner'")


if __name__ == "__main__":
    main()