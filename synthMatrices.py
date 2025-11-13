import numpy as np
import pandas as pd
import scipy.sparse as sp
import os

# ===== CONFIGURATION =====
square_sizes = [10, 100, 1000, 5000, 10000]
rectangular_sizes = [
    (10, 15),
    (100, 150),
    (1000, 1500),
    (10000, 15000),
    (50000, 75000)
]
sparse_density = 0.85   # For sparse matrices
dense_density = 1.0     # For dense matrices (100% non-zero)
dense_square_sizes = [10, 100, 1000, 5000, 10000]  # Smaller sizes for dense
dense_rectangular_sizes = [(10, 15), (100, 150), (1000, 1500)]  # Dense rectangular
seed = 42
output_dir = "synthetic-data"
# =========================

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

print( * 60)
print("GENERATING SQUARE SPARSE MATRICES")
print( * 60)

for n in square_sizes:
    print(f"\nGenerating {n}x{n} sparse matrix...")
    
    np.random.seed(seed)
    A_sparse = sp.random(n, n, density=sparse_density, format='coo')
    
    df = pd.DataFrame({
        'row': A_sparse.row,
        'column': A_sparse.col,
        'value': A_sparse.data
    })
    
    filename = f"{output_dir}/sparse_matrix_{n}x{n}.csv"
    df.to_csv(filename, index=False)
    
    print(f"  Saved to {filename}")
    print(f"  Non-zero entries: {len(df):,}")
    print(f"  Density: {sparse_density*100}%")

print("\n" +  * 60)
print("GENERATING RECTANGULAR SPARSE MATRICES")
print( * 60)

for rows, cols in rectangular_sizes:
    print(f"\nGenerating {rows}x{cols} rectangular sparse matrix...")
    
    np.random.seed(seed + rows)
    A_sparse = sp.random(rows, cols, density=sparse_density, format='coo')
    
    df = pd.DataFrame({
        'row': A_sparse.row,
        'column': A_sparse.col,
        'value': A_sparse.data
    })
    
    filename = f"{output_dir}/sparse_matrix_{rows}x{cols}.csv"
    df.to_csv(filename, index=False)
    
    print(f"  Saved to {filename}")
    print(f"  Non-zero entries: {len(df):,}")
    print(f"  Density: {sparse_density*100}%")

print("\n" +  * 60)
print("GENERATING SQUARE DENSE MATRICES")
print( * 60)

for n in dense_square_sizes:
    print(f"\nGenerating {n}x{n} dense matrix...")
    
    np.random.seed(seed)
    A_dense = sp.random(n, n, density=dense_density, format='coo')
    
    df = pd.DataFrame({
        'row': A_dense.row,
        'column': A_dense.col,
        'value': A_dense.data
    })
    
    filename = f"{output_dir}/dense_matrix_{n}x{n}.csv"
    df.to_csv(filename, index=False)
    
    print(f"  Saved to {filename}")
    print(f"  Non-zero entries: {len(df):,}")
    print(f"  Density: {dense_density*100}%")

print("\n" +  * 60)
print("GENERATING RECTANGULAR DENSE MATRICES")
print( * 60)

for rows, cols in dense_rectangular_sizes:
    print(f"\nGenerating {rows}x{cols} rectangular dense matrix...")
    
    np.random.seed(seed + rows)
    A_dense = sp.random(rows, cols, density=dense_density, format='coo')
    
    df = pd.DataFrame({
        'row': A_dense.row,
        'column': A_dense.col,
        'value': A_dense.data
    })
    
    filename = f"{output_dir}/dense_matrix_{rows}x{cols}.csv"
    df.to_csv(filename, index=False)
    
    print(f"  Saved to {filename}")
    print(f"  Non-zero entries: {len(df):,}")
    print(f"  Density: {dense_density*100}%")

print(f"\nAll matrices generated in '{output_dir}/' folder")