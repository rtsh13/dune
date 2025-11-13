import numpy as np
import pandas as pd
import os

# ===== CONFIGURATION =====
vector_sizes = [10, 100, 1000, 10000, 50000, 100000]
sparse_density = 0.15   # For sparse vectors (15% non-zero)
seed = 42
output_dir = "synthetic-data"
value_range = (-15, 15)  # Range for random values
# =========================

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

print( * 60)
print("GENERATING DENSE VECTORS")
print( * 60)

for n in vector_sizes:
    print(f"\nGenerating dense vector of size {n}...")
    
    np.random.seed(seed)
    values = np.random.uniform(value_range[0], value_range[1], n)
    
    df = pd.DataFrame({
        'index': np.arange(n),
        'value': values
    })
    
    filename = f"{output_dir}/dense_vector_{n}.csv"
    df.to_csv(filename, index=False)
    
    print(f"  Saved to {filename}")
    print(f"  Non-zero entries: {len(df):,}")

print("\n" +  * 60)
print("GENERATING SPARSE VECTORS")
print( * 60)

for n in vector_sizes:
    print(f"\nGenerating sparse vector of size {n}...")
    
    np.random.seed(seed + n)
    
    # Determine how many non-zero entries
    nnz = int(n * sparse_density)
    
    # Random indices for non-zero values
    indices = np.random.choice(n, size=nnz, replace=False)
    indices = np.sort(indices)  # Sort for cleaner output
    
    # Random values
    values = np.random.uniform(value_range[0], value_range[1], nnz)
    
    df = pd.DataFrame({
        'index': indices,
        'value': values
    })
    
    filename = f"{output_dir}/sparse_vector_{n}.csv"
    df.to_csv(filename, index=False)
    
    print(f"  Saved to {filename}")
    print(f"  Non-zero entries: {len(df):,}")
    print(f"  Density: {sparse_density*100}%")

print(f"\nAll vectors generated in '{output_dir}/' folder")