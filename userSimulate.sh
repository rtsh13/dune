echo "=== Cleaning previous results ==="
rm -rf interactiveResults
echo "✓ Done"
echo

echo "=== Compiling project ==="
sbt compile
echo

echo "=== Running multiplication ==="
echo "Input A: synthetic-data/sparse_matrix_1000x1000.csv"
echo "Input B: synthetic-data/dense_vector_1000.csv"
echo "Output:  interactiveResults/"
echo

sbt "runMain Main -a synthetic-data/sparse_matrix_1000x1000.csv -b synthetic-data/dense_vector_1000.csv -o interactiveResults/"

echo
echo "=== Results saved to interactiveResults/ ==="
echo "View with: cat interactiveResults/part-* | head -20"