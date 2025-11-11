#!/bin/bash

# Complete End-to-End Evaluation Script
# Runs all benchmarks and generates comprehensive report
# 
# Usage: ./rwBenchmarks/script.sh [SKIP_PLOTS]
# Example: ./rwBenchmarks/script.sh SKIP_PLOTS

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Get script directory and project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$PROJECT_ROOT"

# Parse command-line arguments
SKIP_PLOTS=false
for arg in "$@"; do
    if [ "$arg" = "SKIP_PLOTS" ]; then
        SKIP_PLOTS=true
    fi
done

echo -e "${BLUE}================================================================${NC}"
echo -e "${BLUE}  COMPREHENSIVE END-TO-END SYSTEM EVALUATION${NC}"
echo -e "${BLUE}================================================================${NC}"
echo
echo "Project root: $PROJECT_ROOT"
if [ "$SKIP_PLOTS" = true ]; then
    echo -e "${YELLOW}Note: Plot generation will be skipped${NC}"
fi
echo

# Step 1: Check prerequisites
echo -e "${YELLOW}[Step 1/5]${NC} Checking prerequisites..."

if ! command -v sbt &> /dev/null; then
    echo -e "${RED}✗ sbt not found${NC}"
    exit 1
fi

# Check Python availability
if ! command -v python3 &> /dev/null; then
    echo -e "${YELLOW}⚠ python3 not found - plots will be skipped${NC}"
    SKIP_PLOTS=true
fi

echo -e "${GREEN}✓${NC} Prerequisites checked"
echo

# Step 2: Verify data exists
echo -e "${YELLOW}[Step 2/5]${NC} Verifying synthetic data..."

if [ ! -d "synthetic-data" ]; then
    echo -e "${RED}✗ synthetic-data/ directory not found${NC}"
    echo "  Please run: sbt \"runMain utils.MatrixGenerator\""
    exit 1
fi

# Check for required datasets
DATASETS=(
    "synthetic-data/sparse_matrix_1000x1000.csv"
    "synthetic-data/dense_vector_1000.csv"
)

for dataset in "${DATASETS[@]}"; do
    if [ ! -f "$dataset" ]; then
        echo -e "${RED}✗ Missing: $dataset${NC}"
        echo "  Please generate test data first"
        exit 1
    fi
done

echo -e "${GREEN}✓${NC} All required datasets found"
echo

# Step 3: Compile
echo -e "${YELLOW}[Step 3/5]${NC} Compiling project..."
sbt compile

if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Compilation failed${NC}"
    exit 1
fi

echo -e "${GREEN}✓${NC} Compilation complete"
echo

# Step 4: Run benchmarks
echo -e "${YELLOW}[Step 4/5]${NC} Running end-to-end benchmarks..."
echo -e "  This will take 10-15 minutes..."
echo

# Create results directory
mkdir -p rwBenchmarks/results/plots

echo "Running: sbt \"runMain realworldbenchmarks.EndToEndBenchmarksRunner\""
echo

# Run the comprehensive benchmarks
sbt "runMain realworldbenchmarks.EndToEndBenchmarksRunner"

if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Benchmarks failed${NC}"
    exit 1
fi

echo -e "${GREEN}✓${NC} Benchmarks complete"
echo

# Step 5: Generate plots (if not skipped)
if [ "$SKIP_PLOTS" = false ]; then
    echo -e "${YELLOW}[Step 5/5]${NC} Generating plots..."
    
    # Check if virtual environment exists
    if [ -d "venv" ] && [ -f "venv/bin/activate" ]; then
        echo "Using existing virtual environment..."
        # ✓ Check if packages are installed in venv
        if ! venv/bin/python3 -c "import matplotlib" &> /dev/null 2>&1; then
            echo "Installing missing packages in venv..."
            venv/bin/pip install --quiet matplotlib seaborn pandas numpy
        fi
        PYTHON_CMD="venv/bin/python3"
    else
        echo "Checking Python dependencies..."
        MISSING_DEPS=false
        
        for package in matplotlib seaborn pandas numpy; do
            if ! python3 -c "import $package" &> /dev/null 2>&1; then
                MISSING_DEPS=true
                break
            fi
        done
        
        if [ "$MISSING_DEPS" = true ]; then
            echo -e "${YELLOW}⚠ Missing Python packages${NC}"
            echo -e "${YELLOW}Setting up virtual environment...${NC}"
            
            # Create virtual environment
            python3 -m venv venv
            if [ $? -ne 0 ]; then
                echo -e "${RED}✗ Failed to create virtual environment${NC}"
                echo -e "${YELLOW}Skipping plot generation${NC}"
                SKIP_PLOTS=true
            else
                echo "Installing required packages..."
                venv/bin/pip install --quiet matplotlib seaborn pandas numpy
                
                if [ $? -ne 0 ]; then
                    echo -e "${RED}✗ Failed to install packages${NC}"
                    echo -e "${YELLOW}Skipping plot generation${NC}"
                    SKIP_PLOTS=true
                else
                    PYTHON_CMD="venv/bin/python3"
                fi
            fi
        else
            PYTHON_CMD="python3"
        fi
    fi
    
    # Run plot generation if dependencies are satisfied
    if [ "$SKIP_PLOTS" = false ] && [ -f "rwBenchmarks/plot.py" ]; then
        echo "Running: $PYTHON_CMD rwBenchmarks/plot.py"
        $PYTHON_CMD rwBenchmarks/plot.py
        
        if [ $? -ne 0 ]; then
            echo -e "${YELLOW}⚠ Plot generation failed (non-fatal)${NC}"
        else
            echo -e "${GREEN}✓${NC} Plots generated"
        fi
    elif [ ! -f "rwBenchmarks/plot.py" ]; then
        echo -e "${RED}✗ Plot script not found: rwBenchmarks/plot.py${NC}"
    fi
else
    echo -e "${YELLOW}[Step 5/5]${NC} Skipping plot generation (as requested)"
fi

echo

# Summary
echo -e "${GREEN}================================================================${NC}"
echo -e "${GREEN}  EVALUATION COMPLETE!${NC}"
echo -e "${GREEN}================================================================${NC}"
echo
echo -e "${BLUE}Results Location:${NC}"
echo "  📄 Report:  rwBenchmarks/results/end_to_end_evaluation.md"
echo "  📊 Plots:   rwBenchmarks/results/plots/"
echo
echo -e "${BLUE}Generated Files:${NC}"

if [ -f "rwBenchmarks/results/end_to_end_evaluation.md" ]; then
    echo -e "  ${GREEN}✓${NC} end_to_end_evaluation.md"
    wc -l rwBenchmarks/results/end_to_end_evaluation.md
else
    echo -e "  ${YELLOW}⚠${NC} end_to_end_evaluation.md (checking alternate locations...)"
    find . -name "end_to_end_evaluation.md" 2>/dev/null | head -5
fi

if [ -d "rwBenchmarks/results/plots" ]; then
    plot_count=$(ls -1 rwBenchmarks/results/plots/*.png 2>/dev/null | wc -l)
    if [ $plot_count -gt 0 ]; then
        echo -e "  ${GREEN}✓${NC} Plots generated: $plot_count files"
        ls rwBenchmarks/results/plots/*.png 2>/dev/null | sed 's/^/    /'
    else
        echo -e "  ${YELLOW}⚠${NC} No plots generated"
        if [ "$SKIP_PLOTS" = true ]; then
            echo "    Run without SKIP_PLOTS to generate plots"
        fi
    fi
fi

echo
echo -e "${BLUE}Quick View Commands:${NC}"
echo "  cat rwBenchmarks/results/end_to_end_evaluation.md"
if [ -f "rwBenchmarks/results/plots/summary_dashboard.png" ]; then
    echo "  open rwBenchmarks/results/plots/summary_dashboard.png"
fi
echo
echo -e "${BLUE}Next Steps:${NC}"
echo "  1. Review rwBenchmarks/results/end_to_end_evaluation.md"
echo "  2. Include plots in dissertation Section 5.5"
echo "  3. Write analysis explaining results"
echo "  4. Compare with related work"
echo
echo -e "${GREEN}Use these results for your dissertation! 🎓${NC}"
echo