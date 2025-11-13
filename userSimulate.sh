#!/bin/bash

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Default parameters
INPUT_A="synthetic-data/sparse_matrix_1000x1000.csv"
INPUT_B="synthetic-data/dense_vector_1000.csv"
OUTPUT_DIR="interactiveResults"
STRATEGY=""
FORMAT=""
VECTOR_TYPE=""
MEMORY="4g"
LOG_LEVEL="WARN"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --input-a)
      INPUT_A="$2"
      shift 2
      ;;
    --input-b)
      INPUT_B="$2"
      shift 2
      ;;
    --output)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --strategy)
      STRATEGY="$2"
      shift 2
      ;;
    --format)
      FORMAT="$2"
      shift 2
      ;;
    --vector-type)
      VECTOR_TYPE="$2"
      shift 2
      ;;
    --memory)
      MEMORY="$2"
      shift 2
      ;;
    --log-level)
      LOG_LEVEL="$2"
      shift 2
      ;;
    --help|-h)
      echo "Usage: $0 [OPTIONS]"
      echo
      echo "Options:"
      echo "  --input-a <file>       First input file (default: synthetic-data/sparse_matrix_1000x1000.csv)"
      echo "  --input-b <file>       Second input file (default: synthetic-data/dense_vector_1000.csv)"
      echo "  --output <dir>         Output directory (default: interactiveResults)"
      echo "  --strategy <name>      Strategy: baseline|optimized|adaptive|efficient|balanced|mapsidejoin|blockpartitioned"
      echo "  --format <format>      Format: COO|CSR|CSC"
      echo "  --vector-type <type>   Vector type: dense|sparse"
      echo "  --memory <size>        Memory allocation (default: 4g)"
      echo "  --log-level <level>    Log level: ERROR|WARN|INFO|DEBUG (default: WARN)"
      echo "  --help, -h             Show this help message"
      echo
      echo "Examples:"
      echo "  $0                                                    # Basic run with defaults"
      echo "  $0 --strategy efficient                              # Use efficient strategy"
      echo "  $0 --format CSR --strategy optimized                 # CSR format with optimization"
      echo "  $0 --strategy blockpartitioned --memory 8g           # Block-partitioned with more memory"
      exit 0
      ;;
    *)
      echo -e "${RED}Unknown option: $1${NC}"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

echo -e "${BLUE}SPARSE MATRIX MULTIPLICATION ENGINE - INTERACTIVE RUN${NC}"
echo

# Verify input files exist
echo -e "${YELLOW} Verifying Input Files ${NC}"
if [ ! -f "$INPUT_A" ]; then
  echo -e "${RED}Error: Input file A not found: $INPUT_A${NC}"
  exit 1
fi
if [ ! -f "$INPUT_B" ]; then
  echo -e "${RED}Error: Input file B not found: $INPUT_B${NC}"
  exit 1
fi
echo -e "${GREEN}Input file A exists: $INPUT_A${NC}"
echo -e "${GREEN}Input file B exists: $INPUT_B${NC}"
echo

# Clean previous results
echo -e "${YELLOW}Cleaning Previous Results${NC}"
if [ -d "$OUTPUT_DIR" ]; then
  rm -rf "$OUTPUT_DIR"
  echo -e "${GREEN}Removed existing output directory${NC}"
else
  echo -e "${GREEN}No previous results to clean${NC}"
fi
echo

# Compile project
echo -e "${YELLOW}Compiling Project${NC}"
if sbt compile; then
  echo -e "${GREEN}Compilation successful${NC}"
else
  echo -e "${RED}Compilation failed${NC}"
  exit 1
fi
echo

# Build command with optional parameters
CMD="runMain Main -a $INPUT_A -b $INPUT_B -o $OUTPUT_DIR --memory $MEMORY --log-level $LOG_LEVEL"

if [ -n "$STRATEGY" ]; then
  CMD="$CMD --strategy $STRATEGY"
fi

if [ -n "$FORMAT" ]; then
  CMD="$CMD --format $FORMAT"
fi

if [ -n "$VECTOR_TYPE" ]; then
  CMD="$CMD --vector-type $VECTOR_TYPE"
fi

# Display run configuration
echo -e "${YELLOW} Run Configuration ${NC}"
echo -e "Input A:      ${GREEN}$INPUT_A${NC}"
echo -e "Input B:      ${GREEN}$INPUT_B${NC}"
echo -e "Output:       ${GREEN}$OUTPUT_DIR/${NC}"
echo -e "Memory:       ${GREEN}$MEMORY${NC}"
echo -e "Log Level:    ${GREEN}$LOG_LEVEL${NC}"
if [ -n "$STRATEGY" ]; then
  echo -e "Strategy:     ${GREEN}$STRATEGY${NC}"
else
  echo -e "Strategy:     ${GREEN}adaptive (default)${NC}"
fi
if [ -n "$FORMAT" ]; then
  echo -e "Format:       ${GREEN}$FORMAT${NC}"
else
  echo -e "Format:       ${GREEN}COO (default)${NC}"
fi
if [ -n "$VECTOR_TYPE" ]; then
  echo -e "Vector Type:  ${GREEN}$VECTOR_TYPE${NC}"
else
  echo -e "Vector Type:  ${GREEN}dense (default)${NC}"
fi
echo

# Run multiplication
echo -e "${YELLOW} Running Multiplication ${NC}"
echo -e "${BLUE}Command: sbt \"$CMD\"${NC}"
echo

if sbt "$CMD"; then
  echo
  echo -e "${GREEN}Multiplication completed successfully${NC}"
else
  echo
  echo -e "${RED}Multiplication failed${NC}"
  exit 1
fi

# Display results
echo -e "${BLUE}${NC}"
echo -e "${BLUE}     RESULTS${NC}"
echo -e "${BLUE}${NC}"

if [ -d "$OUTPUT_DIR" ]; then
  RESULT_FILES=$(find "$OUTPUT_DIR" -name "part-*" 2>/dev/null | wc -l)
  
  if [ "$RESULT_FILES" -gt 0 ]; then
    echo -e "${GREEN}Results saved to: $OUTPUT_DIR/${NC}"
    echo -e "  Number of result files: $RESULT_FILES"
    echo
    
    # Show preview of results
    echo -e "${YELLOW} Result Preview (first 20 entries) ${NC}"
    cat "$OUTPUT_DIR"/part-* 2>/dev/null | head -20
    echo
    
    # Show total result count
    TOTAL_LINES=$(cat "$OUTPUT_DIR"/part-* 2>/dev/null | wc -l)
    echo -e "${GREEN}Total result entries: $TOTAL_LINES${NC}"
    echo
    
    # Useful commands
    echo -e "${YELLOW} Useful Commands ${NC}"
    echo -e "View all results:     ${BLUE}cat $OUTPUT_DIR/part-*${NC}"
    echo -e "Count entries:        ${BLUE}cat $OUTPUT_DIR/part-* | wc -l${NC}"
    echo -e "View first 50:        ${BLUE}cat $OUTPUT_DIR/part-* | head -50${NC}"
    echo -e "Search for index 42:  ${BLUE}cat $OUTPUT_DIR/part-* | grep '^42,'${NC}"
  else
    echo -e "${RED}No result files found in $OUTPUT_DIR${NC}"
  fi
else
  echo -e "${RED}Output directory not created${NC}"
fi

echo
echo -e "${BLUE}${NC}"
echo -e "${GREEN}RUN COMPLETE${NC}"
echo -e "${BLUE}${NC}"