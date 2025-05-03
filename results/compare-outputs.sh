#!/bin/bash

# (in results) ./compare-outputs.sh ratings_small client1 
base="result_${1}.json"
check="../data/results_${2}.json"
python3 comparer.py "$base" "$check"