#!/bin/bash

DATA_DIR=~/data/d3m
AWS_PREFIX=https://s3.amazonaws.com/d3m-data/merged_o_data
AWS_SUFFIX=_merged.csv
OUTPUT=/data/classification.json
DATASETS=(o_185 o_196 o_313 o_38 o_4550)
KAFKA_ENDPOINT=10.108.4.41:9092

for DATASET in "${DATASETS[@]}"
do
    echo "--------------------------------------------------------------------------------"
    echo " Classifying $DATASET dataset"
    echo "--------------------------------------------------------------------------------"
    go run cmd/distil-classify/main.go \
        --kafka-endpoints="$KAFKA_ENDPOINT" \
        --dataset="$AWS_PREFIX/$DATASET$AWS_SUFFIX" \
        --output="$DATA_DIR/$DATASET/$OUTPUT" \
        --include-raw-dataset
done
