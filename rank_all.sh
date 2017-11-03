#!/bin/bash

DATA_DIR=~/data/d3m
SCHEMA=/data/dataSchema.json
MERGED=/data/merged.csv
CLASSIFICATION=/data/classification.json
AWS_OUTPUT_BUCKET=d3m-data
AWS_OUTPUT_KEY_PREFIX=numeric_o_data
AWS_OUTPUT_KEY_SUFFIX=_numeric.csv
OUTPUT=/data/importance.json
DATASETS=(r_26 r_27 r_32 r_60 o_185 o_196 o_313 o_38 o_4550)
KAFKA_ENDPOINT=10.108.4.41:9092
TYPE_SOURCE=classification
HAS_HEADER=1

for DATASET in "${DATASETS[@]}"
do
    echo "--------------------------------------------------------------------------------"
    echo " Ranking $DATASET dataset"
    echo "--------------------------------------------------------------------------------"
    go run cmd/distil-rank/main.go \
        --schema="$DATA_DIR/$DATASET/$SCHEMA" \
        --dataset="$DATA_DIR/$DATASET/$MERGED" \
        --classification="$DATA_DIR/$DATASET/$CLASSIFICATION" \
        --output-bucket="$AWS_OUTPUT_BUCKET" \
        --output-key="$AWS_OUTPUT_KEY_PREFIX/$DATASET$AWS_OUTPUT_KEY_SUFFIX" \
        --has-header=$HAS_HEADER \
        --kafka-endpoints="$KAFKA_ENDPOINT" \
        --output="$DATA_DIR/$DATASET/$OUTPUT" \
        --type-source="$TYPE_SOURCE" \
        --include-raw-dataset
done
