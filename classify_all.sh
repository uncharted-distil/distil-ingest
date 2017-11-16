#!/bin/bash

DATA_DIR=~/data/d3m
SCHEMA=/data/mergedDataSchema.json
MERGED_FILE=data/merged.csv
OUTPUT=/data/classification.json
DATASETS=(r_26 r_27 r_32 r_60 o_185 o_196 o_313 o_38 o_4550)
REST_ENDPOINT=HTTP://localhost:5000
CLASSIFICATION_FUNCTION=fileUpload

for DATASET in "${DATASETS[@]}"
do
    echo "--------------------------------------------------------------------------------"
    echo " Classifying $DATASET dataset"
    echo "--------------------------------------------------------------------------------"
    go run cmd/distil-classify/main.go \
        --schema="$DATA_DIR/$DATASET/$SCHEMA" \
        --rest-endpoint="$REST_ENDPOINT" \
        --classification-function="$CLASSIFICATION_FUNCTION" \
        --dataset="$DATA_DIR/$DATASET/$MERGED_FILE" \
        --output="$DATA_DIR/$DATASET/$OUTPUT" \
        --include-raw-dataset
done
