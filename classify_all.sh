#!/bin/bash

DATA_DIR=~/data/d3m
SCHEMA=/data/mergedDataSchema.json
AWS_PREFIX=https://s3.amazonaws.com/d3m-data/merged_o_data
AWS_SUFFIX=_merged.csv
OUTPUT=/data/classification.json
DATASETS=(r_26 r_27 r_32 r_60 o_185 o_196 o_313 o_38 o_4550)
REST_ENDPOINT="HTTP://localhost:5000"
CLASSIFICATION_FUNCTION="fileUpload"

for DATASET in "${DATASETS[@]}"
do
    echo "--------------------------------------------------------------------------------"
    echo " Classifying $DATASET dataset"
    echo "--------------------------------------------------------------------------------"
    go run cmd/distil-classify/main.go \
        --schema="$DATA_DIR/$DATASET/$SCHEMA" \
        --rest-endpoint="$REST_ENDPOINT" \
        --classification-function="$CLASSIFICATION_FUNCTION" \
        --dataset="$AWS_PREFIX/$DATASET$AWS_SUFFIX" \
        --output="$DATA_DIR/$DATASET/$OUTPUT" \
        --include-raw-dataset
done
