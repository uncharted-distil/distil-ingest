#!/bin/bash

DATA_DIR=~/data/d3m
SCHEMA=/data/dataSchema.json
TRAINING_DATA=/data/trainData.csv
TRAINING_TARGETS=/data/trainTargets.csv
RAW_DATA=/data/raw_data
AWS_OUTPUT_BUCKET=d3m-data
AWS_OUTPUT_KEY_PREFIX=merged_o_data
AWS_OUTPUT_KEY_SUFFIX=_merged.csv
OUTPUT_PATH=/data/merged.csv
DATASETS=(r_32 o_185 o_196 o_313 o_38 o_4550)
HAS_HEADER=1
INCLUDE_HEADER=0
INCLUDE_RAW=1

for DATASET in "${DATASETS[@]}"
do
    echo "--------------------------------------------------------------------------------"
    echo " Merging $DATASET dataset"
    echo "--------------------------------------------------------------------------------"
    go run cmd/distil-merge/main.go \
        --schema="$DATA_DIR/$DATASET/$SCHEMA" \
        --training-data="$DATA_DIR/$DATASET/$TRAINING_DATA" \
        --training-targets="$DATA_DIR/$DATASET/$TRAINING_TARGETS" \
        --raw-data="$DATA_DIR/$DATASET/$RAW_DATA" \
        --output-bucket="$AWS_OUTPUT_BUCKET" \
        --output-key="$AWS_OUTPUT_KEY_PREFIX/$DATASET$AWS_OUTPUT_KEY_SUFFIX" \
        --output-path="$DATA_DIR/$DATASET/$OUTPUT_PATH" \
        --has-header=$HAS_HEADER \
        --include-raw-dataset=$INCLUDE_RAW
done
