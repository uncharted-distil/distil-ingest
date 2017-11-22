#!/bin/bash

DATA_DIR=~/data/d3m
SCHEMA=/data/dataSchema.json
OUTPUT_SCHEMA=/data/mergedDataSchema.json
TRAINING_DATA=/data/trainData.csv
TRAINING_TARGETS=/data/trainTargets.csv
RAW_DATA=/data/raw_data
OUTPUT_PATH=/data/merged.csv
DATASETS=(r_26 r_27 r_32 r_60 o_185 o_196 o_313 o_38 o_4550)
HAS_HEADER=1

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
        --output-path="$DATA_DIR/$DATASET/$OUTPUT_PATH" \
        --output-schema-path="$DATA_DIR/$DATASET/$OUTPUT_SCHEMA" \
        --has-header=$HAS_HEADER \
        --include-raw-dataset
done
