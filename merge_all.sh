#!/bin/bash

DATA_DIR=~/data/d3m
SCHEMA=/data/dataSchema.json
TRAINING_DATA=/data/trainData.csv
TRAINING_TARGETS=/data/trainTargets.csv
OUTPUT=/data/merged.csv
DATASETS=(o_185 o_196 o_313 o_38 o_4550)

for DATASET in "${DATASETS[@]}"
do
    go run cmd/merge/main.go \
        --schema="$DATA_DIR/$DATASET/$SCHEMA" \
        --training-data="$DATA_DIR/$DATASET/$TRAINING_DATA" \
        --training-targets="$DATA_DIR/$DATASET/$TRAINING_TARGETS" \
        --output="$DATA_DIR/$DATASET/$OUTPUT"
done
