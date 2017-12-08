#!/bin/bash

DATA_DIR=~/data/d3m_new
SCHEMA=/datasetDoc.json
OUTPUT_SCHEMA=tables/mergedDataSchema.json
DATA_PATH=/tables/learningData.csv
RAW_DATA=/data/raw_data
OUTPUT_PATH=tables/merged.csv
DATASET_FOLDER_SUFFIX=_dataset
DATASETS=(26_radon_seed 32_wikiqa 60_jester 185_baseball 196_autoMpg 313_spectrometer 38_sick 4550_MiceProtein)
HAS_HEADER=1

for DATASET in "${DATASETS[@]}"
do
    echo "--------------------------------------------------------------------------------"
    echo " Merging $DATASET dataset"
    echo "--------------------------------------------------------------------------------"
    go run cmd/distil-merge/main.go \
        --schema="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX/$SCHEMA" \
        --data="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX/$DATA_PATH" \
        --raw-data="$DATA_DIR/$DATASET/$RAW_DATA" \
        --output-path="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX/$OUTPUT_PATH" \
        --output-schema-path="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX/$OUTPUT_SCHEMA" \
        --has-header=$HAS_HEADER
done
