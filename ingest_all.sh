#!/bin/bash

DATA_DIR=~/data/d3m
SCHEMA=/data/dataSchema.json
MERGED=/data/merged.csv
CLASSIFICATION=/data/classification.json
SUMMARY=/data/summary.json
IMPORTANCE=/data/importance.json
METADATA_INDEX=datasets
DATASETS=(o_185 o_196 o_313 o_38 o_4550)
ES_ENDPOINT=http://localhost:9200

for DATASET in "${DATASETS[@]}"
do
    echo "--------------------------------------------------------------------------------"
    echo " Ingesting $DATASET dataset"
    echo "--------------------------------------------------------------------------------"
    go run cmd/distil-ingest/main.go \
        --es-endpoint="$ES_ENDPOINT" \
        --es-metadata-index="$METADATA_INDEX" \
        --es-data-index="$DATASET" \
        --schema="$DATA_DIR/$DATASET/$SCHEMA" \
        --dataset="$DATA_DIR/$DATASET/$MERGED" \
        --classification="$DATA_DIR/$DATASET/$CLASSIFICATION" \
        --summary="$DATA_DIR/$DATASET/$SUMMARY" \
        --importance="$DATA_DIR/$DATASET/$IMPORTANCE" \
        --clear-existing \
        --include-raw-dataset
done
