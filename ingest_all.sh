#!/bin/bash

DATA_DIR=~/data/d3m
SCHEMA=/data/dataSchema.json
MERGED=/data/merged.csv
DATASETS=(o_185 o_196 o_313 o_38 o_4550)
ES_ENDPOINT=http://localhost:9200

for DATASET in "${DATASETS[@]}"
do
    echo "--------------------------------------------------------------------------------"
    echo " Ingesting $DATASET dataset"
    echo "--------------------------------------------------------------------------------"
    go run cmd/distil-ingest/main.go \
        --es-endpoint="$ES_ENDPOINT" \
        --es-index="$DATASET" \
        --schema="$DATA_DIR/$DATASET/$SCHEMA" \
        --dataset="$DATA_DIR/$DATASET/$MERGED" \
        --clear-existing
done
