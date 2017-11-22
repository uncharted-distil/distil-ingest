#!/bin/bash

DATA_DIR=~/data/d3m
SCHEMA=/data/mergedDataSchema.json
MERGED=/data/merged.csv
CLASSIFICATION=/data/classification.json
OUTPUT=/data/importance.json
DATASETS=(r_26 r_27 r_32 r_60 o_185 o_196 o_313 o_38 o_4550)
TYPE_SOURCE=classification
HAS_HEADER=1
REST_ENDPOINT=HTTP://localhost:5000
RANKING_FUNCTION=pca
NUMERIC_OUTPUT_SUFFIX=_numeric.csv
DATASET_DATA_DIR=data

docker run -d --rm --name ranking_rest  -p 5000:5000 primitives.azurecr.io/http_features:0.2
./wait-for-it.sh -t 0 localhost:5000
echo "Waiting for the service to be available..."
sleep 10

for DATASET in "${DATASETS[@]}"
do
    echo "--------------------------------------------------------------------------------"
    echo " Ranking $DATASET dataset"
    echo "--------------------------------------------------------------------------------"
    go run cmd/distil-rank/main.go \
        --schema="$DATA_DIR/$DATASET/$SCHEMA" \
        --dataset="$DATA_DIR/$DATASET/$MERGED" \
        --rest-endpoint="$REST_ENDPOINT" \
        --ranking-function="$RANKING_FUNCTION" \
        --numeric-output="$DATA_DIR/$DATASET/$DATASET_DATA_DIR/$DATASET$NUMERIC_OUTPUT_SUFFIX" \
        --classification="$DATA_DIR/$DATASET/$CLASSIFICATION" \
        --has-header=$HAS_HEADER \
        --output="$DATA_DIR/$DATASET/$OUTPUT" \
        --type-source="$TYPE_SOURCE" \
        --include-raw-dataset
done

docker stop ranking_rest
