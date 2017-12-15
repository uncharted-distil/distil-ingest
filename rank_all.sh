#!/bin/bash

DATA_DIR=~/data/d3m_new
SCHEMA=/tables/mergedDataSchema.json
MERGED=/tables/merged.csv
CLASSIFICATION=/tables/classification.json
OUTPUT=tables/importance.json
DATASETS=(26_radon_seed 32_wikiqa 60_jester 185_baseball 196_autoMpg 313_spectrometer 38_sick 4550_MiceProtein)
TYPE_SOURCE=classification
HAS_HEADER=1
REST_ENDPOINT=HTTP://localhost:5000
RANKING_FUNCTION=pca
NUMERIC_OUTPUT_SUFFIX=_numeric.csv
DATASET_FOLDER_SUFFIX=_dataset

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
        --schema="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX/$SCHEMA" \
        --dataset="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX/$MERGED" \
        --rest-endpoint="$REST_ENDPOINT" \
        --ranking-function="$RANKING_FUNCTION" \
        --numeric-output="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX/$DATASET$NUMERIC_OUTPUT_SUFFIX" \
        --classification="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX/$CLASSIFICATION" \
        --has-header=$HAS_HEADER \
        --output="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX/$OUTPUT" \
        --type-source="$TYPE_SOURCE"
done

docker stop ranking_rest
