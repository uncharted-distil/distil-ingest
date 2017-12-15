#!/bin/bash

DATA_DIR=~/data/d3m_new
SCHEMA=tables/mergedDataSchema.json
MERGED_FILE=tables/merged.csv
OUTPUT=/tables/classification.json
DATASET_FOLDER_SUFFIX=_dataset
DATASETS=(26_radon_seed 32_wikiqa 60_jester 185_baseball 196_autoMpg 313_spectrometer 38_sick 4550_MiceProtein)
REST_ENDPOINT=HTTP://localhost:5000
CLASSIFICATION_FUNCTION=fileUpload

# start classification REST API container
docker run -d --rm --name classification_rest -p 5000:5000 primitives.azurecr.io/data.world_container:v1.0
./wait-for-it.sh -t 0 localhost:5000
echo "Waiting for the service to be available..."
sleep 10

for DATASET in "${DATASETS[@]}"
do
    echo "--------------------------------------------------------------------------------"
    echo " Classifying $DATASET dataset"
    echo "--------------------------------------------------------------------------------"
    go run cmd/distil-classify/main.go \
        --schema="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX/$SCHEMA" \
        --rest-endpoint="$REST_ENDPOINT" \
        --classification-function="$CLASSIFICATION_FUNCTION" \
        --dataset="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX/$MERGED_FILE" \
        --output="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX/$OUTPUT"
done

# stop classification REST API container
docker stop classification_rest
