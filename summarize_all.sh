#!/bin/bash

DATA_DIR=~/data/d3m_new
DATA_FILE=tables/merged_header.csv
OUTPUT=/tables/summary.json
DATASET_FOLDER_SUFFIX=_dataset
DATASETS=(26_radon_seed 32_wikiqa 60_jester 185_baseball 196_autoMpg 313_spectrometer 38_sick 4550_MiceProtein)
REST_ENDPOINT=HTTP://10.108.4.42:5001
SUMMARY_FUNCTION=fileUpload

# start summary REST API container
#docker run -d --rm --name summary_rest -p 5000:5000 primitives.azurecr.io/simon:1.0.0
#./wait-for-it.sh -t 0 localhost:5000
#echo "Waiting for the service to be available..."
#sleep 10

for DATASET in "${DATASETS[@]}"
do
    echo "--------------------------------------------------------------------------------"
    echo " Summarizing $DATASET dataset"
    echo "--------------------------------------------------------------------------------"
    go run cmd/distil-summary/main.go \
        --rest-endpoint="$REST_ENDPOINT" \
        --summary-function="$SUMMARY_FUNCTION" \
        --dataset="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX/$DATA_FILE" \
        --output="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX/$OUTPUT"
done

# stop classification REST API container
docker stop summary_rest
