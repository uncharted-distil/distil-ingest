#!/bin/bash

DATA_DIR=~/datasets/seed_datasets_current
SCHEMA=mergedDatasetDoc.json
MERGED_FILE=tables/merged.csv
OUTPUT=/classification.json
DATASET_FOLDER_SUFFIX=_dataset
DATASETS=(26_radon_seed 32_wikiqa 60_jester 185_baseball 196_autoMpg 313_spectrometer 38_sick 1491_one_hundred_plants_margin 27_wordLevels 57_hypothyroid 299_libras_move 534_cps_85_wages 1567_poker_hand 22_handgeometry)
REST_ENDPOINT=HTTP://localhost:5000
CLASSIFICATION_FUNCTION=fileUpload

# start classification REST API container
docker run -d --rm --name classification_rest -p 5000:5000 primitives.azurecr.io/simon:1.0.0
./wait-for-it.sh -t 0 localhost:5000
echo "Waiting for the service to be available..."
sleep 10

for DATASET in "${DATASETS[@]}"
do
    echo "--------------------------------------------------------------------------------"
    echo " Classifying $DATASET dataset"
    echo "--------------------------------------------------------------------------------"
    go run cmd/distil-classify/main.go \
        --rest-endpoint="$REST_ENDPOINT" \
        --classification-function="$CLASSIFICATION_FUNCTION" \
        --dataset="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN/$MERGED_FILE" \
        --output="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN/$OUTPUT"
done

# stop classification REST API container
docker stop classification_rest
