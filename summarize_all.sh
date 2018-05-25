#!/bin/bash

DATA_DIR=~/datasets/seed_datasets_current
DATA_FILE=tables/merged_header.csv
OUTPUT=summary-machine.json
DATASET_FOLDER_SUFFIX=_dataset
DATASETS=(26_radon_seed 32_wikiqa 60_jester 185_baseball 196_autoMpg 313_spectrometer 38_sick 1491_one_hundred_plants_margin 27_wordLevels 57_hypothyroid 299_libras_move 534_cps_85_wages 1567_poker_hand 22_handgeometry)
REST_ENDPOINT=HTTP://10.108.4.42:5003
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
        --dataset="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN/$DATA_FILE" \
        --output="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN/$OUTPUT"
done

# stop classification REST API container
#docker stop summary_rest
