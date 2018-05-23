#!/bin/bash

DATA_DIR=~/datasets/seed_datasets_current
SCHEMA=/datasetDoc.json
OUTPUT_SCHEMA=tables/mergedDataSchema.json
OUTPUT_PATH=features/
DATASET_FOLDER_SUFFIX=_dataset
DATASETS=(26_radon_seed 32_wikiqa 60_jester 185_baseball 196_autoMpg 313_spectrometer 38_sick 1491_one_hundred_plants_margin 27_wordLevels 57_hypothyroid 299_libras_move 534_cps_85_wages 1567_poker_hand 22_handgeometry)
HAS_HEADER=1
FEATURIZE_FUNCTION=fileUpload
REST_ENDPOINT=HTTP://10.108.4.42:5002

for DATASET in "${DATASETS[@]}"
do
    echo "--------------------------------------------------------------------------------"
    echo " Featurizing $DATASET dataset"
    echo "--------------------------------------------------------------------------------"
    go run cmd/distil-featurize/main.go \
        --rest-endpoint="$REST_ENDPOINT" \
        --featurize-function="$FEATURIZE_FUNCTION" \
        --dataset="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX" \
        --media-path="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX" \
        --schema="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX/$SCHEMA" \
        --output="$DATA_DIR/${DATASET}/${DATASET}$DATASET_FOLDER_SUFFIX/$OUTPUT_PATH" \
        --has-header=$HAS_HEADER
done
