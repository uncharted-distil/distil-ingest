#!/bin/bash

DATA_DIR=~/datasets/seed_datasets_current
SCHEMA=/featureDatasetDoc.json
OUTPUT_SCHEMA=mergedDatasetDoc.json
DATA_PATH=/features/features.csv
OUTPUT_PATH=tables/merged.csv
OUTPUT_PATH_HEADER=tables/merged_header.csv
DATASET_FOLDER_SUFFIX=_dataset
DATASETS=(26_radon_seed 32_wikiqa 60_jester 185_baseball 196_autoMpg 313_spectrometer 38_sick 1491_one_hundred_plants_margin 27_wordLevels 57_hypothyroid 299_libras_move 534_cps_85_wages 1567_poker_hand 22_handgeometry)
HAS_HEADER=1

for DATASET in "${DATASETS[@]}"
do
    echo "--------------------------------------------------------------------------------"
    echo " Merging $DATASET dataset"
    echo "--------------------------------------------------------------------------------"
    go run cmd/distil-merge/main.go \
        --schema="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN/$SCHEMA" \
        --data="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN/$DATA_PATH" \
        --raw-data="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN/" \
        --output-path="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN/$OUTPUT_PATH" \
        --output-path-relative="$OUTPUT_PATH" \
        --output-path-header="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN/$OUTPUT_PATH_HEADER" \
        --output-schema-path="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN/$OUTPUT_SCHEMA" \
        --has-header=$HAS_HEADER
done
