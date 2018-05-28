#!/bin/bash

DATA_DIR=~/datasets/seed_datasets_current
SCHEMA=/mergedDatasetDoc.json
MERGED=/tables/merged.csv
CLASSIFICATION=/classification.json
OUTPUT=importance.json
DATASETS=(26_radon_seed 32_wikiqa 60_jester 185_baseball 196_autoMpg 313_spectrometer 38_sick 1491_one_hundred_plants_margin 27_wordLevels 57_hypothyroid 299_libras_move 534_cps_85_wages 1567_poker_hand 22_handgeometry)
TYPE_SOURCE=classification
ROW_LIMIT=1000
HAS_HEADER=1
REST_ENDPOINT=HTTP://localhost:5000
RANKING_FUNCTION=pca
NUMERIC_OUTPUT_SUFFIX=_numeric.csv
DATASET_FOLDER_SUFFIX=_dataset

docker run -d --rm --name ranking_rest  -p 5000:5000 primitives.azurecr.io/http_features:0.4
./wait-for-it.sh -t 0 localhost:5000
echo "Waiting for the service to be available..."
sleep 10

for DATASET in "${DATASETS[@]}"
do
    echo "--------------------------------------------------------------------------------"
    echo " Ranking $DATASET dataset"
    echo "--------------------------------------------------------------------------------"
    go run cmd/distil-rank/main.go \
        --schema="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN/$SCHEMA" \
        --dataset="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN/$MERGED" \
        --rest-endpoint="$REST_ENDPOINT" \
        --ranking-function="$RANKING_FUNCTION" \
        --ranking-output="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN/$DATASET$NUMERIC_OUTPUT_SUFFIX" \
        --classification="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN/$CLASSIFICATION" \
        --has-header=$HAS_HEADER \
        --row-limit=$ROW_LIMIT \
        --output="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN/$OUTPUT" \
        --type-source="$TYPE_SOURCE"
done

docker stop ranking_rest
