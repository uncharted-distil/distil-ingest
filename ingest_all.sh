#!/bin/bash

DATA_DIR=~/data/d3m
DATASETS=(o_185 o_196 o_313 o_38 o_4550)
ES_ENDPOINT=http://10.64.16.120:9200

for dataset in "${DATASETS[@]}"
do    
    ./distil-ingest -es-endpoint http://10.64.16.120:9200 -es-index $dataset -clear-existing -dataset-path $DATA_DIR/$dataset
done

