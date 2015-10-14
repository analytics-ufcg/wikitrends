#!/bin/bash

INPUT_FOLDER=$1
OUTPUT_FOLDER=$2

cp $1/*tsv $2/
cat $1/absolute-realtime.tsv | sed "s/[(),']//g" | sed 's/ /\t/g' >> $2/absolute.tsv