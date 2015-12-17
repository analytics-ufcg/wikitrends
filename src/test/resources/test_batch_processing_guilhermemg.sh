#!/bin/bash

# script must be executed from /target directory

# unzip dist ------------------------------------------------------------------------------------------
## unzip wikitrends-2.0-dist.zip ; cd wikitrends

echo
echo

# drop master dataset schema ------------------------------------------------------------------------
./wikitrends master_dataset drop 

echo
echo

# create master dataset schema ------------------------------------------------------------------------
./wikitrends master_dataset create 

echo
echo

# migrate/populate data from json file to master dataset -------------------------------------------------------
./wikitrends -m spark://guilhermemg-laptop:7077 -w 2 master_dataset populate ../../src/test/resources/big_test_data2.json

echo
echo

# drop serving layer 1 and 2 schemas -----------------------------------------------------------------------
./wikitrends serving_1 dropschema 
./wikitrends serving_2 dropschema 

echo
echo

# create serving layer 1 and 2 schemas -----------------------------------------------------------------------
./wikitrends serving_1 createschema 
./wikitrends serving_2 createschema 

echo
echo

# execute batch layer 1 jobs --------------------------------------------------------------------
./wikitrends -m spark://guilhermemg-laptop:7077 -w 2 batch_1 TOP_IDIOMS_BATCH_1
./wikitrends -m spark://guilhermemg-laptop:7077 -w 2 batch_1 TOP_EDITORS_BATCH_1
./wikitrends -m spark://guilhermemg-laptop:7077 -w 2 batch_1 TOP_PAGES_BATCH_1
./wikitrends -m spark://guilhermemg-laptop:7077 -w 2 batch_1 TOP_CONTENT_PAGES_BATCH_1
./wikitrends -m spark://guilhermemg-laptop:7077 -w 2 batch_1 ABSOLUTE_VALUES_BATCH_1

# execute batch layer 2 jobs
./wikitrends -m spark://guilhermemg-laptop:7077 -w 2 batch_2 TOP_IDIOMS_BATCH_2
./wikitrends -m spark://guilhermemg-laptop:7077 -w 2 batch_2 TOP_EDITORS_BATCH_2
./wikitrends -m spark://guilhermemg-laptop:7077 -w 2 batch_2 TOP_PAGES_BATCH_2
./wikitrends -m spark://guilhermemg-laptop:7077 -w 2 batch_2 TOP_CONTENT_PAGES_BATCH_2
./wikitrends -m spark://guilhermemg-laptop:7077 -w 2 batch_2 ABSOLUTE_VALUES_BATCH_2

