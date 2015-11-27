#!/bin/bash

# script must be executed from /target directory

# unzip dist ------------------------------------------------------------------------------------------
## unzip wikitrends-2.0-dist.zip ; cd wikitrends

echo
echo

# create master dataset schema ------------------------------------------------------------------------
./wikitrends master_dataset createschema localhost

echo
echo

# migrate data from json file to master dataset -------------------------------------------------------
./wikitrends -m spark://guilhermemg-laptop:7077 -w 2 master_dataset migrate localhost  ../../src/test/resources/small_test_data.json

echo
echo

# create serving layer 1 and 2 schemas -----------------------------------------------------------------------
./wikitrends serving_1 createschema localhost
./wikitrends serving_2 createschema localhost

echo
echo

# execute batch layer 1 processing --------------------------------------------------------------------
./wikitrends -m spark://guilhermemg-laptop:7077 -w 2 batch 

