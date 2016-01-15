#!/bin/bash

cd ~/batch/wikitrends;

./wikitrends batch_1 TOP_IDIOMS_BATCH_1;
./wikitrends batch_1 TOP_EDITORS_BATCH_1;
./wikitrends batch_1 TOP_PAGES_BATCH_1;
./wikitrends batch_1 TOP_CONTENT_PAGES_BATCH_1;
./wikitrends batch_1 ABSOLUTE_VALUES_BATCH_1;

./wikitrends batch_2 ABSOLUTE_VALUES_BATCH_2;
./wikitrends batch_2 TOP_IDIOMS_BATCH_2;
./wikitrends batch_2 TOP_EDITORS_BATCH_2;
./wikitrends batch_2 TOP_PAGES_BATCH_2;
./wikitrends batch_2 TOP_CONTENT_PAGES_BATCH_2;
