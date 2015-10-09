#!/bin/bash

while [ -f /tmp/.gabdi.lock ]; do
echo "waiting"
sleep 1
done
echo "done"
touch /tmp/.gabdi.lock
hdfs dfs -getmerge /user/ubuntu/serving/absolute-realtime/part-00000 > /tmp/absolute-realtime.tsv
sudo bash /user/ubuntu/gabdi/wikitrends/scripts/merge_layers.sh /tmp/ /var/www/wikitrends/data/
rm /tmp/.gabdi.lock