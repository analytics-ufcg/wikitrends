#!/bin/bash

while [ -f /tmp/.gabdi.lock ]; do
echo "waiting"
sleep 1
done
echo "done"
touch /tmp/.gabdi.lock
last=`hdfs dfs -ls /user/ubuntu/speed_tmp/ | tail -1 | awk '{print $8}'`
hdfs dfs -getmerge ${last} > /tmp/absolute-realtime.tsv
sudo bash /home/ubuntu/gabdi/wikitrends/scripts/merge_layers.sh /tmp/ /var/www/wikitrends/data/
rm /tmp/.gabdi.lock