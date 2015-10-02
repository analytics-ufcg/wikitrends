#!/bin/bash

source /home/ubuntu/.profile;
spark-submit --master spark://hdfs-namenode:7077 /home/ubuntu/gabdi/wikitrends/scripts/wiki_trends.py hdfs://hdfs-namenode:9000/data/dataset/data.json http://hdfs-namenode:50070 ubuntu >> /var/log/gabdi/gabdi.log 2>&1
rm -rf /tmp/*tsv
hdfs dfs -get /data/serving/* /tmp/
sudo mv /tmp/*tsv /var/www/wikitrends/data/
