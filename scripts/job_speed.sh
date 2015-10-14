#!/bin/bash
source /home/ubuntu/.profile;
spark-submit --master spark://hdfs-namenode:7077 --total-executor-cores 2 --py-files /home/ubuntu/gabdi/wikitrends/scripts/wiki_trends.py,/home/ubuntu/gabdi/wikitrends/scripts/config.py /home/ubuntu/gabdi/wikitrends/scripts/streaming_client.py hdfs://hdfs-namenode:9000/user/ubuntu >> /var/log/gabdi/gabdi-streaming.log 2>&1
