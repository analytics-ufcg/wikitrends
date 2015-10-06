#!/bin/bash
hadoop fs -rm -r /user/ubuntu/serving/idioms /user/ubuntu/serving/pages /user/ubuntu/serving/editors /user/ubuntu/serving/absolute
spark-submit --master spark://hdfs-namenode:7077 /home/ubuntu/gabdi/wikitrends/scripts/wiki_trends.py hdfs://hdfs-namenode:9000/user/ubuntu >> /var/log/gabdi/gabdi.log 2>&1

