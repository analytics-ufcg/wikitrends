#!/bin/bash

source /home/ubuntu/.profile;
spark_submit --master spark://hdfs-namenode:7077 /home/ubuntu/gabdi/api2/scripts/wiki_trends.py hdfs://hdfs-namenode:9000/data/dataset/data.json http://hdfs-namenode:50070 ubuntu >> /var/log/gabdi/gabdi.log 2>&1
