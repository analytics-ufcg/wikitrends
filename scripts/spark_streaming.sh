#!/bin/bash
source /home/ubuntu/.profile;
hadoop fs -rm -r /user/ubuntu/serving/idioms /user/ubuntu/serving/pages /user/ubuntu/serving/editors /user/ubuntu/serving/absolute
spark-submit --master spark://hdfs-namenode:7077 /home/ubuntu/gabdi/wikitrends/scripts/streaming_client.py hdfs://hdfs-namenode:9000/user/ubuntu >> /var/log/gabdi/gabdi-streaming.log 2>&1
while [ -f /tmp/.gabdi.lock ]; do
	echo "waiting"
	sleep 1
done
echo "done"
touch /tmp/.gabdi.lock
hdfs dfs -cat /user/ubuntu/serving/absolute-realtime/part-00000 > /tmp/absolute-realtime.tsv
sudo bash /user/ubuntu/gabdi/wikitrends/scripts/merge_layers.sh /tmp/ /var/www/wikitrends/data/
rm /tmp/.gabdi.lock