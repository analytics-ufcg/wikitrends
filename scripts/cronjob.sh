#!/bin/bash
source /home/ubuntu/.profile;
hadoop fs -rm -r /user/ubuntu/serving/idioms /user/ubuntu/serving/pages /user/ubuntu/serving/editors /user/ubuntu/serving/absolute
spark-submit --master spark://hdfs-namenode:7077 /home/ubuntu/gabdi/wikitrends/scripts/wiki_trends.py hdfs://hdfs-namenode:9000/user/ubuntu >> /var/log/gabdi/gabdi.log 2>&1
while [ -f /tmp/.gabdi.lock ]; do
	echo "waiting"
	sleep 1
done
echo "done"
touch /tmp/.gabdi.lock
hdfs dfs -cat /user/ubuntu/serving/absolute/part-00000 > /tmp/absolute.tsv
hdfs dfs -cat /user/ubuntu/serving/editors/part-00000 > /tmp/editors.tsv
hdfs dfs -cat /user/ubuntu/serving/idioms/part-00000 > /tmp/idioms.tsv
hdfs dfs -cat /user/ubuntu/serving/pages/part-00000 > /tmp/pages.tsv
hdfs dfs -cat /user/ubuntu/serving/absolute-realtime/template.tsv > /tmp/absolute-realtime.tsv
rm /tmp/.gabdi.lock