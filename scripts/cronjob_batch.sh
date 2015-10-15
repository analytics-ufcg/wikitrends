#!/bin/bash
source /home/ubuntu/.profile;
hadoop fs -rm -r /user/ubuntu/serving/idioms /user/ubuntu/serving/pages /user/ubuntu/serving/editors /user/ubuntu/serving/absolute
batch_elapsed_time=`(time spark-submit --master spark://hdfs-namenode:7077 --total-executor-cores 4 /home/ubuntu/gabdi/wikitrends/scripts/wiki_trends.py hdfs://hdfs-namenode:9000/user/ubuntu >> /var/log/gabdi/gabdi.log 2>&1) 2>&1 | grep real | cut -f2`
while [ -f /tmp/.gabdi.lock ]; do
	echo "waiting"
	sleep 1
done
echo "done"
touch /tmp/.gabdi.lock
hdfs dfs -getmerge /user/ubuntu/serving/absolute /tmp/absolute.tsv
echo -e "batch_elapsed_time\t${batch_elapsed_time}" >> /tmp/absolute.tsv
echo -e "total_executor_cores\t4" >> /tmp/absolute.tsv
size=`hdfs dfs -ls /user/ubuntu/dataset/data.json | awk '{print $5}'`
echo -e "input_size\t${size}" >> /tmp/absolute.tsv
hdfs dfs -getmerge /user/ubuntu/serving/editors /tmp/editors.tsv
hdfs dfs -getmerge /user/ubuntu/serving/idioms /tmp/idioms.tsv
hdfs dfs -getmerge /user/ubuntu/serving/pages /tmp/pages.tsv
rm /tmp/.gabdi.lock