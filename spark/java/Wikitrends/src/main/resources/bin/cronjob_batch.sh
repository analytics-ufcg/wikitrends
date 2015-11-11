#!/bin/bash
source /home/ubuntu/.profile;
hadoop fs -rm -r /user/ubuntu/serving_java/idioms /user/ubuntu/serving_java/pages /user/ubuntu/serving_java/pages_content /user/ubuntu/serving_java/editors /user/ubuntu/serving_java/absolute
batch_elapsed_time=`(time ./wikitrends -e /home/ubuntu/.profile -m spark://master:7077 -w 4 batch >> /var/log/gabdi/gabdi.log 2>&1) 2>&1 | grep real | cut -f2`
while [ -f /tmp/.gabdi.lock ]; do
	echo "waiting"
	sleep 1
done
echo "done"
touch /tmp/.gabdi.lock
hdfs dfs -getmerge /user/ubuntu/serving_java/absolute /tmp/absolute.tsv
echo -e "batch_elapsed_time\t${batch_elapsed_time}" >> /tmp/absolute.tsv
echo -e "total_executor_cores\t4" >> /tmp/absolute.tsv
size=`hdfs dfs -ls /user/ubuntu/dataset/data.json | awk '{print $5}'`
echo -e "input_size\t${size}" >> /tmp/absolute.tsv
hdfs dfs -getmerge /user/ubuntu/serving_java/editors /tmp/editors.tsv
hdfs dfs -getmerge /user/ubuntu/serving_java/idioms /tmp/idioms.tsv
hdfs dfs -getmerge /user/ubuntu/serving_java/pages /tmp/pages.tsv
hdfs dfs -getmerge /user/ubuntu/serving_java/pages_content /tmp/pages_content.tsv
rm /tmp/absolute-realtime.tsv
rm /tmp/.gabdi.lock
