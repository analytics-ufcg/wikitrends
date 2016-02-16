#!/bin/bash
source /home/ubuntu/.profile;
echo " ******************* starting batch layer **********************" >> /var/log/gabdi/gabdi.log
date >> /var/log/gabdi/gabdi.log 2>&1
./wikitrends -e /home/ubuntu/.profile -m spark://master:7077 -w 3 batch idioms >> /var/log/gabdi/gabdi.log 2>&1
./wikitrends -e /home/ubuntu/.profile -m spark://master:7077 -w 3 batch pages >> /var/log/gabdi/gabdi.log 2>&1
./wikitrends -e /home/ubuntu/.profile -m spark://master:7077 -w 3 batch editors >> /var/log/gabdi/gabdi.log 2>&1
./wikitrends -e /home/ubuntu/.profile -m spark://master:7077 -w 3 batch content_pages >> /var/log/gabdi/gabdi.log 2>&1
./wikitrends -e /home/ubuntu/.profile -m spark://master:7077 -w 3 batch metrics >> /var/log/gabdi/gabdi.log 2>&1
date >> /var/log/gabdi/gabdi.log 2>&1
echo " ******************* finished batch layer **********************" >> /var/log/gabdi/gabdi.log
