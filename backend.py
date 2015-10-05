# -*- encoding: utf-8 -*-

from hdfs import InsecureClient as HDFSClient
import os

HDFS_HOST="hdfs-namenode"
HDFS_PORT=50070
HDFS_URL="http://{0}:{1}".format(HDFS_HOST,HDFS_PORT)
USERNAME = 'ubuntu'

hdfs_client = HDFSClient(url=HDFS_URL, user=USERNAME)

def read_file_from_hdfs(fileName):
	path = "/user/ubuntu/serving/%s.tsv" % fileName
	outFilePath = "/home/ubuntu/gabdi/wikitrends/tmp/tmp"
	with hdfs_client.read(path) as reader:
		content = reader.read()
		print "File read from HDFS with success"
		tmpPath = os.path.join(outFilePath, fileName+".tsv")
		with open(tmpPath, "wb") as f:
			f.write(content)
			print "File wrote to /tmp with success"

def __parse_content(content):
	lines = content.split("\n")
	print "lines"
	print lines
	m = {}
	for i in range(1,len(lines)):
		newL = lines[i].split("\t")
		print "newL"
		print newL
		m[newL[0]] = newL[1]
	print "map of content: "
	print m
	return m

def read_absolute_values():
	path = "/user/ubuntu/serving/absolute.tsv"
	with hdfs_client.read(path) as reader:
		content = reader.read()
		entries = __parse_content(content)
		return entries
