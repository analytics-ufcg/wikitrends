# -*- encoding: utf-8 -*-

from hdfs import InsecureClient as HDFSClient
import os
#import settings_local as env

HDFS_HOST = "localhost"
HDFS_PORT = 50070
HDFS_URL = "http://{0}:{1}".format(HDFS_HOST,HDFS_PORT)
HDFS_USERNAME = "guilhermemg"

print "HDFS_HOST: " + HDFS_HOST
print "HDFS_PORT: " + str(HDFS_PORT)
print "HDFS_URL: " + HDFS_URL
print "HDFS_USERNAME: " + HDFS_USERNAME

hdfs_client = HDFSClient(url=HDFS_URL, user=HDFS_USERNAME)

def read_file_from_hdfs(fileName):
	path = "/data/serving/%s.tsv" % fileName
	outFilePath = "/home/{0}/gadbi/wikitrends/tmp/".format(HDFS_USERNAME)
	print "outFilePath: " + outFilePath
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
	for i in range(1,len(lines)-1):
		newL = lines[i].split("\t")
		print "newL"
		print newL
		m[newL[0]] = newL[1]
	print "map of content: "
	print m
	return m

def read_absolute_values():
	path = "/data/serving/absolute.tsv"
	with hdfs_client.read(path) as reader:
		content = reader.read()
		entries = __parse_content(content)
		return entries
