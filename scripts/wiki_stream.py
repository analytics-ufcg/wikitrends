#!/usr/bin/python
# -*- coding: utf-8 -*-
import socketIO_client
import os
import sys
import logging
import math
import argparse

from hdfs import InsecureClient as HDFSClient

BASE_DIR = os.path.join('/', 'user', "ubuntu")

INPUT_PATH = os.path.join('dataset', 'data.json')
DATASET_PATH = os.path.join(BASE_DIR, INPUT_PATH)

logger = logging.getLogger('streaming_application')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('streaming.log')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

logger.addHandler(fh)


class WikiNamespace(socketIO_client.BaseNamespace):

    def __init__(self, *args, **kwargs):
        super(WikiNamespace, self).__init__(*args, **kwargs)
        self.buffer = ""
        self.hdfs_client = HDFSClient(url=namenode_address, user=hdfs_user)

    def on_change(self, change):
        if sys.getsizeof(self.buffer.strip()) > buffer_size:
            logger.info('Copying %fMB (%i Bytes) to HDFS...' % (sys.getsizeof(
                self.buffer.strip()) * math.pow(10, -6),
                sys.getsizeof(self.buffer.strip())))
            self.hdfs_client.write(
                hdfs_path=DATASET_PATH, data=self.buffer.strip(), append=True)
            logger.info('Copy complete!')

            self.buffer = ""
        else:
            self.buffer += "%s\n" % change

    def on_connect(self):
        self.emit('subscribe', '*')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WikiTrends Streaming")
    parser.add_argument('namenode_address', help="The HDFS namenode address")
    parser.add_argument('buffer_size', help="The buffer size that has to be \
                        reached in the filesystem to start the HDFS copy")
    parser.add_argument('hdfs_user', help="The HDFS user")
    args = parser.parse_args()

    namenode_address = args.namenode_address
    buffer_size = int(args.buffer_size)
    hdfs_user = args.hdfs_user

    socketIO = socketIO_client.SocketIO('stream.wikimedia.org', 80)
    socketIO.define(WikiNamespace, '/rc')

    socketIO.wait()
