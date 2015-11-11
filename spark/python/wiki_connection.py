#!/usr/bin/python
# -*- coding: utf-8 -*-
import socketIO_client
import os
import sys
import logging
import math
import argparse
import socket
import time
import json

from hdfs import InsecureClient as HDFSClient
from threading import Lock, Thread

import config

DATASET_PATH = config.WIKI_CONN_CONFIG['DATASET_PATH']

batch_logger = logging.getLogger('batch_storage')
batch_logger.setLevel(logging.DEBUG)

streaming_logger = logging.getLogger('streaming')
streaming_logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(config.LOG_CONFIG['BATCH_LOG_FILE'])
fh.setLevel(logging.DEBUG)

fh_streaming = logging.FileHandler(config.LOG_CONFIG['STREAMING_LOG_FILE'])
fh_streaming.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
fh_streaming.setFormatter(formatter)

batch_logger.addHandler(fh)
streaming_logger.addHandler(fh_streaming)

lock = Lock()


class WikiNamespace(socketIO_client.BaseNamespace):

    def __init__(self, *args, **kwargs):
        super(WikiNamespace, self).__init__(*args, **kwargs)
        self.buffer = ""
        self.streaming_buffer = ""
        self.streaming_connection = None
        self.streaming_thread = None
        self.hdfs_client = HDFSClient(url=namenode_address, user=hdfs_user)

    def __forward_buffered_streaming(self):
        streaming_logger.info('Waiting Spark consumer to connect')
        self.streaming_connection, address = s.accept()
        self.streaming_thread = None
        streaming_logger.info('Spark consumer connected')

    def on_change(self, change):
        json_change = json.dumps(change)
        try:
            self.streaming_connection.send("%s\n" % json_change)
        except:
            self.streaming_connection = None
            self.__wait_for_consumer()
        self.buffer += "%s\n" % json_change
        if sys.getsizeof(self.buffer.strip()) > buffer_size:
            batch_logger.info('Copying %fMB (%i Bytes) to HDFS...' %
                              (sys.getsizeof(self.buffer.strip()) *
                               math.pow(10, -6),
                               sys.getsizeof(self.buffer.strip())))
            with self.hdfs_client.write(DATASET_PATH, append=True) as writer:
                writer.write(self.buffer)
                writer.flush()
            batch_logger.info('Copy complete!')

            self.buffer = ""

    def on_connect(self):
        streaming_logger.info('Connected to Wikipedia')
        self.emit('subscribe', '*')
        self.__wait_for_consumer()
        
    def __wait_for_consumer(self):
        if not self.streaming_thread:
            self.streaming_thread = Thread(target=self.__forward_buffered_streaming)
            self.streaming_thread.daemon = True
            self.streaming_thread.start()



if __name__ == "__main__":
    host = config.HDFS_CONFIG['HOST']
    port = config.HDFS_CONFIG['WEB_PORT']
    namenode_address = "http://{0}:{1}".format(host, port)  #args.namenode_address
    buffer_size = config.WIKI_CONN_CONFIG['BUFFER_SIZE'] #int(args.buffer_size)
    hdfs_user = config.HDFS_CONFIG['USERNAME'] #args.hdfs_user

    # Streaming Socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((config.STREAMING_CLIENT_CONFIG['HOST'], config.STREAMING_CLIENT_CONFIG['PORT']))
    s.listen(1)

    # Wikipedia Socket
    socketIO = socketIO_client.SocketIO('stream.wikimedia.org', 80)
    socketIO.define(WikiNamespace, '/rc')
    socketIO.wait()
