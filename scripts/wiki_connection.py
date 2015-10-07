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

from hdfs import InsecureClient as HDFSClient
from threading import Lock, Thread

BASE_DIR = os.path.join('/', 'user', "ubuntu")

INPUT_PATH = os.path.join('dataset', 'data.json')
DATASET_PATH = os.path.join(BASE_DIR, INPUT_PATH)

# Buffer size = 1MB
batch_buffer_size = 1000000
streaming_buffer_size = 5000
namenode_address = "http://localhost:50070"
hdfs_user = "root"

batch_logger = logging.getLogger('batch_storage')
batch_logger.setLevel(logging.DEBUG)

streaming_logger = logging.getLogger('streaming')
streaming_logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('batch.log')
fh.setLevel(logging.DEBUG)

fh_streaming = logging.FileHandler('streaming.log')
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
        self.hdfs_client = HDFSClient(url=namenode_address, user=hdfs_user)

    def __forward_buffered_streaming(self):
        self.streaming_connection, address = s.accept()
        while(True):
            try:
                self.streaming_connection.send(str(self.streaming_buffer))
            except:
                self.__forward_buffered_streaming()
            streaming_logger.info("%iKB Stream sent to %s:%i!" % (
                sys.getsizeof(self.streaming_buffer.strip()) / 1000,
                address[0], address[1]))
            lock.acquire()
            self.streaming_buffer = ""
            lock.release()
            time.sleep(15)

    def on_change(self, change):
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
        else:
            self.buffer += "%s\n" % change
            lock.acquire()
            self.streaming_buffer += "%s\n" % change
            lock.release()

    def on_connect(self):
        self.emit('subscribe', '*')
        streaming_thread = Thread(target=self.__forward_buffered_streaming)
        streaming_thread.daemon = True
        streaming_thread.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WikiTrends Streaming")
    parser.add_argument('namenode_address', help="The HDFS namenode address")
    parser.add_argument('buffer_size',
                        help="The buffer size that has to be \
                        reached in the filesystem to start the HDFS copy")
    parser.add_argument('hdfs_user', help="The HDFS user")
    args = parser.parse_args()

    namenode_address = args.namenode_address
    buffer_size = int(args.buffer_size)
    hdfs_user = args.hdfs_user

    # Streaming Socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("localhost", 9999))
    s.listen(10)

    # Wikipedia Socket
    socketIO = socketIO_client.SocketIO('stream.wikimedia.org', 80)
    socketIO.define(WikiNamespace, '/rc')
    socketIO.wait()