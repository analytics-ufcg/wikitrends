#!/usr/bin/python
# -*- coding: utf-8 -*-

import ast
import os
import csv
import sys
import collections
import argparse

from pyspark.sql import Row
from pyspark import SparkContext
from hdfs import InsecureClient as HDFSClient

BASE_DIR = os.path.join('/', 'data')

OUTPUT_PATH = os.path.join(BASE_DIR, 'serving')

SERVER_OUTPUT = os.path.join(OUTPUT_PATH, 'idioms.tsv')
SERVER_HEADER = ["server","count"]

PAGES_OUTPUT = os.path.join(OUTPUT_PATH, 'pages.tsv')
PAGES_HEADER = ["page", "count"]

USERS_OUTPUT = os.path.join(OUTPUT_PATH, 'editors.tsv')
USERS_HEADER = ["user", "count"]

ABSOLUTE_OUTPUT = os.path.join(OUTPUT_PATH, 'absolute.tsv')
ABSOLUTE_HEADER = ["field", "count"]

def _get_edit_length(length_value):
    if not length_value:
        return 0
    return length_value

def parse_wiki_edit(edit_entry):
    try:
        parsed_data = ast.literal_eval(edit_entry)
        return (Row(
            edited_page = parsed_data["title"].encode("utf-8"),
            editor = parsed_data["user"].encode("utf-8"),
            bot = parsed_data["bot"],
            minor = parsed_data.get("minor", False),
            server = parsed_data["server_name"].encode("utf-8"),
            old_length = _get_edit_length(ast.literal_eval(str(parsed_data["length"]))["old"]) if parsed_data.get("length") else -1,
            new_length = _get_edit_length(ast.literal_eval(str(parsed_data["length"]))["new"]) if parsed_data.get("length") else -1
        ), 1)
    except:
        return (edit_entry, 0)


def write_output_to_file(output_file, output_headers, items, hdfs_address, hdfs_user):
    hdfs_client = HDFSClient(url=hdfs_address, user=hdfs_user)

    buffer = "%s\t%s\n" % (output_headers[0], output_headers[1])
    for item in items:
        buffer += "%s\t%i\n" % (item[0], item[1])

    hdfs_client.write(hdfs_path=output_file, data=buffer.strip(), overwrite=True)      

### Spark Jobs

def parse_edits(master_dataset):
    all_edits = (sc
                   .textFile(master_dataset)
                   .map(parse_wiki_edit)
                   .cache())

    parsed_edits = (all_edits
                   .filter(lambda entry: entry[1] == 1)
                   .map(lambda entry: entry[0])
                   .cache())

    failed_edits = (all_edits
                   .filter(lambda entry: entry[1] == 0)
                   .map(lambda entry: entry[0]))

    return parsed_edits, failed_edits

def top_pages(rdd):
    return rdd.filter(lambda edit: not (edit.edited_page.startswith("File:") or edit.edited_page.startswith("User:")))\
        .map(lambda edit: (edit.edited_page, 1))\
        .reduceByKey(lambda a,b: a+b)\
        .takeOrdered(20, lambda x: -x[1])

def top_servers(rdd):
    return rdd.filter(lambda edit: edit.server.endswith("wikipedia.org"))\
        .map(lambda edit: (edit.server,1))\
        .reduceByKey(lambda a,b: a+b)\
        .takeOrdered(20, lambda x: -x[1])

def top_editors(rdd):
    return rdd.filter(lambda edit: not edit.bot)\
        .map(lambda edit: (edit.editor,1))\
        .reduceByKey(lambda a,b: a+b)\
        .takeOrdered(20, lambda x: -x[1])

def all_edits_count(rdd):
    return rdd.count()

def minor_edits_count(rdd):
    return rdd.filter(lambda edit: edit.minor).count()

def average_change_length(rdd):
    editLength =  rdd.filter(lambda edit: edit.new_length != -1)\
        .map(lambda edit: (int(edit.new_length) - int(edit.old_length))).cache()
    totalEditLength = editLength.reduce(lambda a,b : a+b)
    return totalEditLength / editLength.count()

def distinct_pages(rdd):
    return rdd.map(lambda edit: edit.edited_page).distinct().count()

def distinct_servers(rdd):
    return rdd.filter(lambda edit: edit.server.endswith("wikipedia.org")).map(lambda edit: edit.server).distinct().count()

def distinct_editors(rdd):
    return rdd.filter(lambda edit: not edit.bot).map(lambda edit: edit.editor).distinct().count()

def absolute_data(rdd):
    absolute_data = []
    absolute_data.append(("all_edits", all_edits_count(rdd)))
    absolute_data.append(("minor_edits", minor_edits_count(rdd)))
    absolute_data.append(("average_size", average_change_length(rdd)))
    absolute_data.append(("distinct_pages", distinct_pages(rdd)))
    absolute_data.append(("distinct_editors", distinct_editors(rdd)))
    absolute_data.append(("distinct_servers", distinct_servers(rdd)))

    return absolute_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A dummy program")
    parser.add_argument('hdfs_file', help="The master dataset file")
    parser.add_argument('hdfs_address', help="The HDFS namenode address")
    parser.add_argument('hdfs_user', help="The HDFS user")
    args = parser.parse_args()

    sc = SparkContext()

    parsedEdits, failedEdits = parse_edits(args.hdfs_file)

    write_output_to_file(PAGES_OUTPUT, PAGES_HEADER, top_pages(parsedEdits), args.hdfs_address, args.hdfs_user)
    write_output_to_file(USERS_OUTPUT, USERS_HEADER, top_editors(parsedEdits), args.hdfs_address, args.hdfs_user)
    write_output_to_file(SERVER_OUTPUT, SERVER_HEADER, top_servers(parsedEdits), args.hdfs_address, args.hdfs_user)    
    write_output_to_file(ABSOLUTE_OUTPUT, ABSOLUTE_HEADER, absolute_data(parsedEdits), args.hdfs_address, args.hdfs_user)