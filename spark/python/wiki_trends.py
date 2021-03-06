#!/usr/bin/python
# -*- coding: utf-8 -*-

import ast
import json
import os
import json

from pyspark.sql import Row
from pyspark import SparkContext

import config

BASE_DIR = os.path.join('/', 'user', config.HDFS_CONFIG['USERNAME'])
OUTPUT_PATH = os.path.join(BASE_DIR, 'serving')

SERVER_HEADER = [("server", "count")]
PAGES_HEADER = [("page", "count")]
USERS_HEADER = [("user", "count")]
ABSOLUTE_HEADER = [("field", "count")]

WIKIPEDIA_SPECIAL_PAGES = ("Wikipedia:", "User:", "File:", "Commons:",
                           "Wikipédia:", "Special:", "Draft:", "Wikipedysta:",
                           "Συζήτηση χρήστη:", "Vorlage:", "Talk:", "کاربر:",
                           "Portal:", "Wikipedia Diskussion:", "Usuario:",
                           "User talk:", "Template:", "Wikiprojekt:", "Benutzer:",
                           "Benutzer Diskussion:", "초안:", "Користувач:", "Utente:",
                           "Wikipedia talk:", "Обсуждение:", "Anexo:", "Википедия:",
                           "Wikipedia talk:", "Diskussion:", "Участник:", "Utilisateur:",
                           ":ויקיפדיה")


class OutputRow(Row):

    def __str__(self):
        return u'%s\t%s' % (self.key.decode("utf-8"), self.value)
    __repr__ = __str__


def get_edit_length(length_value):
    if not length_value:
        return 0
    return length_value


def parse_output_entry(entry):
    return (OutputRow(
        key=entry[0],
        value=entry[1]))


def parse_wiki_edit(edit_entry):
    try:
        parsed_data = ast.literal_eval(edit_entry)
        edit_length = parsed_data["length"]
        return (Row(
                edited_page=parsed_data["title"].encode("utf-8"),
                editor=parsed_data["user"].encode("utf-8"),
                bot=parsed_data["bot"],
                minor=parsed_data.get("minor", False),
                server=parsed_data["server_name"].encode("utf-8"),
                timestamp=parsed_data["timestamp"],
                old_length=(edit_length["old"] or 0) if edit_length else -1,
                new_length=(edit_length["new"] or 0) if edit_length else -1
            ), 1)
    except:
        return (edit_entry, 0)

def parse_json_wiki_edit(edit_entry):
    return json.loads(edit_entry)

def parse_edits(hdfs_user_folder):
    all_edits = sc.textFile("%s/dataset/data.json" % hdfs_user_folder).map(parse_wiki_edit)
    parsed_edits = all_edits.filter(lambda entry: entry[1] == 1).map(lambda entry: entry[0])
    failed_edits = []#all_edits.filter(lambda entry: entry[1] == 0).map(lambda entry: entry[0])

    return parsed_edits, failed_edits


def process_top_pages(rdd, hdfs_user_folder, proc_type, content=False):
    ranking = rdd.map(lambda edit: (edit.edited_page, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda pair: -pair[1])
#
    full_result = PAGES_HEADER + ranking.take(20)
    sc.parallelize(full_result).coalesce(1).map(parse_output_entry).saveAsTextFile("{0}/{1}/pages{2}".format(hdfs_user_folder, proc_type, ""))

    content_result = PAGES_HEADER + ranking.filter(lambda pair: not (pair[0].startswith(WIKIPEDIA_SPECIAL_PAGES))).take(20)
    sc.parallelize(content_result).coalesce(1).map(parse_output_entry).saveAsTextFile("{0}/{1}/pages{2}".format(hdfs_user_folder, proc_type, "_content"))


def process_top_servers(rdd, hdfs_user_folder, proc_type):
    sc.parallelize(SERVER_HEADER +
                   rdd.map(lambda edit: (edit.server, 1))
                   .reduceByKey(lambda a, b: a + b)
                   .takeOrdered(20, lambda edit: -edit[1])
                   ).coalesce(1).map(parse_output_entry)\
        .saveAsTextFile("{0}/{1}/idioms".format(hdfs_user_folder, proc_type))


def process_top_editors(rdd, hdfs_user_folder, proc_type):
    sc.parallelize(USERS_HEADER +
                   rdd.map(lambda edit: (edit.editor, 1))
                   .reduceByKey(lambda a, b: a + b)
                   .takeOrdered(20, lambda edit: -edit[1])
                   ).coalesce(1).map(parse_output_entry)\
        .saveAsTextFile("{0}/{1}/editors".format(hdfs_user_folder, proc_type))


def process_absolute_data(rdd, hdfs_user_folder, proc_type):
    absolute_data = []
    absolute_data.append(("all_edits", all_edits_count(rdd)))
    absolute_data.append(("minor_edits", minor_edits_count(rdd)))
    absolute_data.append(("average_size", average_change_length(rdd)))
    absolute_data.append(("distinct_pages", distinct_pages(rdd)))
    absolute_data.append(("distinct_editors", distinct_editors(rdd)))
    absolute_data.append(("distinct_servers", distinct_servers(rdd)))
    absolute_data.append(("origin", get_origin(rdd)))

    sc.parallelize(ABSOLUTE_HEADER + absolute_data).coalesce(1)\
        .map(parse_output_entry)\
        .saveAsTextFile("{0}/{1}/absolute".format(hdfs_user_folder, proc_type))

    return absolute_data


def all_edits_count(rdd):
    return rdd.count()


def minor_edits_count(rdd):
    return rdd.filter(lambda edit: edit.minor).count()


def average_change_length(rdd):
    editLength = rdd.filter(lambda edit: edit.new_length != -1).map(lambda edit: (int(edit.new_length) - int(edit.old_length)))
    totalEditLength = editLength.reduce(lambda a, b: a + b)
    return totalEditLength / editLength.count()


def distinct_pages(rdd):
    return rdd.map(lambda edit: edit.edited_page).distinct().count()


def distinct_servers(rdd):
    return rdd.filter(lambda edit: edit.server.endswith("wikipedia.org"))\
        .map(lambda edit: edit.server).distinct().count()


def distinct_editors(rdd):
    return rdd.filter(lambda edit: not edit.bot)\
        .map(lambda edit: edit.editor).distinct().count()

def get_origin(rdd):
    return rdd.first().timestamp

def clean_rdd(rdd):
    return rdd.filter(lambda edit: edit.server.endswith("wikipedia.org"))\
              .filter(lambda edit: not edit.bot)

if __name__ == "__main__":
    sc = SparkContext()

    host = config.HDFS_CONFIG['HOST']
    port = config.HDFS_CONFIG['PORT']
    user = config.HDFS_CONFIG['USERNAME']
    hdfs_user_folder = "hdfs://{0}:{1}/user/{2}".format(host, port, user)

    parsed_edits, failed_edits = parse_edits(hdfs_user_folder)
    parsed_edits = clean_rdd(parsed_edits)
    parsed_edits.cache()

    process_top_pages(parsed_edits, hdfs_user_folder, 'serving')
    process_top_editors(parsed_edits, hdfs_user_folder, 'serving')
    process_top_servers(parsed_edits, hdfs_user_folder, 'serving')

    process_absolute_data(parsed_edits, hdfs_user_folder, 'serving')
