#!/usr/bin/python
# -*- coding: utf-8 -*-

import ast
import json
import os

from pyspark.sql import Row
from pyspark import SparkContext

import config

WIKIPEDIA_SPECIAL_PAGES = ("Wikipedia:", "User:", "File:", "Commons:",
                           "Wikipédia:", "Special:", "Draft:", "Wikipedysta:",
                           "Συζήτηση χρήστη:", "Vorlage:", "Talk:", "کاربر:",
                           "Portal:", "Wikipedia Diskussion:", "Usuario:",
                           "User talk:", "Template:", "Wikiprojekt:", "Benutzer:",
                           "Benutzer Diskussion:", "초안:", "Користувач:", "Utente:",
                           "Wikipedia talk:", "Обсуждение:", "Anexo:", "Википедия:")


def get_edit_length(length_value):
    if not length_value:
        return 0
    return length_value


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


def parse_edits(file):
    all_edits = sc.textFile(file).map(parse_wiki_edit)
    parsed_edits = all_edits.filter(lambda entry: entry[1] == 1).map(lambda entry: entry[0])
    failed_edits = []#all_edits.filter(lambda entry: entry[1] == 0).map(lambda entry: entry[0])

    return parsed_edits, failed_edits


def process_top_editors(rdd):
    collection = sc.parallelize(
                                rdd.map(lambda edit: (edit.editor, 1))
                                    .reduceByKey(lambda a, b: a + b)
                                    .takeOrdered(20, lambda edit: -edit[1])
                                )
    collection.saveToCassandra("top_editors_batch_view", "top_editors")



def clean_rdd(rdd):
    return rdd.filter(lambda edit: edit.server.endswith("wikipedia.org"))\
              .filter(lambda edit: not edit.bot)

if __name__ == "__main__":
    sc = SparkContext()

    parsed_edits, failed_edits = parse_edits("sample.json")
    parsed_edits = clean_rdd(parsed_edits)
    parsed_edits.cache()

    process_top_editors(parsed_edits)

