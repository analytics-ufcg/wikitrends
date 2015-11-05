from pyspark.streaming import StreamingContext
from pyspark import SparkContext

import config
import wiki_trends
from wiki_trends import OutputRow

ABSOLUTE_HEADER = [("field", "count")]

WIKIPEDIA_SPECIAL_PAGES = ()

host = config.HDFS_CONFIG['HOST']
port = config.HDFS_CONFIG['PORT']
user = config.HDFS_CONFIG['USERNAME']
hdfs_user_folder = "hdfs://{0}:{1}/user/{2}".format(host, port, user)


def parse_edits_rt(dstream):
    all_edits = dstream.map(wiki_trends.parse_wiki_edit)

    parsed_edits = (all_edits
                    .filter(lambda entry: entry[1] == 1)
                    .map(lambda entry: entry[0])
                    )

    failed_edits = (all_edits
                    .filter(lambda entry: entry[1] == 0)
                    .map(lambda entry: entry[0]))

    return parsed_edits, failed_edits


def process_absolute_data_rt(parsed_edits, hdfs_user_folder):
    proc_type = 'speed_tmp'

    ae = wiki_trends.all_edits_count(parsed_edits)
    me = wiki_trends.minor_edits_count(parsed_edits)

    ae.pprint()
    me.pprint()
    
    ae_rdd = parsed_edits.map(lambda edit: ("stream_all_edits", 1)).reduceByKey(lambda a, b: a+b)
    me_rdd = parsed_edits.filter(lambda edit: edit.minor).map(lambda edit: ("stream_minor_edits", 1)).reduceByKey(lambda a, b: a+b)
    speed_rdd = ae_rdd.union(me_rdd)
    
    
    speed_rdd.saveAsTextFiles(
        "{0}/{1}/{2}".format(hdfs_user_folder, proc_type, 'absolute'), "tsv")

#    ae.saveAsTextFiles(
#        "{0}/{1}/{2}".format(hdfs_user_folder, proc_type, 'all_edits'))
#    me.saveAsTextFiles(
#        "{0}/{1}/{2}".format(hdfs_user_folder, proc_type, 'minor_edits'))


if __name__ == "__main__":
    sc = SparkContext()
    scc = StreamingContext(sc, config.STREAMING_CLIENT_CONFIG['WINDOW_SIZE'])

    lines_dstream = scc.socketTextStream(config.STREAMING_CLIENT_CONFIG['HOST'],
                                         config.STREAMING_CLIENT_CONFIG['PORT'])
    proc_type = 'speed_tmp'

    lines_dstream.map(lambda edit: ("stream_all_edits", 1)).reduceByKey(lambda a, b: a+b).saveAsTextFiles("{0}/{1}/{2}".format(hdfs_user_folder, proc_type, 'absolute'), "tsv")

    # lines_dstream.count().pprint()

    # parsed_edits, failed_edits = parse_edits_rt(lines_dstream)
    # parsed_edits.count().pprint()

    # parsed_edits = wiki_trends.clean_rdd(parsed_edits)
    # parsed_edits.count().pprint()

    # process_absolute_data_rt(parsed_edits, hdfs_user_folder)

    scc.start()
    scc.awaitTermination()
