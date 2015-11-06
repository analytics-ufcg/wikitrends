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

if __name__ == "__main__":
    sc = SparkContext()
    scc = StreamingContext(sc, config.STREAMING_CLIENT_CONFIG['WINDOW_SIZE'])

    lines_dstream = scc.socketTextStream(config.STREAMING_CLIENT_CONFIG['HOST'],
                                         config.STREAMING_CLIENT_CONFIG['PORT'])
    proc_type = 'speed_tmp'
    
    parsed_edits = lines_dstream.map(parse_json_wiki_edit)
    
    ae_rdd = parsed_edits.map(lambda edit: ("stream_all_edits", 1)).reduceByKey(lambda a, b: a+b)
    me_rdd = parsed_edits.filter(lambda edit: edit.get('minor', False)).map(lambda edit: ("stream_minor_edits", 1)).reduceByKey(lambda a, b: a+b)
    speed_rdd = ae_rdd.union(me_rdd)

    speed_rdd.saveAsTextFiles(
        "{0}/{1}/{2}".format(hdfs_user_folder, proc_type, 'absolute'), "tsv")

    scc.start()
    scc.awaitTermination()
