from __future__ import print_function
from pyspark.streaming import StreamingContext
from pyspark import SparkContext

import config

sc = SparkContext()
scc = StreamingContext(sc, config.STREAMING_CLIENT_CONFIG['WINDOW_SIZE'])

lines = scc.socketTextStream(config.STREAMING_CLIENT_CONFIG['HOST'], \
			     config.STREAMING_CLIENT_CONFIG['PORT'])

lines.count().pprint()

scc.start()
scc.awaitTermination()
