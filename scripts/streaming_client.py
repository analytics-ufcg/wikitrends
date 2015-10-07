from __future__ import print_function
from pyspark.streaming import StreamingContext
from pyspark import SparkContext

sc = SparkContext()
scc = StreamingContext(sc, 30)

lines = scc.socketTextStream("localhost", 9999)

lines.count().pprint()

scc.start()
scc.awaitTermination()
