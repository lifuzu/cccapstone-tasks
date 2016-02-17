from __future__ import print_function

import sys
import signal

appName = "group1-2"

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row

def exit_gracefully(ssc):
    print("Gracefully stopping Spark Streaming Application")
    ssc.stop(True, True)
    print("Application stopped")
    sys.exit(0)
    
def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def main(ssc):
    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 2})
    lines = kvs.map(lambda x: x[1])

    # Convert RDDs of the words DStream to DataFrame and run SQL query
    def process(time, rdd):
        print("========= %s =========" % str(time))
        try:
            # Get the singleton instance of SQLContext
            sqlContext = getSqlContextInstance(rdd.context)
            # Convert RDD[String] to RDD[Row] to DataFrame
            parts = rdd.map(lambda line: line.split(","))
            delays= parts.map(lambda w: Row(carrier=w[0], delay=float(w[1])))
            dataFrame = sqlContext.createDataFrame(delays)
            # Register as table
            dataFrame.registerTempTable("carrier_delays")
            # Do word count on table using SQL and print it
            carrier_delays_df = \
                sqlContext.sql("SELECT carrier, avg(delay) AS avg_delay FROM carrier_delays GROUP BY carrier ORDER BY avg_delay ASC LIMIT 10")
            carrier_delays_df.show()
        except Exception as e: print (e)
        #except:
        #    pass
    #data = lines.map(lambda line: line.split(",")) \
    #    .map(lambda word: (word[0], float(word[1])) ) \
    #    .aggregateByKey((0,0), lambda a,b: (a[0] + b, a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1])) \
    #    .mapValues(lambda v: v[0]/v[1]) \
    #    .updateStateByKey(updateFunc) \
    #    .transform(lambda rdd: rdd.sortBy(lambda (word, count): -count))
    #data.pprint()
    lines.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_group1_2.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    conf = SparkConf().setAppName(appName).setMaster("yarn-cluster")
    sc = SparkContext(conf = conf)
    #sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")
    ssc.remember(60)

    main(ssc)
    #try:
    #    main(ssc)
    #except KeyboardInterrupt:
    #    pass
    #finally:
    #    exit_gracefully(ssc)

