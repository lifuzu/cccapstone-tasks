from __future__ import print_function

import sys
import signal

appName = "group3-2"

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row, DataFrame

import heapq
 
def takeOrderedByKey(self, num, sortValue = None, reverse=False):
    def init(a):
        return [a]
    def combine(agg, a):
        agg.append(a)
        return getTopN(agg)
    def merge(a, b):
        agg = a + b
        return getTopN(agg)
    def getTopN(agg):
        if reverse == True:
            return heapq.nlargest(num, agg, sortValue)
        else:
            return heapq.nsmallest(num, agg, sortValue)              

    return self.combineByKey(init, combine, merge)

from pyspark.rdd import RDD
RDD.takeOrderedByKey = takeOrderedByKey
#DataFrame.takeOrderedByKey = takeOrderedByKey

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
            delays= parts.map(lambda w: Row(flight_date=w[0], origin=w[1], dest=w[2], delay=float(w[3])))
            dataFrame = sqlContext.createDataFrame(delays)
            # Register as table
            dataFrame.registerTempTable("origin_dest_delays")
            # Do word count on table using SQL and print it
            delays_df = \
                sqlContext.sql("SELECT origin, dest, flight_date, avg(delay) AS avg_delay FROM origin_dest_delays GROUP BY origin, dest, flight_date")
            #carrier_delays_df.registerTempTable("origin_carrier_avg_delays")
            #carrier_avg_delays_df = \
            #    sqlContext.sql("SELECT origin, carrier, avg_delay FROM origin_carrier_avg_delays GROUP BY origin ORDER BY avg_delay LIMIT 10")
            #for i in carrier_delays_df.rdd.takeOrderedByKey(10, sortValue=lambda x: x[2], reverse=False).map(lambda x: x[1]).collect():
            #    print (i)
            delays_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options( table = "task2_part2_group3_2", keyspace = "mykeyspace") \
                .save(mode="append")
            #delays_df.show()
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
        print("Usage: kafka_group3_2.py <zk> <topic>", file=sys.stderr)
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

