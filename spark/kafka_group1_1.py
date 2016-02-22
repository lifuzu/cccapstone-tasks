from __future__ import print_function

import sys
import signal

appName = "group1-1"

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def exit_gracefully(ssc):
    print("Gracefully stopping Spark Streaming Application")
    ssc.stop(True, True)
    print("Application stopped")
    sys.exit(0)
    
def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)

def main(ssc):
    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    data = lines.map(lambda line: line.split(",")) \
        .flatMap(lambda word: [(word[0], 1), (word[1], 1)]) \
        .reduceByKey(lambda a, b: a+b) \
        .updateStateByKey(updateFunc) \
        .transform(lambda rdd: rdd.sortBy(lambda (word, count): -count))
    data.pprint()

    ssc.start()
    while True:
        res = ssc.awaitTerminationOrTimeout(10)
        if res:
            # stopped elsewhere
            print('TERMINATION !!!!!!')
            break
        else
            # still running
            pass
    exit_gracefully(ssc)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_group1_1.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    conf = SparkConf().setAppName(appName).setMaster("yarn-cluster")
    sc = SparkContext(conf = conf)
    #sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("checkpoint")

    main(ssc)
    #try:
    #    main(ssc)
    #except KeyboardInterrupt:
    #    pass
    #finally:
    #    exit_gracefully(ssc)

