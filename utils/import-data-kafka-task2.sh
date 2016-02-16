#!/usr/bin/env bash

export KAFKA_HOME=/usr/local/kafka

export LOG_DIR=/data/hadoop/logs
export KAFKA_LOG4J_OPTS="-Dkafka.logs.dir=$LOG_DIR $KAFKA_LOG4J_OPTS"

for csv in `ls /data/source/aviation/ontime/On_Time_On_Time_Performance_*.csv`; do

echo "$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list dnjplatbuild02:9092 --topic cccapstone < $csv"
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list dnjplatbuild02:9092 --topic cccapstone < $csv

done
#hadoop fs -cat /input/CLEAN_CSV/part-*| kafka-console-producer.sh --broker-list mininet:9092 --topic cleaned

