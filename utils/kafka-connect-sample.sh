#!/usr/bin/env bash

export KAFKA_HOME=/usr/local/kafka


export LOG_DIR=/data/hadoop/logs
export KAFKA_LOG4J_OPTS="-Dkafka.logs.dir=$LOG_DIR $KAFKA_LOG4J_OPTS"


$KAFKA_HOME/bin/connect-standalone.sh $KAFKA_HOME/config/connect-standalone.properties $KAFKA_HOME/config/connect-file-source.properties $KAFKA_HOME/config/connect-file-sink.properties
