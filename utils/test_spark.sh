#!/usr/bin/env bash

$SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.SparkPi     --master yarn     --deploy-mode cluster     --driver-memory 4g     --executor-memory 2g     --executor-cores 1     $SPARK_HOME/lib/spark-examples*.jar     100
