#!/usr/bin/env bash
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
#echo $*
$SPARK_HOME/bin/spark-submit --master yarn     --deploy-mode cluster     --driver-memory 4g     --executor-memory 2g     --executor-cores 1 --packages com.databricks:spark-csv_2.10:1.3.0 $*
#$SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.SparkPi     --master yarn     --deploy-mode cluster     --driver-memory 4g     --executor-memory 2g     --executor-cores 1     $SPARK_HOME/lib/spark-examples*.jar     100
