#!/usr/bin/env bash

# START DFS
/usr/local/hadoop/sbin/start-dfs.sh
# START YARN
/usr/local/hadoop/sbin/start-yarn.sh

# Start history server for PIG
export HADOOP_MAPRED_LOG_DIR=/data/hadoop/logs
/usr/local/hadoop/sbin/mr-jobhistory-daemon.sh start historyserver
