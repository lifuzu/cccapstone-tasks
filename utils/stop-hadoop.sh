#!/usr/bin/env bash

# Start history server for PIG
export HADOOP_MAPRED_LOG_DIR=/data/hadoop/logs
/usr/local/hadoop/sbin/mr-jobhistory-daemon.sh stop historyserver

# STOP YARN
/usr/local/hadoop/sbin/stop-yarn.sh
# STOP DFS
/usr/local/hadoop/sbin/stop-dfs.sh

#start-dfs.sh
#/usr/local/hadoop/sbin/start-yarn.sh

