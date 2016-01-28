#!/usr/bin/env bash

# STOP YARN
/usr/local/hadoop/sbin/stop-yarn.sh
# STOP DFS
/usr/local/hadoop/sbin/stop-dfs.sh

#start-dfs.sh
#/usr/local/hadoop/sbin/start-yarn.sh

# Start history server for PIG
#export HADOOP_MAPRED_LOG_DIR=/data/hadoop/logs
#/usr/local/hadoop/sbin/mr-jobhistory-daemon.sh start historyserver
