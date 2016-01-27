#/usr/local/hadoop/sbin/start-dfs.sh

#start-dfs.sh
#/usr/local/hadoop/sbin/start-yarn.sh

# Start history server for PIG
export HADOOP_MAPRED_LOG_DIR=/data/hadoop/logs
/usr/local/hadoop/sbin/mr-jobhistory-daemon.sh start historyserver
