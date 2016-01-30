hadoop fs -mkdir -p /cccapstone/aviation
hadoop fs -copyFromLocal /data/source/aviation/ontime /cccapstone/aviation
hadoop fs -ls /cccapstone/aviation/ontime
