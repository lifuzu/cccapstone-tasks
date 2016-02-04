#!/usr/bin/env bash

for i in $(seq 1 10); do

FILE=task1_part2_group3_2.$i

# CLEAN UP the environment
rm -fr $FILE.csv*
hadoop fs -rm -f -r '/cccapstone/output/'"$FILE"

# RUN PIG script
pig -x tez -f task1_part2_group3_2.pig -param output=$FILE -param month=$i

# EXPORT to csv
hadoop fs -getmerge '/cccapstone/output/'"$FILE" "$FILE".csv

done
# IMPORT to Cassandra
#cqlsh -f $FILE.cql
