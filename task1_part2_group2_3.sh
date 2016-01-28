#!/usr/bin/env bash
FILE=task1_part2_group2_3

# CLEAN UP the environment
rm -fr $FILE.csv*
hadoop fs -rm -r '/cccapstone/output/'"$FILE"

# RUN PIG script
pig -f $FILE.pig -param output=$FILE

# EXPORT to csv
hadoop fs -getmerge '/cccapstone/output/'"$FILE" "$FILE".csv.raw

# FORMAT csv
tr -s ',' '\t' < $FILE.csv.raw | tr -d '()' > $FILE.csv

# IMPORT to Cassandra
cqlsh -f $FILE.cql
