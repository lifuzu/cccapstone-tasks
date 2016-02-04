#!/usr/bin/env bash
FILE=task1_part2_group2_3
CQLSH="/usr/local/cassandra/bin/cqlsh 10.5.178.79 9042"

# CLEAN UP the environment
rm -fr $FILE.csv*
hadoop fs -rm -f -r '/cccapstone/output/'"$FILE"

# RUN PIG script
pig -x tez -f $FILE.pig -param output=$FILE

# EXPORT to csv
hadoop fs -getmerge '/cccapstone/output/'"$FILE" "$FILE".csv.raw

# FORMAT csv
tr -s ',' '\t' < $FILE.csv.raw | tr -d '()' > $FILE.csv

# IMPORT to Cassandra
$CQLSH -f $FILE.cql
