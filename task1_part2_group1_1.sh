#!/usr/bin/env bash
FILE=task1_part2_group1_1

# CLEAN UP the environment
rm -fr $FILE.csv*
hadoop fs -rm -f -r '/cccapstone/output/'"$FILE"

# RUN PIG script
pig -x tez -f $FILE.pig -param output=$FILE

# EXPORT to csv
hadoop fs -getmerge '/cccapstone/output/'"$FILE" "$FILE".csv

