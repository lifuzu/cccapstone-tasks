#!/usr/bin/env bash
FILE=task1_part2_group2_3

pig -f $FILE.pig -param output=$FILE

hadoop fs -rm -r '/cccapstone/output/'"$FILE"
rm -fr $FILE.csv*

hadoop fs -getmerge '/cccapstone/output/'"$FILE" "$FILE".csv.raw

tr -s ',' '\t' < $FILE.csv.raw | tr -d '()' > $FILE.csv

cqlsh -f $FILE.cql
