#!/usr/bin/env bash
CQLSH="/usr/local/cassandra/bin/cqlsh 10.5.178.79 9042"

for i in $(seq 3 10); do

FILE=task1_part2_group3_2.$i

# IMPORT to Cassandra
$CQLSH -f $FILE.cql

done
