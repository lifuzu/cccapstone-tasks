#!/usr/bin/env bash
CQLSH="/usr/local/cassandra/bin/cqlsh 10.5.178.79 9042"

FILE=task1_part2_group3_2.create

# IMPORT to Cassandra
$CQLSH -f $FILE.cql

