#!/usr/bin/env bash
FILE=question3_2
CQLSH="/usr/local/cassandra/bin/cqlsh 10.5.178.79 9042"

# IMPORT to Cassandra
$CQLSH -f $FILE.cql
