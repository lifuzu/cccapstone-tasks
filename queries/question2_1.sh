#!/usr/bin/env bash
FILE=question2_1
CQLSH="/usr/local/cassandra/bin/cqlsh 10.5.178.79 9042"

# IMPORT to Cassandra
$CQLSH -f $FILE.cql
