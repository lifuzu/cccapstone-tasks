#!/usr/bin/env bash
FILE=question2_3
CQLSH="/usr/local/cassandra/bin/cqlsh 10.5.178.79 9042"

# IMPORT to Cassandra
$CQLSH -f $FILE.cql
