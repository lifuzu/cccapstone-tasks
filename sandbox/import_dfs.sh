#!/usr/bin/env bash
IMPORT_FILE=$1 || movies_data.csv
hadoop fs -copyFromLocal "$IMPORT_FILE" '/cccapstone/playground/'
