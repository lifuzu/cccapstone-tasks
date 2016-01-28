#!/usr/bin/env bash
EXPORT_FILE=$1
hadoop fs -getmerge '/cccapstone/playground/'"$EXPORT_FILE" "$EXPORT_FILE".out
