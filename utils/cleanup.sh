#!/usr/bin/env bash
mkdir -p aviation/ontime

for zipfile in `find /data/hadoop/aviation/airline_ontime/{2007,2008} -name "*.zip"`; do
  echo $zipfile;
  unzip $zipfile -d aviation/ontime -x readme.html
done
