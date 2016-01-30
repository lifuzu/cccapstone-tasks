#!/usr/bin/env bash
mkdir -p /data/source/aviation/ontime

for zipfile in `find /data/hadoop/aviation/airline_ontime/ -name "*.zip"`; do
  echo $zipfile;
  unzip $zipfile -d /data/source/aviation/ontime -x readme.html
done
