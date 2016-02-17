#!/usr/bin/env bash

sourceDir='/data/source/aviation/ontime/On_Time_On_Time_Performance_*'
destinDir='/data/source/aviation/cleaned/group2-3'

rm -fr $destinDir
mkdir -p $destinDir

for file in `ls $sourceDir`; do
  echo $file
  filename=`basename $file`
  echo $filename
  cut -d ',' -f 7,12,19,40 $file > $destinDir/$filename # UniqueCarrier, Origin, Dest, ArrDelayMinutes
  sed -i '1d' $destinDir/$filename                  # Get rid of the header
  sed -i -r '/(^|,)\s*(,|$)/d' $destinDir/$filename # Remove the lines include empty
done
