#!/usr/bin/env bash

sourceDir='/data/source/aviation/ontime/On_Time_On_Time_Performance_*'
destinDir='/data/source/aviation/cleaned/group3-2'

rm -fr $destinDir
mkdir -p $destinDir

for file in `ls $sourceDir`; do
  echo $file
  filename=`basename $file`
  echo $filename
  cut -d ',' -f 6,12,19,29 $file > $destinDir/$filename # FlightDate, Origin, Dest, DepDelayMinutes
  sed -i '1d' $destinDir/$filename                  # Get rid of the header
  sed -i -r '/(^|,)\s*(,|$)/d' $destinDir/$filename # Remove the lines include empty
done
