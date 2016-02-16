#!/usr/bin/env bash

sourceDir='/data/source/aviation/ontime/On_Time_On_Time_Performance_*'
destinDir='/data/source/aviation/cleaned/group1-1'

rm -fr $destinDir
mkdir -p $destinDir

for file in `ls $sourceDir`; do
  echo $file
  filename=`basename $file`
  echo $filename
  cut -d ',' -f 12,19 $file > $destinDir/$filename
  sed -i '1d' $destinDir/$filename
done
