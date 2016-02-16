"""Group1_1.py"""
from __future__ import print_function
from pyspark import SparkContext, SparkConf
import os

appName = "Group1_1"
sourceDir = "/data/source/aviation/ontime/On_Time_On_Time_Performance_*"
cleanedDir = "/data/source/aviation/cleaned/" + appName + "/"
#sourceDir = "hdfs:///cccapstone/aviation/ontime/"
#cleanedDir = "hdfs:///cccapstone/aviation/cleaned/" + appName + "/"

conf = SparkConf().setAppName(appName).setMaster("local[4]")
sc = SparkContext(conf = conf)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

#sc.textFile(sourceDir + "*.csv") \
#  .flatMap( lambda filenames: filenames.split(" ")).foreach(print)
#def handle(filename, content):
#  print(filename)

#sc.wholeTextFiles(sourceDir + "*.csv").map( lambda filename, content: filename).foreach(print) 
    #df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(sourceFile)
    #df.select('Origin', 'Dest').write.mode('overwrite').format('com.databricks.spark.csv').save(cleanedDir + filename)
    #print df.select('Origin', 'Dest').show()

