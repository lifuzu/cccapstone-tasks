"""SimpleApp.py"""
from pyspark import SparkContext, SparkConf

appName = "Group1_1"
dataFile = "hdfs:///cccapstone/aviation/ontime/On_Time_On_Time_Performance_2008_1.csv"

conf = SparkConf().setAppName(appName).setMaster("yarn-cluster")
sc = SparkContext(conf = conf)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(dataFile)
df.select('Origin', 'Dest').write.mode('overwrite').format('com.databricks.spark.csv').save(dataFile + '.projected')

# Read the data from projected file
data = sc.textFile(dataFile + '.projected')

#header = data.first()                      #extract header
#data = data.filter(lambda x:x !=header)    #filter out header

data = data.map(lambda line: line.split(",")) \
           .filter(lambda line: len(line) is 2) \
           .flatMap(lambda line: [(line[0],1), (line[1], 1)])

#print data.collect()

data = data.reduceByKey(lambda a, b: a + b)

data = data.sortByKey('descending').take(10)
print data



#for x in data.collect():
#  print x
