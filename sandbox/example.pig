--register hdfs:/udf/cassandra-all.jar;
--define CqlStorage org.apache.cassandra.hadoop.pig.CqlNativeStorage();

data = LOAD '/cccapstone/playground/example.csv' using
PigStorage(',') AS
(row_id: chararray, value1: chararray, value2: int);

data_to_insert =
FOREACH data GENERATE
TOTUPLE(
TOTUPLE('row_id',row_id)
),
TOTUPLE(value1, value2);

dump data_to_insert;
-- Example Output Schema
--(((row_id,eedaf059-8bac-42c7-af92-4bf6f4bb7945)),(free,81549682))
--(((row_id,b6660321-9c17-41d8-b795-5abc82472df1)),(free,2049834))
--
-- 1.2.8 UPDATE CMD STORE data_to_insert INTO
--‘cql://myschema/example?output_query=update example set value1 @ #,value2 @ #’ USING CqlStorage();

--Corrected for 2.0

--STORE data_to_insert INTO 'cql://mykeyspace/example?output_query=update example set value1 %3D%3F,value2 %3D%3F' USING CqlStorage();
STORE data_to_insert INTO 'cassandra://mykeyspace/example?output_query=update example set value1 %3D%3F,value2 %3D%3F' USING CassandraStorage();
