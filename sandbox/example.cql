--CREATE KEYSPACE mykeyspace WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1};
USE mykeyspace;
CREATE TABLE example ( row_id text PRIMARY KEY, value1 text, value2 int );
EXIT;
