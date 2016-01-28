CREATE TABLE domains (row_id text PRIMARY KEY, url text, timestamp text, flag text);
COPY domains FROM 'test.csv' WITH HEADER=true;
SELECT * FROM domains;
