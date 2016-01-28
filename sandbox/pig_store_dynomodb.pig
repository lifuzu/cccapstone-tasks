-- register the jar
REGISTER 's3n://carlogo/pig-dynamodb-0.2-SNAPSHOT.jar'
--REGISTER /home/build/3rdparty/pig-dynamodb/target/pig-dynamodb-0.2-SNAPSHOT.jar;

-- Percentage of the table's write throughput to use
-- Default: 0.5, valid Range 0.1 - 1.5
SET dynamodb.throughput.write.percent 1.0;

-- Must disable Pig's MultiQuery optimization
-- when using DynamoDBStorage
SET opt.multiquery false;

-- Dynamo table where writes should go
%default DYNAMODB_TABLE 'movies';

-- AWS access key to use for writing to DynamoDB table
%default DYNAMODB_AWS_ACCESS_KEY_ID 'AKIAIT4BN36YRQZOJSLQ';

-- AWS secret key to use for writing to DynamoDB table
%default DYNAMODB_AWS_SECRET_ACCESS_KEY 'S5nEFp8rb1nady3ls0QZxUPLlAPRW45r0GLmzWhu';

%default INPUT_PATH '/cccapstone/playground/movies_data.csv';

-- Load up some input data
input_data = LOAD '$INPUT_PATH'
    USING PigStorage(',')
    AS (id, name, year, rating, duration);

--DUMP input_data;

-- Select exactly the fields you want to store to dynamodb.
-- MUST include your DynamoDB table's primary key.
exact_fields_to_store = FOREACH input_data
    GENERATE id AS id,
             name AS name,
             year AS year,
             rating AS rating,
             duration AS duration;

-- Store the data to DynamoDB
STORE exact_fields_to_store
  INTO 's3n://carlogo/ignored'
  USING com.mortardata.pig.storage.DynamoDBStorage('$DYNAMODB_TABLE', '$DYNAMODB_AWS_ACCESS_KEY_ID', '$DYNAMODB_AWS_SECRET_ACCESS_KEY');

