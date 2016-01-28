/*
 * HOW TO RUN: $ pig myscript.pig
 * 
 * Group 3 - 1: Does the popularity distribution of airports follow a Zipf distribution? If not, what distribution does it follow?
 *
 */

REGISTER /usr/local/pig/lib/piggybank.jar;

--%default INPUT_PATH '/cccapstone/aviation/ontime/On_Time_On_Time_Performance_2008_1.csv';
%default INPUT_PATH '/cccapstone/aviation/ontime';
%default OUTPUT_PATH '/cccapstone/output/$output';

-- LOAD data

--2008,1,1,3,4,2008-01-03,"WN",19393,"WN","N483WN",
--"51","HOU","Houston, TX","TX","48","Texas",74,"MCO","Orlando, FL","FL",
--"12","Florida",33,"2020","2024",4.00,4.00,0.00,0,"2000-2059",
--7.00,"2031","2312",13.00,"2325","2325",0.00,0.00,0.00,0,
--"2300-2359",0.00,"",0.00,125.00,121.00,101.00,1.00,848.00,4,
--,,,,,

raw_data = 
  LOAD '$INPUT_PATH'
  USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
  AS (
    Year,Quarter,Month,DayofMonth,DayOfWeek,FlightDate,UniqueCarrier:chararray,AirlineID,Carrier,TailNum,
    FlightNum,Origin:chararray,OriginCityName,OriginState,OriginStateFips,OriginStateName,OriginWac,Dest:chararray,DestCityName,DestState,
    DestStateFips,DestStateName,DestWac,CRSDepTime,DepTime,DepDelay,DepDelayMinutes:float,DepDel15,DepartureDelayGroups,DepTimeBlk,
    TaxiOut,WheelsOff,WheelsOn,TaxiIn,CRSArrTime,ArrTime,ArrDelay,ArrDelayMinutes:float,ArrDel15,ArrivalDelayGroups,
    ArrTimeBlk,Cancelled,CancellationCode,Diverted,CRSElapsedTime,ActualElapsedTime,AirTime,Flights,Distance,DistanceGroup,
    CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay
  );

-- Fetch a portion of data for debugging
data_portion = LIMIT raw_data 1000;
DESCRIBE data_portion;
--DUMP data_portion;

-- FOCUS on the problem domain data
airports_origin_counts = FOREACH raw_data GENERATE Origin, 1 AS one:int;
DESCRIBE airports_origin_counts;
--DUMP airports_origin_counts;

airports_dest_counts = FOREACH raw_data GENERATE Dest, 1 AS one:int;
DESCRIBE airports_dest_counts;
--DUMP airports_dest_counts;

-- MAPREDUCE
airports_group = COGROUP airports_origin_counts BY $0, airports_dest_counts BY $0;
DESCRIBE airports_group;
--DUMP airports_group;

airports_pops = FOREACH airports_group GENERATE LOG(COUNT(airports_origin_counts.one) + COUNT(airports_dest_counts.one)), group;
DESCRIBE airports_pops;
--DUMP airports_pops;

-- RANK
airports_rank = RANK airports_pops BY $0 DESC;
DESCRIBE airports_rank;
--DUMP airports_rank;

-- STORE
STORE airports_rank INTO '$OUTPUT_PATH';
