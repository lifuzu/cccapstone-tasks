/*
 * HOW TO RUN: $ pig myscript.pig
 * 
 * Group 1 - 1: Rank the top 10 most popular airports by numbers of flights to/from the airport.
 *
 */

REGISTER /usr/local/pig/lib/piggybank.jar;

--%default INPUT_PATH '/cccapstone/aviation/ontime/On_Time_On_Time_Performance_2008_1.csv';
%default INPUT_PATH '/cccapstone/aviation/ontime';
%default output 'task1_part2_group1_1';
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
    Year,Quarter,Month,DayofMonth,DayOfWeek,FlightDate:datetime,UniqueCarrier:chararray,AirlineID,Carrier,TailNum,
    FlightNum,Origin:chararray,OriginCityName,OriginState,OriginStateFips,OriginStateName,OriginWac,Dest:chararray,DestCityName,DestState,
    DestStateFips,DestStateName,DestWac,CRSDepTime,DepTime:chararray,DepDelay,DepDelayMinutes:float,DepDel15,DepartureDelayGroups,DepTimeBlk,
    TaxiOut,WheelsOff,WheelsOn,TaxiIn,CRSArrTime,ArrTime:chararray,ArrDelay,ArrDelayMinutes:float,ArrDel15,ArrivalDelayGroups,
    ArrTimeBlk,Cancelled,CancellationCode,Diverted,CRSElapsedTime,ActualElapsedTime:float,AirTime,Flights,Distance,DistanceGroup,
    CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay
  );

-- FOCUS on the problem domain data
flights_ones = FOREACH raw_data GENERATE $11, 1 AS one:int;
DESCRIBE flights_ones;
--DUMP flights_ones;

-- MAPREDUCE
flights_group = GROUP flights_ones BY $0;
DESCRIBE flights_group;
--DUMP flights_group;

flights_count = FOREACH flights_group GENERATE COUNT(flights_ones.one), group;
DESCRIBE flights_count;
--DUMP flights_count;

-- SORT
airports_rank = ORDER flights_count BY $0 DESC;
DESCRIBE airports_rank;
--DUMP airports_rank;

-- LIMIT 10
airports_top_10 = LIMIT airports_rank 10;
DESCRIBE airports_top_10;
--DUMP airports_top_10;

-- STORE
STORE airports_top_10 INTO '$OUTPUT_PATH';
