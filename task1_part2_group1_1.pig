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

airports_pops = FOREACH airports_group GENERATE (COUNT(airports_origin_counts.one) + COUNT(airports_dest_counts.one)), group;
DESCRIBE airports_pops;
--DUMP airports_pops;

-- SORT
airports_rank = ORDER airports_pops BY $0 DESC;
DESCRIBE airports_rank;
--DUMP airports_rank;

-- LIMIT 10
airports_top_10 = LIMIT airports_rank 10;
DESCRIBE airports_top_10;
--DUMP airports_top_10;

-- STORE
STORE airports_top_10 INTO '$OUTPUT_PATH';
