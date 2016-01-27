/*
 * HOW TO RUN: $ pig myscript.pig
 * 
 * Group 1 - 1: Rank the top 10 most popular airports by numbers of flights to/from the airport.
 *
 */

-- LOAD data

raw_data = LOAD '/cccapstone/aviation/ontime/On_Time_On_Time_Performance_2008_1.csv' USING PigStorage(',') as (Year,Quarter,Month,DayofMonth,DayOfWeek,FlightDate,UniqueCarrier,AirlineID,Carrier,TailNum,FlightNum,Origin,OriginCityName,OriginState,OriginStateFips,OriginStateName,OriginWac,Dest,DestCityName,DestState,DestStateFips,DestStateName,DestWac,CRSDepTime,DepTime,DepDelay,DepDelayMinutes,DepDel15,DepartureDelayGroups,DepTimeBlk,TaxiOut,WheelsOff,WheelsOn,TaxiIn,CRSArrTime,ArrTime,ArrDelay,ArrDelayMinutes,ArrDel15,ArrivalDelayGroups,ArrTimeBlk,Cancelled,CancellationCode,Diverted,CRSElapsedTime,ActualElapsedTime,AirTime,Flights,Distance,DistanceGroup,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay);

-- CLEAN UP the data
data = FILTER raw_data BY Year != '"Year"';
DESCRIBE data;

-- FOCUS on the problem domain data
flights_ones = FOREACH data GENERATE $11, 1 AS one:int;
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
DUMP airports_top_10;

-- STORE
--STORE airports_top_10 INTO '/cccapstone/group1_1/airports_rank_10';
