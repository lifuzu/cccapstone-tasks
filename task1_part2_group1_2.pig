/*
 * HOW TO RUN: $ pig myscript.pig
 * 
 * Group 1 - 2: Rank the top 10 airlines by on-time arrival performance.
 *
 */

REGISTER /usr/local/pig/lib/piggybank.jar;

-- LOAD data

--2008,1,1,3,4,2008-01-03,"WN",19393,"WN","N483WN",
--"51","HOU","Houston, TX","TX","48","Texas",74,"MCO","Orlando, FL","FL",
--"12","Florida",33,"2020","2024",4.00,4.00,0.00,0,"2000-2059",
--7.00,"2031","2312",13.00,"2325","2325",0.00,0.00,0.00,0,
--"2300-2359",0.00,"",0.00,125.00,121.00,101.00,1.00,848.00,4,
--,,,,,

raw_data = 
  LOAD '/cccapstone/aviation/ontime'
  USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
  AS (
    Year,Quarter,Month,DayofMonth,DayOfWeek,FlightDate,UniqueCarrier:chararray,AirlineID,Carrier,TailNum,
    FlightNum,Origin,OriginCityName,OriginState,OriginStateFips,OriginStateName,OriginWac,Dest,DestCityName,DestState,
    DestStateFips,DestStateName,DestWac,CRSDepTime,DepTime,DepDelay,DepDelayMinutes,DepDel15,DepartureDelayGroups,DepTimeBlk,
    TaxiOut,WheelsOff,WheelsOn,TaxiIn,CRSArrTime,ArrTime,ArrDelay,ArrDelayMinutes:float,ArrDel15,ArrivalDelayGroups,
    ArrTimeBlk,Cancelled,CancellationCode,Diverted,CRSElapsedTime,ActualElapsedTime,AirTime,Flights,Distance,DistanceGroup,
    CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay
  );

--data_10 = LIMIT raw_data 10;
--DESCRIBE data_10;
--DUMP data_10;

-- FOCUS on the problem domain data
carriers_delays = FOREACH raw_data GENERATE UniqueCarrier, ArrDelayMinutes AS delay:double;
DESCRIBE carriers_delays;
--DUMP carriers_delays;

-- FILTER the null data
carriers_delays_no_nulls = FILTER carriers_delays BY $1 is not null;
DESCRIBE carriers_delays_no_nulls;
--DUMP carriers_delays_no_nulls;


-- MAPREDUCE
carriers_group = GROUP carriers_delays_no_nulls BY $0;
DESCRIBE carriers_group;
--DUMP carriers_group;

-- AVG functon in PIG failed, workaround with this solution: http://stackoverflow.com/questions/12593527/finding-mean-using-pig-or-hadoop
--carriers_delay = FOREACH carriers_group GENERATE AVG(carriers_delays_no_nulls.delay), group;
carriers_delay = FOREACH carriers_group {
  sum = SUM(carriers_delays_no_nulls.delay);
  count = COUNT(carriers_delays_no_nulls);
  GENERATE sum/count AS avg, group as id, sum as sum, count as count;
};
DESCRIBE carriers_delay;
--DUMP carriers_delay;

-- SORT
carriers_delay_rank = ORDER carriers_delay BY $0 ASC;
DESCRIBE carriers_delay_rank;
--DUMP carriers_delay_rank;

-- LIMIT 10
carriers_delay_top_10 = LIMIT carriers_delay_rank 10;
DESCRIBE carriers_delay_top_10;
--DUMP carriers_delay_top_10;

-- STORE
STORE carriers_delay_top_10 INTO '/cccapstone/group1_2/carriers_delay_rank_10_full';
