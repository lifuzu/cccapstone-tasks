/*
 * HOW TO RUN: $ pig myscript.pig
 * 
 * Group 2 - 3: For each source-destination pair X-Y, rank the top-10 carriers in decreasing order of on-time arrival performance at Y from X.
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
    FlightNum,Origin:chararray,OriginCityName,OriginState,OriginStateFips,OriginStateName,OriginWac,Dest,DestCityName,DestState,
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
airports_carriers_delays = FOREACH raw_data GENERATE TOTUPLE(Origin, Dest), UniqueCarrier, ArrDelayMinutes AS delay:double;
DESCRIBE airports_carriers_delays;
--DUMP airports_carriers_delays;

-- FILTER the null data
airports_carriers_delays_no_nulls = FILTER airports_carriers_delays BY $2 is not null;
DESCRIBE airports_carriers_delays_no_nulls;
--DUMP airports_carriers_delays_no_nulls;


-- MAPREDUCE
airports_carriers_group = GROUP airports_carriers_delays_no_nulls BY ($0, $1);
DESCRIBE airports_carriers_group;
--DUMP airports_carriers_group;

-- AVG functon in PIG failed, workaround with this solution: http://stackoverflow.com/questions/12593527/finding-mean-using-pig-or-hadoop
--carriers_delay = FOREACH carriers_group GENERATE AVG(carriers_delays_no_nulls.delay), group;
airports_carriers_delay = FOREACH airports_carriers_group {
  sum = SUM(airports_carriers_delays_no_nulls.delay);
  count = COUNT(airports_carriers_delays_no_nulls);
  avg = ROUND_TO(sum/count, 4);
  --sorted = ORDER group BY avg DESC;
  --top = LIMIT sorted 2;
  GENERATE FLATTEN(group), avg AS avg, sum as sum, count as count;
};
DESCRIBE airports_carriers_delay;
--DUMP airports_carriers_delay;

-- RANK and LIMIT
airports_carriers_delay_group = GROUP airports_carriers_delay BY ($0);
DESCRIBE airports_carriers_delay_group;
--DUMP airports_carriers_delay_group;

airports_carriers_delay_top_10 = FOREACH airports_carriers_delay_group {
  -- NO BOTTOM function
  --result = TOP(3, 2, airports_carriers_delay);
  --GENERATE FLATTEN(result);
  sorted = ORDER airports_carriers_delay BY $2 ASC;
  top = LIMIT sorted 10;
  GENERATE FLATTEN(top);
};
DESCRIBE airports_carriers_delay_top_10;
--DUMP airports_carriers_delay_top_10;

-- STORE
STORE airports_carriers_delay_top_10 INTO '$OUTPUT_PATH';
