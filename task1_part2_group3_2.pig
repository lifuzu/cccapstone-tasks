/*
 * HOW TO RUN: $ pig myscript.pig
 * 
 * Group 3 - 2: Tom wants to travel from airport X to airport Z. However, Tom also wants to stop at airport Y for some sightseeing on the way. 
 * More concretely, Tom has the following requirements:
 * 1. The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y). For example, if X-Y departs January 5, 2008, Y-Z must depart January 7, 2008.
 * 2. Tom wants his flights scheduled to depart airport X before 12:00 PM local time and to depart airport Y after 12:00 PM local time.
 * 3. Tom wants to arrive at each destination with as little delay as possible (assume you know the actual delay of each flight).
 *
 */

REGISTER /usr/local/pig/lib/piggybank.jar;

%default INPUT_PATH '/cccapstone/aviation/ontime/On_Time_On_Time_Performance_2008_1.csv';
--%default INPUT_PATH '/cccapstone/aviation/ontime/On_Time_On_Time_Performance_2008_*.csv';
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

-- Fetch a portion of data for debugging
data_portion = LIMIT raw_data 10000;
DESCRIBE data_portion;
--DUMP data_portion;

-- FOCUS on the problem domain data
airports_A_leg_raw = FOREACH (
  FILTER data_portion BY DepTime <= '1200' AND
  DepDelayMinutes IS NOT null) GENERATE Origin, Dest, DepTime, ArrTime, DepDelayMinutes, ArrDelayMinutes, FlightDate;
DESCRIBE airports_A_leg_raw;
--DUMP airports_A_leg_raw;

-- DISTINCT
airports_A_leg = DISTINCT airports_A_leg_raw;

airports_B_leg_raw = FOREACH (
  FILTER data_portion BY DepTime >= '1200' AND
  DepDelayMinutes IS NOT null) GENERATE Origin, Dest, DepTime, ArrTime, DepDelayMinutes, ArrDelayMinutes, FlightDate;
DESCRIBE airports_B_leg_raw;
--DUMP airports_B_leg_raw;

-- DISTINCT
airports_B_leg = DISTINCT airports_B_leg_raw;

airports_AB_leg = JOIN airports_A_leg BY (Dest), airports_B_leg BY (Origin);
DESCRIBE airports_AB_leg;
--DUMP airports_AB_leg;

airports_AB_legs_2days = FOREACH (
  -- Some rows in ArrDelayMinutes are missing, so ignore them!
  FILTER airports_AB_leg BY DaysBetween(airports_B_leg::FlightDate, airports_A_leg::FlightDate) == (long)2
  AND
  airports_B_leg::ArrDelayMinutes IS NOT null
) GENERATE airports_A_leg::Origin, airports_A_leg::Dest, airports_B_leg::Dest,
  (airports_A_leg::DepDelayMinutes + airports_A_leg::ArrDelayMinutes +  airports_B_leg::DepDelayMinutes +  airports_B_leg::ArrDelayMinutes) AS (Delay:float),
  --airports_A_leg::DepDelayMinutes, airports_A_leg::ArrDelayMinutes, airports_B_leg::DepDelayMinutes, airports_B_leg::ArrDelayMinutes,
  ToString(airports_A_leg::FlightDate, 'dd/MM/yyyy'), ToString(airports_B_leg::FlightDate, 'dd/MM/yyyy');
DESCRIBE airports_AB_legs_2days;
--DUMP airports_AB_legs_2days;

-- DISTINCT
airports_AB_legs_2days_distinct = DISTINCT airports_AB_legs_2days;

-- RANK
airports_AB_legs_rank = RANK airports_AB_legs_2days_distinct BY $3 ASC;
DESCRIBE airports_AB_legs_rank;
--DUMP airports_AB_legs_rank;

-- LIMIT for debugging
--airports_AB_legs_rank_lmt = LIMIT airports_AB_legs_rank 100;
--DESCRIBE airports_AB_legs_rank_lmt;
--DUMP airports_AB_legs_rank_lmt;

-- STORE
STORE airports_AB_legs_rank INTO '$OUTPUT_PATH';
