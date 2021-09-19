# Databricks notebook source
# MAGIC %md
# MAGIC #### Flight Delay ETL 
# MAGIC 
# MAGIC 1. Flights.csv
# MAGIC 2. Airlines.csv
# MAGIC 3. Airports.csv

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup access to Object Store

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://datasets@deepaksekarstorage.blob.core.windows.net",
  mount_point = "/mnt/deepaksekaradls",
  extra_configs = {"fs.azure.account.key.deepaksekarstorage.blob.core.windows.net":dbutils.secrets.get(scope = "deepakscope", key = "adlsdatasets")})

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Explore Data & Create temp views

# COMMAND ----------

# MAGIC %fs ls /mnt/deepaksekaradls/Airlines_Data/

# COMMAND ----------

airlines_df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("dbfs:/mnt/deepaksekaradls/Airlines_Data/airlines.csv"))

airlines_df.createOrReplaceTempView("airlines_tmpview")

display(airlines_df)

# COMMAND ----------

airports_df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("dbfs:/mnt/deepaksekaradls/Airlines_Data/airports.csv"))

airports_df.createOrReplaceTempView("airports_tmpview")

display(airports_df)

# COMMAND ----------

flights_df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("dbfs:/mnt/deepaksekaradls/Airlines_Data/flights.csv"))

flights_df.createOrReplaceTempView("flights_tmpview")

display(flights_df)

# COMMAND ----------

flights_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### It took 30 to 40+ secs to read 5.8 Million rows as CSV/ get a count

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Try Parquet? 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS deepak_va

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS deepak_va.flights_parquet USING PARQUET PARTITIONED BY (MONTH) LOCATION '/mnt/deepaksekaradls/Airlines_Data/Flights_Parquet' AS
# MAGIC SELECT
# MAGIC   *
# MAGIC from
# MAGIC   flights_tmpview

# COMMAND ----------

# MAGIC %fs ls /mnt/deepaksekaradls/Airlines_Data/Flights_Parquet/

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM deepak_va.flights_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM deepak_va.flights_parquet

# COMMAND ----------

# MAGIC %md
# MAGIC #### Try Delta? 
# MAGIC 
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" width=200/>
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/delta_reliability.png?raw=true" width=1000/>
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/delta_performance.png?raw=true" width=1000/>

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS deepak_va.flights_delta_bronze PARTITIONED BY (MONTH) LOCATION '/mnt/deepaksekaradls/Airlines_Data/VA/Flights_Delta/Bronze/' AS
# MAGIC SELECT
# MAGIC   *
# MAGIC from
# MAGIC   flights_tmpview

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM deepak_va.flights_delta_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM deepak_va.flights_delta_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ##### See the time difference? 
# MAGIC 
# MAGIC 30 to 40+ secs with CSV 
# MAGIC 
# MAGIC 20+ secs with parquet 
# MAGIC 
# MAGIC 2 to 8 secs with Delta
# MAGIC 
# MAGIC 
# MAGIC ##### We have just partitioned, we can improve the performance even more with Indexing & Caching

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Architecture
# MAGIC 
# MAGIC <img src="https://delta.io/wp-content/uploads/2019/04/Delta-Lake-marketecture-0423c.png" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create other Bronze Tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS deepak_va.airports_delta_bronze LOCATION '/mnt/deepaksekaradls/Airlines_Data/VA/Airports_Delta/Bronze/' AS
# MAGIC SELECT
# MAGIC   *
# MAGIC from
# MAGIC   airports_tmpview

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS deepak_va.airlines_delta_bronze LOCATION '/mnt/deepaksekaradls/Airlines_Data/VA/Airlines_Delta/Bronze/' AS
# MAGIC SELECT
# MAGIC   *
# MAGIC from
# MAGIC   airlines_tmpview

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bronze to Silver ETL
# MAGIC 
# MAGIC ##### 1. Remove rows where DEPARTURE_TIME and ARRIVAL_TIME is null
# MAGIC ##### 2. Convert SCHEDULED_DEPARTURE, SCHEDULED_ARRIVAL, DEPARTURE_TIME, ARRIVAL_TIME from int to timestamp
# MAGIC ##### 3. Add a new column to populate the name of the day of week

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### ETL Logic 1 - Remove rows where DEPARTURE_TIME and ARRIVAL_TIME is null

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ARRIVAL_TIME, count(*) FROM deepak_va.flights_delta_bronze WHERE ARRIVAL_TIME IS NULL GROUP BY ARRIVAL_TIME

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DEPARTURE_TIME, count(*) FROM deepak_va.flights_delta_bronze WHERE DEPARTURE_TIME IS NULL GROUP BY DEPARTURE_TIME

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Try to delete these from Parquet?

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DELETE FROM deepak_va.flights_parquet WHERE DEPARTURE_TIME IS NULL OR ARRIVAL_TIME IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC #### How about with the Delta Silver Table? 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let us clone the Bronze table to Silver and then perform Delete

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS deepak_va.flights_delta_silver
# MAGIC DEEP CLONE deepak_va.flights_delta_bronze
# MAGIC LOCATION '/mnt/deepaksekaradls/Airlines_Data/VA/Flights_Delta/Silver/'

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DELETE FROM deepak_va.flights_delta_silver WHERE DEPARTURE_TIME IS NULL OR ARRIVAL_TIME IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY deepak_va.flights_delta_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL Logic no 2 & 3 - Convert SCHEDULED_DEPARTURE, SCHEDULED_ARRIVAL, DEPARTURE_TIME, ARRIVAL_TIME from int to timestamp. Add a new column to populate the name of the day of week
# MAGIC ##### Creating User Defined Function to implement ETL Logic 2 & 3

# COMMAND ----------

def weirdintToTS(s):
  if s < 60:
    return s
  elif s >= 100 and s < 200:
    return (s - 40)
  elif s >= 200 and s < 300:
    return (s - 80)
  elif s >= 300 and s < 400:
    return (s - 120)
  elif s >= 400 and s < 500:
    return (s - 160)
  elif s >= 500 and s < 600:
    return (s - 200)
  elif s >= 600 and s < 700:
    return (s - 240)
  elif s >= 700 and s < 800:
    return (s - 280)
  elif s >= 800 and s < 900:
    return (s - 320)
  elif s >= 900 and s < 1000:
    return (s - 360)
  elif s >= 1000 and s < 1100:
    return (s - 400)
  elif s >= 1100 and s < 1200:
    return (s - 440)
  elif s >= 1200 and s < 1300:
    return (s - 480)
  elif s >= 1300 and s < 1400:
    return (s - 520)
  elif s >= 1400 and s < 1500:
    return (s - 560)
  elif s >= 1500 and s < 1600:
    return (s - 600)
  elif s >= 1600 and s < 1700:
    return (s - 640)
  elif s >= 1700 and s < 1800:
    return (s - 680)
  elif s >= 1800 and s < 1900:
    return (s - 720)
  elif s >= 1900 and s < 2000:
    return (s - 760)
  elif s >= 2000 and s < 2100:
    return (s - 800)
  elif s >= 2100 and s < 2200:
    return (s - 840)
  elif s >= 2200 and s < 2300:
    return (s - 880)
  elif s >= 2300 and s < 2400:
    return (s - 920)
  
spark.udf.register("fn_weirdintToTS", weirdintToTS)

# COMMAND ----------

def weekdayIntToName(s):
  if s == 1:
    return 'Sunday'
  if s == 2:
    return 'Monday'
  elif s == 3:
    return 'Tuesday'
  elif s == 4:
    return 'Wednesday'
  elif s == 5:
    return 'Thursday'
  elif s == 6:
    return 'Friday'
  elif s == 7:
    return 'Saturday'
  
spark.udf.register("fn_weekdayIntToName", weekdayIntToName)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   SCHEDULED_DEPARTURE, from_unixtime(fn_weirdintToTS(SCHEDULED_DEPARTURE) * 60,'hh:mm:ss a') as SCHEDULED_DEPARTURE_TS
# MAGIC from
# MAGIC   deepak_va.flights_delta_silver
# MAGIC limit 100

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE deepak_va.flights_delta_silver LOCATION '/mnt/deepaksekaradls/Airlines_Data/VA/Flights_Delta/Silver/' AS
# MAGIC SELECT
# MAGIC   *, from_unixtime(fn_weirdintToTS(SCHEDULED_DEPARTURE) * 60,'hh:mm:ss a') as SCHEDULED_DEPARTURE_TS, 
# MAGIC   from_unixtime(fn_weirdintToTS(SCHEDULED_ARRIVAL) * 60,'hh:mm:ss a') as SCHEDULED_ARRIVAL_TS,
# MAGIC   from_unixtime(fn_weirdintToTS(DEPARTURE_TIME) * 60,'hh:mm:ss a') as DEPARTURE_TIME_TS,
# MAGIC   from_unixtime(fn_weirdintToTS(ARRIVAL_TIME) * 60,'hh:mm:ss a') as ARRIVAL_TIME_TS,
# MAGIC   fn_weekdayIntToName(DAY_OF_WEEK) as DAY_OF_WEEK_NAME
# MAGIC from
# MAGIC   deepak_va.flights_delta_silver version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from deepak_va.flights_delta_silver limit 100

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) MERGE INTO Support
# MAGIC 
# MAGIC #### INSERT or UPDATE with Parquet: 7-step process
# MAGIC 
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC 7. Drop the temp table
# MAGIC 
# MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif)
# MAGIC 
# MAGIC #### INSERT or UPDATE with Delta Lake: 2-step process
# MAGIC 
# MAGIC With Delta Lake, inserting or updating a table is a simple 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use the `MERGE` command

# COMMAND ----------

--
%sql
MERGE INTO tableA as A
USING tableB as B
ON
A.id = B.id
WHEN MATCHED THEN
UPDATE SET A.x = B.x, A.y = B.y
WHEN NOT MATCHED THEN
INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Silver to Gold ETL
# MAGIC 
# MAGIC ##### 1. Create AIRLINES AND AIRPORT DIM Tables
# MAGIC ##### 2. Create FLIGHTS FACT Table

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://i.ibb.co/zQHhFcg/modelling.png'>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE deepak_va.airlines_delta_gold_dim
# MAGIC LOCATION '/mnt/deepaksekaradls/Airlines_Data/VA/Airlines_Delta/Gold/'
# MAGIC as select row_number() over (ORDER BY IATA_CODE) as IATA_AIRLINE_CODE_SKEY ,IATA_CODE as IATA_AIRLINE_CODE, AIRLINE,'Y' as CURRENT_RECORD,cast('1900-01-01 00:00:00'as timestamp) as START_DATE, cast(null as timestamp) as END_DATE
# MAGIC from deepak_va.airlines_delta_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from deepak_va.airlines_delta_gold_dim

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE deepak_va.airports_delta_gold_dim
# MAGIC LOCATION '/mnt/deepaksekaradls/Airlines_Data/VA/Airports_Delta/Gold/'
# MAGIC as select row_number() over (ORDER BY IATA_CODE) as IATA_AIRPORT_CODE_SKEY ,IATA_CODE as IATA_AIRPORT_CODE, AIRPORT, CITY, STATE, COUNTRY, LATITUDE, LONGITUDE,'Y' as CURRENT_RECORD,cast('1900-01-01 00:00:00'as timestamp) as START_DATE, cast(null as timestamp) as END_DATE
# MAGIC from deepak_va.airports_delta_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from deepak_va.airports_delta_gold_dim

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE deepak_va.flights_fact
# MAGIC LOCATION '/mnt/deepaksekaradls/Airlines_Data/VA/Flights_Delta/Gold/'
# MAGIC WITH flights_delta_silver_tmp AS (SELECT f.ORIGIN_AIRPORT AS ORIGIN_AIRPORT, f.DESTINATION_AIRPORT AS DESTINATION_AIRPORT, f.AIRLINE AS AIRLINE, f.YEAR AS YEAR, f.MONTH AS MONTH, f.DAY AS DAY, f.DAY_OF_WEEK_NAME AS DAY_OF_WEEK_NAME, f.SCHEDULED_DEPARTURE_TS AS SCHEDULED_DEPARTURE_TS, f.SCHEDULED_ARRIVAL_TS AS SCHEDULED_ARRIVAL_TS, f.DEPARTURE_TIME_TS AS DEPARTURE_TIME_TS, f.ARRIVAL_TIME_TS AS ARRIVAL_TIME_TS, f.DEPARTURE_DELAY AS DEPARTURE_DELAY, f.ARRIVAL_DELAY AS ARRIVAL_DELAY, f.CANCELLED AS CANCELLED, f.DIVERTED AS DIVERTED, f.CANCELLATION_REASON AS CANCELLATION_REASON, f.AIR_SYSTEM_DELAY AS AIR_SYSTEM_DELAY, f.SECURITY_DELAY AS SECURITY_DELAY, f.AIRLINE_DELAY as AIRLINE_DELAY, f.LATE_AIRCRAFT_DELAY AS LATE_AIRCRAFT_DELAY, f.WEATHER_DELAY AS WEATHER_DELAY
# MAGIC FROM deepak_va.flights_delta_silver f
# MAGIC )
# MAGIC 
# MAGIC SELECT apo.IATA_AIRPORT_CODE_SKEY AS IATA_AIRPORT_CODE_SKEY_ORIGIN, apd.IATA_AIRPORT_CODE_SKEY AS IATA_AIRPORT_CODE_SKEY_DEST, al.IATA_AIRLINE_CODE_SKEY, f.* FROM flights_delta_silver_tmp f
# MAGIC 
# MAGIC /* Get the Airport Origin SKEY record */
# MAGIC 
# MAGIC JOIN deepak_va.airports_delta_gold_dim apo
# MAGIC ON f.ORIGIN_AIRPORT = apo.IATA_AIRPORT_CODE
# MAGIC 
# MAGIC /* Get the Airport Destination SKEY record */
# MAGIC JOIN deepak_va.airports_delta_gold_dim apd
# MAGIC ON f.DESTINATION_AIRPORT = apd.IATA_AIRPORT_CODE
# MAGIC 
# MAGIC /* Get the Airline SKEY record */
# MAGIC JOIN deepak_va.airlines_delta_gold_dim al
# MAGIC ON f.AIRLINE = al.
# MAGIC IATA_AIRLINE_CODE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM deepak_va.flights_fact limit 100
