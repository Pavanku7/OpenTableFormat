-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Popular OpenStorgae Format
-- MAGIC AWS - Apache Iceberg
-- MAGIC Azure - Delta Lake 
-- MAGIC GCP - Apache Hudi

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data Reading csv file

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format("csv")\
-- MAGIC .option("header",True)\
-- MAGIC .option("inferSchema",True)\
-- MAGIC .load('/FileStore/OpenTableFormat/rawdata/sales_data_first.csv')
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC '''
-- MAGIC convert the above df into delta format
-- MAGIC * we can provide path till folder level, we cant give the filename when writing into delta format '''
-- MAGIC
-- MAGIC df.write.format("delta")\
-- MAGIC     .option("path","/FileStore/OpenTableFormat/sinkdata/sales_data")\
-- MAGIC         .save()

-- COMMAND ----------


-- Even without creating a delta table , we can still query the data in table format 

select * from delta.`/FileStore/OpenTableFormat/sinkdata/sales_data`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Behind the scene of Opentable format 
-- MAGIC
-- MAGIC * It read the csv data and converted into delta format, but indeed we dont have the delta format file. instead it will convert the csv file into parquet file
-- MAGIC * In delta format it contains 2 files
-- MAGIC     1. Parquet file
-- MAGIC     2. Transaction files aka (_delata_log) basically a folder concept
-- MAGIC * The _delata_log folder contains the json files, schema checkpoint
-- MAGIC     * Basically its keep on tracking the changes you are performing on the file ( basically your transactions)
-- MAGIC     * every time you make the changes, it will keep on adding the new json files. And each time it will not store the state of the table
-- MAGIC     * if you want the latest state of the table, then it will read all the previous json files.
-- MAGIC
-- MAGIC * Note:
-- MAGIC   If there are 100 transaction,whether we need to read all the 100 json files?
-- MAGIC   No, Every 10th json files its created in delta_log it will maintain the history of the past 9 previous json file in the 10th json file. So you need the current state then it will read the 10th + 1 file (i.e 11th file)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ''' to read the content of json file'''
-- MAGIC
-- MAGIC df = spark.read.json("/FileStore/OpenTableFormat/sinkdata/sales_data/_delta_log/00000000000000000000.json")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create DELTA Table

-- COMMAND ----------


create table my_delta_table
(
  ID int,
  Name string,
  salary double
)
using delta
location '/FileStore/OpenTableFormat/sinkdata/first_delta_table'

-- COMMAND ----------

-- ''' create Schema'''

create schema bronze

-- COMMAND ----------


-- Enforcing the schema while creating the table, knows as Schema Enforcement

create table bronze.my_delta_table
(
  ID int,
  Name string,
  salary double
)
using delta
location '/FileStore/OpenTableFormat/sinkdata/my_delta_table'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Insert values into table

-- COMMAND ----------

INSERT INTO bronze.my_delta_table
VALUES 
(1, 'AAA', 1000),
(2, 'BBB', 2000)

-- COMMAND ----------

select * from bronze.my_delta_table

-- COMMAND ----------


-- Will run the sql cmd 10time just to verify whether it will create the checkpoint in the delta_log folder or not 

INSERT INTO bronze.my_delta_table
VALUES 
(1, 'AAA', 1000),
(2, 'BBB', 2000)

-- COMMAND ----------


-- create 2nd table

create table bronze.my_delta_table2
(
  ID int,
  Name string,
  salary double
)
using delta
location '/FileStore/OpenTableFormat/sinkdata/my_delta_table2'

-- COMMAND ----------

describe table extended bronze.my_delta_table2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DELETION VECTOR DISABLE

-- COMMAND ----------

-- We will trun off the deletion vector

ALTER TABLE bronze.my_delta_table2
SET TBLPROPERTIES ('delta.enableDeletionVectors' = false);

-- COMMAND ----------

INSERT INTO bronze.my_delta_table2
VALUES 
(1, 'AAA', 1000),
(2, 'BBB', 2000)

-- COMMAND ----------

INSERT INTO bronze.my_delta_table2
VALUES 
(3, 'ccc', 1000),
(4, 'ddd', 2000)

-- COMMAND ----------

select * from bronze.my_delta_table2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Update values in table

-- COMMAND ----------

update bronze.my_delta_table2
set name = 'ZZZ' where id = 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ''' to read the content of json file'''
-- MAGIC
-- MAGIC df = spark.read.json("/FileStore/OpenTableFormat/sinkdata/my_delta_table2/_delta_log/00000000000000000005.json")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DML in open table
-- MAGIC
-- MAGIC whenever you perform the DML operation on the table, it will create a new parquet file.
-- MAGIC 1. The new parquet file basically contain the changes which you are performed , before this it will copy the content from the previous parquet file and it will remove the previous file ( it is also called as TOMBSTONING) means soft delete 
-- MAGIC 2. It will keep the current state of the table/file in parquet

-- COMMAND ----------

select * from bronze.my_delta_table2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Delete the data from table
-- MAGIC

-- COMMAND ----------

delete from bronze.my_delta_table2
where id = 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ''' to read the content of json file'''
-- MAGIC
-- MAGIC df = spark.read.json("/FileStore/OpenTableFormat/sinkdata/my_delta_table2/_delta_log/00000000000000000010.json")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data Versioning & Time Travel concept
-- MAGIC
-- MAGIC As we know, if we acceidently delete the dataframe and we want to resuse the data which was recetly deleted then we can use the feature of data versioning / time travel functionalites

-- COMMAND ----------

describe history bronze.my_delta_table2

-- COMMAND ----------

-- After applying the versioning to table

select * from bronze.my_delta_table2 version as of 9

-- COMMAND ----------

-- Latest State of the table

select * from bronze.my_delta_table2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### VACCUM Command
-- MAGIC
-- MAGIC 1. Hard Deletion of the data from delta
-- MAGIC   Its the right of the person/organaization , can ask for remove the
-- MAGIC 2. It will remove the files which are older than 7 days
-- MAGIC 3. If you are performing any vaccum run, then always make sure to use the 1st DRY RUN cmd as precautionary action
-- MAGIC

-- COMMAND ----------

vacuum bronze.my_delta_table2

-- COMMAND ----------

-- If we want to know what are the deleted partition when we execute vaccum command then use the below command
  -- 1.If you created the table recently then you ill not see the list of partition data bcoz data is not older than 7 days

vacuum bronze.my_delta_table2 DRY RUN

-- COMMAND ----------

-- Set the retention period

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

-- Very Risky command to execute on table, always perform dry run before executing the VACCUM cmd. Else we cant traverse back/version back any data once its deleted, it will be permanently deleted.

VACUUM bronze.my_delta_table2 RETAIN 0 HOURS DRY RUN;

-- COMMAND ----------

-- Deleted all the parquet file which mentioned in the above cmd, and we can't traverse back or version back the data

VACUUM bronze.my_delta_table2 RETAIN 0 HOURS 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SCHEMA ENFORCEMENT & SCHEMA EVOLUTION 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format("csv")\
-- MAGIC .option("header",True)\
-- MAGIC .option("inferSchema",True)\
-- MAGIC .load('/FileStore/OpenTableFormat/rawdata/sales_data_third.csv')
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC '''
-- MAGIC convert the above df into delta format
-- MAGIC     1. we can provide path till folder level, we cant give the filename when writing into delta format 
-- MAGIC     2. whenever there is schema evolution while writing the data in delta format use the "mergeSchema = true" in option
-- MAGIC '''
-- MAGIC
-- MAGIC df.write.format("delta")\
-- MAGIC     .mode('append')\
-- MAGIC     .option("path","/FileStore/OpenTableFormat/sinkdata/sales_data")\
-- MAGIC     .option('mergeSchema', True)\
-- MAGIC         .save()

-- COMMAND ----------

-- During the read of 2 sheet we didn't have the schema changes so we have enforced the schema, but in the third sheet we see the schema evolution.
-- Note : How to identify the SCHEMA EVOLUTION ?, Just read the file, you can see the last new column name "ReturnFlag" it mention the schema evolution which returns 1. if there is no schema evolution then it will return null values.

select * from delta.`/FileStore/OpenTableFormat/sinkdata/sales_data`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### METADATA LEVEL CHANGES

-- COMMAND ----------

-- Changes applied on the metadata level not on the data level, so we can see new json files created in _delta_log file, but nothing parquet file is created

-- Adding new column "Flag"

alter table bronze.my_delta_table2
add column flag INT

-- COMMAND ----------

SELECT * FROM  bronze.my_delta_table2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ''' to read the content of json file'''
-- MAGIC
-- MAGIC df = spark.read.json("/FileStore/OpenTableFormat/sinkdata/my_delta_table2/_delta_log/00000000000000000011.json")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

-- Reordering the column names

alter table bronze.my_delta_table2
alter column id after Name

-- COMMAND ----------

select * from bronze.my_delta_table2

-- COMMAND ----------

--  Dropping the column from the table

alter table bronze.my_delta_table2
drop column flag

-- COMMAND ----------

-- when we try to drop the column from the table, we need to make some changes in the column mapping still the changes will be on metadata level

-- minReaderVersion = 2
-- minWriterVersion = 5
-- MappingMode = name

alter table BRONZE.my_delta_table2
set tblproperties (
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5',
  'delta.columnMapping.mode' = 'name'
)

-- COMMAND ----------

alter table bronze.my_delta_table2
drop column flag

-- COMMAND ----------

-- Renaming the column name

alter table bronze.my_delta_table2
rename column id to cust_id

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ''' to read the content of json file'''
-- MAGIC
-- MAGIC df = spark.read.json("/FileStore/OpenTableFormat/sinkdata/my_delta_table2/_delta_log/00000000000000000015.json")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Optimization Techniques in Delta 

-- COMMAND ----------

optimize bronze.my_delta_table2 zorder by (cust_id)

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC ''' to read the content of json file'''
-- MAGIC
-- MAGIC df = spark.read.json("/FileStore/OpenTableFormat/sinkdata/my_delta_table2/_delta_log/00000000000000000016.json")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DELETION VECTOR ENABLE
-- MAGIC

-- COMMAND ----------

create table bronze.new_delta
(
  id int,
  name string
)
using delta
location '/FileStore/OpenTableFormat/sinkdata/new_delta'

-- COMMAND ----------

insert into bronze.new_delta
values
(4,'aa'),
(5,'bb'),
(6,'cc') 


-- COMMAND ----------

update bronze.new_delta 
set name = 'ZZ' where id =6

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC '''By default the deletion vector has not been enabled , so we can manually enable it. This happened only in the community edition while in enterprise it will be automaticallly enabled'''
-- MAGIC
-- MAGIC df = spark.read.json('/FileStore/OpenTableFormat/sinkdata/new_delta/_delta_log/00000000000000000003.json')
-- MAGIC display(df)

-- COMMAND ----------

create table bronze.new_delta2
(
  id int,
  name string
)
using delta
location '/FileStore/OpenTableFormat/sinkdata/new_delta2'

-- COMMAND ----------

-- We will trun on the deletion vector

ALTER TABLE bronze.new_delta2
SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);

-- COMMAND ----------

insert into bronze.new_delta2
values
(7,'aa'),
(8,'bb'),
(9,'cc') 

-- COMMAND ----------

update bronze.new_delta2
set name = 'ZZ' where id =6

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Streaming Technique in open format

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.readStream.table('bronze.new_delta2')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df.writeStream.format("delta")\
-- MAGIC     .option("checkpointLocation","/FileStore/OpenTableFormat/sinkdata/stream_table/checkpoint")\
-- MAGIC     .trigger(processingTime='10 seconds')\
-- MAGIC     .option("path","/FileStore/OpenTableFormat/sinkdata/stream_table/")\
-- MAGIC     .toTable("bronze.streamTable")

-- COMMAND ----------

select * from bronze.streamTable

-- COMMAND ----------

