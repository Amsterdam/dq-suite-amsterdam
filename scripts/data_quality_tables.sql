-- Databricks notebook source
-- MAGIC %md
-- MAGIC This script creates the schema and tables for dq-suite-amsterdam

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT "dpxx_dev"

-- COMMAND ----------

create schema if not exists ${catalog}.data_quality

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.data_quality.brondataset (
  bronDatasetId STRING,
  medaillonLaag STRING)
USING delta
COMMENT 'Deployed by dq-suite-amsterdam'
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.data_quality.brontabel (
  bronTabelId STRING,
  tabelNaam STRING,
  uniekeSleutel STRING)
USING delta
COMMENT 'Deployed by dq-suite-amsterdam'
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.data_quality.bronattribuut (
  bronAttribuutId STRING,
  bronTabelId STRING,
  attribuutNaam STRING)
USING delta
COMMENT 'Deployed by dq-suite-amsterdam'
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.data_quality.regel (
  regelId STRING,
  regelNaam STRING,
  regelParameters STRING,
  norm INT,
  bronTabelId STRING,
  attribuut STRING)
USING delta
COMMENT 'Deployed by dq-suite-amsterdam'
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.data_quality.validatie (
  regelId STRING,
  aantalValideRecords BIGINT,
  aantalReferentieRecords BIGINT,
  percentageValideRecords DOUBLE,
  dqDatum TIMESTAMP,
  dqResultaat STRING)
USING delta
COMMENT 'Deployed by dq-suite-amsterdam'
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.data_quality.afwijking (
  regelId STRING,
  identifierVeldWaarde STRING,
  afwijkendeAttribuutWaarde STRING,
  dqDatum TIMESTAMP)
USING delta
COMMENT 'Deployed by dq-suite-amsterdam'
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
