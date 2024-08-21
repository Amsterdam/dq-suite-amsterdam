-- Databricks notebook source
-- MAGIC %md
-- MAGIC schema and tables for Dataquality great expectations

-- COMMAND ----------

create schema if not exists ${catalog}.data_quality

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.data_quality.regel (
  `regelId` STRING,
  `bronTabelId` STRING,
  `bronAttribuutId` STRING)
USING delta
COMMENT 'Created by the file upload UI'
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.data_quality.bronattribuut (
  name STRING,
  `bronAttribuutId` STRING,
  `bronTabelId` STRING,
  `attribuutNaam` STRING)
USING delta
COMMENT 'Created by the file upload UI'
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
  dqDatum TIMESTAMP,
  dqResultaat STRING)
USING delta
COMMENT 'Created by the file upload UI'
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
COMMENT 'Created by the file upload UI'
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.data_quality.brontabel (
  bronTabelId STRING,
  uniekeSleutel STRING)
USING delta
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2')

-- COMMAND ----------

-- MAGIC %environment
-- MAGIC "client": "1"
-- MAGIC "base_environment": ""
