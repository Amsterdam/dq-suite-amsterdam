-- Databricks notebook source
CREATE TABLE IF NOT EXISTS dpd1_dev.data_quality.profilingTabel (
        `id` STRING,
        `bronTabelId` STRING,
        `aantalRecords` BIGINT,
        `aantalAttributen` BIGINT,
        `dqDatum` TIMESTAMP
)
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

CREATE TABLE IF NOT EXISTS dpd1_dev.data_quality.profilingAttribuut (
        `id` STRING,
        `bronAttribuutId` STRING,
        `vulgraad` BIGINT,
        `aantalUniekeWaardes` BIGINT,
        `minWaarde` STRING,
        `maxWaarde` STRING,
        `dqDatum` TIMESTAMP
)
USING delta
COMMENT 'Created by the file upload UI'
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
