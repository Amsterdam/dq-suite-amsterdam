from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("profilingTabelId", "string")
    .add("bronTabelId", "string")
    .add("aantalRecords", "long")
    .add("aantalNullRecords", "long")
    .add("aantalAttributen", "long")
    .add("aantalNietUniekeRecords", "long")
    .add("dqDatum", "timestamp")
)