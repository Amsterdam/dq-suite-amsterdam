from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("teamId", "string")
    .add("profilingTabelId", "string")
    .add("bronTabelId", "string")
    .add("tabelNaam", "string")
    .add("medaillonLaag", "string")
    .add("aantalRecords", "long")
    .add("aantalNullRecords", "long")
    .add("aantalAttributen", "long")
    .add("aantalNietUniekeRecords", "long")
    .add("dqDatum", "timestamp")
)
