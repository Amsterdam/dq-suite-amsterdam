from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("id", "string")
    .add("bronTabelId", "string")
    .add("aantalRecords", "long")
    .add("aantalNietUniekeRecords", "long")
    .add("aantalAttributen", "long")
    .add("dqDatum", "string")   
)