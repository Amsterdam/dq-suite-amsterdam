from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("aantalValideRecords", "long")
    .add("aantalReferentieRecords", "long")
    .add("dqDatum", "timestamp")
    .add("dqResultaat", "string")
    .add("regelNaam", "string")
    .add("regelParameters", "string")
    .add("bronTabelId", "string")
)
