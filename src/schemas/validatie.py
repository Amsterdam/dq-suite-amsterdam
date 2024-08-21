from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("regelId", "string")
    .add("aantalValideRecords", "long")
    .add("aantalReferentieRecords", "long")
    .add("dqDatum", "timestamp")
    .add("dqResultaat", "string")
)
