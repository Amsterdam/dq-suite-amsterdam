from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("bronDatasetId", "string")
    .add("bronDatasetNaam", "string")
    .add("medaillonLaag", "string")
    .add("teamId", "string")
)
