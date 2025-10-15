from pyspark.sql.types import StructType

SCHEMA = (
    StructType().add("bronDatasetId", "string").add("medaillonLaag", "string")
)
