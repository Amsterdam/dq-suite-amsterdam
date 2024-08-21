from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("bronTabelId", "string")
    .add("bronAttribuutId", "string")
    .add("regelId", "string")
)
