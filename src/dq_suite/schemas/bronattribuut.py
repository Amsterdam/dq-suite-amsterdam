from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("bronAttribuutId", "string")
    .add("attribuutNaam", "string")
    .add("bronTabelId", "string")
)
