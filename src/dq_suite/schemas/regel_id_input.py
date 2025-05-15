from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("regelNaam", "string")
    .add("regelParameters", "string")
    .add("bronTabelId", "string")
)
