from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("regelNaam", "string")
    .add("regelParameters", "string")
    .add("norm", "integer")
    .add("bronTabelId", "string")
    .add("attribuut", "string")
    .add("severity", "string")
)
