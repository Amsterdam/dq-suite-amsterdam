from pyspark.sql.types import StructType

SCHEMA = (
    StructType().add("bronTabelId", "string").add("uniekeSleutel", "string")
)
