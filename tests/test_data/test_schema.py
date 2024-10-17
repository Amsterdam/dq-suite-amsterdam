from pyspark.sql.types import StructType

SCHEMA = (
    StructType().add("the_string", "string").add("the_timestamp", "timestamp")
)
