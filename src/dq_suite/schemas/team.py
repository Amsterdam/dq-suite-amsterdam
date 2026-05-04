from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("teamId", "string")
    .add("teamName", "string")
    .add("teamDescription", "string")
)
