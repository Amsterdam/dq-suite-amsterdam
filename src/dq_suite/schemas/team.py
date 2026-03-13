from pyspark.sql.types import StructType
SCHEMA = (
    StructType()
    .add("teamId", "string")
    .add("teamNaam", "string")
    .add("teamDescription", "string")
)
