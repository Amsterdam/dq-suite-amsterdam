from pyspark.sql.types import StructType

SCHEMA = (
    StructType().add("the_string", "string").add("the_timestamp", "timestamp")
)

SCHEMA2 = (
    StructType()
    .add("voornaam", "string")
    .add("achternaam", "string")
    .add("leeftijd", "integer")
)
