from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("bronTabelId", "string")
    .add("tabelNaam", "string")
    .add("uniekeSleutel", "string")
)
