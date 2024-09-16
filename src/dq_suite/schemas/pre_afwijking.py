from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("identifierVeldWaarde", "string")
    .add("afwijkendeAttribuutWaarde", "string")
    .add("dqDatum", "timestamp")
    .add("regelNaam", "string")
    .add("regelParameters", "string")
    .add("bronTabelId", "string")
)
