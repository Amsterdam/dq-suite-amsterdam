from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("regelId", "string")
    .add("identifierVeldWaarde", "string")
    .add("afwijkendeAttribuutWaarde", "string")
    .add("dqDatum", "timestamp")
)
