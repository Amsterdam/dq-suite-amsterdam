from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("id", "string")
    .add("bronAttribuutId", "string")
    .add("vulgraad", "long")
    .add("aantalUniekeWaardes", "long")
    .add("minWaarde", "string")
    .add("maxWaarde", "string")
    .add("topVoorkomenWaarde", "string")
    .add("dqDatum", "string")
)
