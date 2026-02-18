from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("profilingAttribuutId", "string")
    .add("bronAttribuutId", "string")
    .add("vulgraad", "double")
    .add("minWaarde", "string")
    .add("maxWaarde", "string")
    .add("aantalUniekeWaardes", "long")
    .add("topVoorkomenWaardes", "string")
    .add("dataType", "string")
    .add("dqDatum", "timestamp")
)