from pyspark.sql.types import StructType

SCHEMA = (
    StructType()
    .add("profilingAttribuutId", "string")
    .add("profilingTabelId", "string")
    .add("bronAttribuutId", "string")
    .add("attribuutNaam", "string")
    .add("missingDataPercentage", "double")
    .add("minWaarde", "string")
    .add("maxWaarde", "string")
    .add("aantalUniekeWaardes", "long")
    .add("topVoorkomendeWaardes", "string")
    .add("dataType", "string")
    .add("dqDatum", "timestamp")
)
