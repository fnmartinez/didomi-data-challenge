from pyspark.sql.types import StructType, TimestampType, StringType

raw_consent_schema = StructType() \
    .add('datetime', TimestampType()) \
    .add('id', StringType()) \
    .add('type', StringType()) \
    .add('domain', StringType()) \
    .add('user', StructType()
         .add('id', StringType())
         .add('country', StringType())
         .add('token', StringType())
         ) \
    .add('datehour', StringType())


normalized_consent = StructType() \
    .add('timestamp', TimestampType()) \
    .add('id', StringType()) \
    .add('type', StringType()) \
    .add('domain', StringType()) \
    .add('user_id', StringType()) \
    .add('user_country', StringType()) \
    .add('user_token', StringType()) \
    .add('datehour', StringType())
