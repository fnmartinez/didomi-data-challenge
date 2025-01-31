from pyspark.sql.types import StructType, TimestampType, StringType, ArrayType, IntegerType, DoubleType, LongType

raw_consent = StructType() \
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


raw_consent_token = StructType() \
    .add('vendors', StructType()
         .add('enabled', ArrayType(StringType()))
         .add('disabled', ArrayType(StringType()))
         )\
    .add('purposes', StructType()
         .add('enabled', ArrayType(StringType()))
         .add('disabled', ArrayType(StringType()))
         )


normalized_consent = StructType() \
    .add('datetime', TimestampType()) \
    .add('id', StringType()) \
    .add('type', StringType()) \
    .add('domain', StringType()) \
    .add('user_id', StringType()) \
    .add('user_country', StringType()) \
    .add('token_vendors_enabled', ArrayType(StringType())) \
    .add('token_vendors_disabled', ArrayType(StringType())) \
    .add('token_purposes_enabled', ArrayType(StringType())) \
    .add('token_purposes_disabled', ArrayType(StringType())) \
    .add('datehour', StringType())


stats = StructType() \
    .add('datehour', StringType()) \
    .add('domain', StringType()) \
    .add('user_country', StringType()) \
    .add('pageviews', LongType()) \
    .add('pageviews_with_consent', LongType()) \
    .add('consents_asked', LongType()) \
    .add('consents_given', LongType()) \
    .add('consents_given_with_consent', LongType()) \
    .add('avg_pageviews_per_user', DoubleType())
