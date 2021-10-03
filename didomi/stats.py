from datetime import datetime
from typing import Tuple

from pyspark.sql import functions as f
from pyspark.sql import SparkSession, DataFrame

from didomi import base, schemas


class StatsGenerator(base.AbstractBaseDidomiJob):

    def __init__(self, spark: SparkSession, input_path: str, input_partitions: Tuple[datetime], output_path: str):
        super().__init__(spark, input_path, input_partitions, output_path)
        self.normalized_consents: DataFrame = None
        self.stats: DataFrame = None

    @property
    def input_data(self) -> DataFrame:
        return self.normalized_consents

    @input_data.setter
    def input_data(self, x: DataFrame):
        self.normalized_consents = x

    def do_extraction(self):
        self.normalized_consents = self.spark.read \
            .option('basePath', self.input_path) \
            .parquet(self.input_path)

    def do_work(self):
        def _event_type(event_type: str):
            return f.col('type') == event_type
        # Create all the necessary columns
        self.stats = self.normalized_consents \
            .withColumn('pageview', f.when(_event_type('pageview'), True)) \
            .withColumn('with_consent', f.when(f.size(f.col('token_purposes_enabled')) > 0, True)) \
            .withColumn('consent_asked', f.when(_event_type('consent.asked'), True)) \
            .withColumn('consent_given', f.when(_event_type('consent.given'), True))
        # Group by datehour, domain, user_country and user_id first, to be able to get avg_pageviews_per_user
        self.stats = self.stats \
            .groupby('datehour', 'domain', 'user_country', 'user_id') \
            .agg(f.count(f.col('pageview')).alias('pageviews'),
                 f.count(f.when(f.col('pageview') & f.col('with_consent'), True)).alias('pageviews_with_consent'),
                 f.count(f.col('consent_asked')).alias('consents_asked'),
                 f.count(f.col('consent_given')).alias('consents_given'),
                 f.count(f.when(f.col('consent_given') & f.col('with_consent'), True)).alias('consents_given_with_consent'),
                 )
        # Create the final statistics
        self.stats = self.stats \
            .groupby('datehour', 'domain', 'user_country') \
            .agg(f.sum(f.col('pageviews')).alias('pageviews'),
                 f.sum(f.col('pageviews_with_consent')).alias('pageviews_with_consent'),
                 f.sum(f.col('consents_asked')).alias('consents_asked'),
                 f.sum(f.col('consents_given')).alias('consents_given'),
                 f.sum(f.col('consents_given_with_consent')).alias('consents_given_with_consent'),
                 f.avg(f.col('pageviews')).alias('avg_pageviews_per_user')
                 )

    def do_save(self):
        self.stats.write.parquet(self.output_path, mode='overwrite')

