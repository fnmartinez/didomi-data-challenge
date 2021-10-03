import configparser
import logging

import click
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from didomi import etls, stats


DEFAULT_LOGGING_PATTERN = \
    '%(asctime)s - %(name)s - %(levelname)s - %(filename)s > %(funcName)s:%(lineno)d - %(message)s'
BASE_LOGGING_LEVEL = logging.WARNING

CONTEXT_SETTINGS = dict(auto_envvar_prefix='DIDOMI')


class SparkResourceManager:
    """
    Facility class, to allow us a clean usage of the Spark cluster across the different scripts and subcommands.
    """

    DEFAULT_CORE_NUMBER = 2
    DEFAULT_DRIVER_MEMORY = 4

    DEFAULT_SPARK_CONFIG = [
        ('spark.master', f'local[{DEFAULT_CORE_NUMBER}]'),
        ('spark.driver.memory', f'{DEFAULT_DRIVER_MEMORY}g')
    ]

    def __init__(self, spark_config, app_name):
        self.spark_config = self._load_spark_config(spark_config) or self.DEFAULT_SPARK_CONFIG
        self.app_name = app_name
        self.spark = None

    @staticmethod
    def _load_spark_config(spark_config):
        if spark_config:
            config_parser = configparser.ConfigParser()
            config_parser.read(spark_config)
            spark_config = list(config_parser['SPARK'].items())
        return spark_config

    def __enter__(self):
        conf = SparkConf() \
            .setAppName(self.app_name) \
            .setAll(self.spark_config)
        sc = SparkContext(conf=conf)
        self.spark = SparkSession(sc)
        return self.spark

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.spark.stop()


@click.group()
@click.pass_context
@click.option('-sc', '--spark-config', type=click.Path(exists=True, file_okay=True, dir_okay=False),
              help='Path to the config file for spark. See file specification in the README. '
                   'If none specified, then local mode is assumed with some sane defaults.',
              default=None)
def cli(ctx, spark_config):
    logging.basicConfig(format=DEFAULT_LOGGING_PATTERN,
                        level=BASE_LOGGING_LEVEL)
    if ctx.invoked_subcommand:
        ctx.obj = ctx.with_resource(SparkResourceManager(spark_config=spark_config,
                                                         app_name=f'hellofresh.{ctx.invoked_subcommand}'))


@cli.command('normalize')
@click.pass_obj
@click.option('-ip', '--input-path', type=click.Path(exists=True, dir_okay=True, file_okay=False),
              help='Path to the directory the raw consent data is.',
              default='./data/input')
@click.option('-pd', '--partition-date-hour', 'input_partitions', type=click.DateTime(), multiple=True,
              help='The partition(s) from the raw data to use for normalization. '
                   'If not provided, then all the partition will be used')
@click.option('-op', '--output-path', type=click.Path(exists=False, dir_okay=True, file_okay=False),
              help='Path to the directory where the normalized consent will reside.',
              default='./data/consents')
def task_1(spark, input_path, input_partitions, output_path):
    etls.ConsentNormalization(spark=spark,
                              input_path=input_path,
                              input_partitions=input_partitions,
                              output_path=output_path) \
        .execute()
