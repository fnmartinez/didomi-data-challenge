from datetime import datetime
from typing import Tuple, List

from pyspark.sql import functions as f
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.types import StructType

from didomi import schemas


def flatten_schema(schema: StructType, prefix: str = None) -> List[Column]:
    columns = list()
    for field in schema.fields:
        column_name = f'{prefix}.{field.name}' if prefix else field.name
        if isinstance(field.dataType, StructType):
            columns.extend(flatten_schema(field.dataType, column_name))
        else:
            columns.append(f.col(column_name).alias(column_name.replace('.', '_')))
    return columns


class ConsentNormalization:

    def __init__(self, spark: SparkSession, input_path: str, input_partitions: Tuple[datetime], output_path: str):
        self.spark = spark
        self.input_path = input_path
        if input_partitions is None or isinstance(input_partitions, tuple) and len(input_partitions) == 0:
            self.input_partitions = None
        else:
            self.input_partitions = input_partitions
        self.output_path = output_path
        self.raw_consents: DataFrame = None
        self.normalized_consents: DataFrame = None

    def _partition_datetime_to_str(self, partition: datetime):
        return partition.strftime('%Y-%m-%d-%H')

    def extract(self):
        self.raw_consents = self.spark.read \
            .option('basePath', self.input_path) \
            .json(self.input_path, schema=schemas.raw_consent_schema)
        if self.input_partitions:
            for partition in self.input_partitions:
                partition_filter = self.raw_consents['datehour'] == self._partition_datetime_to_str(partition)
                self.raw_consents = self.raw_consents.filter(partition_filter)

    def normalize(self):
        self.normalized_consents = self.raw_consents \
            .select(flatten_schema(schemas.raw_consent_schema)) \
            .drop_duplicates()

    def save(self):
        self.normalized_consents.write.parquet(self.output_path,
                                               mode='overwrite',
                                               partitionBy=['datehour'])

    def execute(self):
        self.extract()
        self.normalize()
        self.save()
