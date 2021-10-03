from datetime import datetime
from typing import Tuple, List

from pyspark.sql import functions as f
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.types import StructType

from didomi import base, schemas


def flatten_schema(schema: StructType, prefix: str = None) -> List[Column]:
    columns = list()
    for field in schema.fields:
        column_name = f'{prefix}.{field.name}' if prefix else field.name
        if isinstance(field.dataType, StructType):
            columns.extend(flatten_schema(field.dataType, column_name))
        else:
            columns.append(f.col(column_name).alias(column_name.replace('.', '_')))
    return columns


class ConsentNormalization(base.AbstractBaseDidomiJob):

    def __init__(self, spark: SparkSession, input_path: str, input_partitions: Tuple[datetime], output_path: str):
        super().__init__(spark, input_path, input_partitions, output_path)
        self.raw_consents: DataFrame = None
        self.normalized_consents: DataFrame = None

    def do_extraction(self):
        self.raw_consents = self.spark.read \
            .option('basePath', self.input_path) \
            .json(self.input_path, schema=schemas.raw_consent_schema)

    def do_work(self):
        self.normalized_consents = self.raw_consents \
            .select(flatten_schema(schemas.raw_consent_schema)) \
            .drop_duplicates()

    def do_save(self):
        self.normalized_consents.write.parquet(self.output_path,
                                               mode='overwrite',
                                               partitionBy=['datehour'])

    @property
    def input_data(self) -> DataFrame:
        return self.raw_consents

    @input_data.setter
    def input_data(self, x: DataFrame):
        self.raw_consents = x
