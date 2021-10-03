import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame


class AbstractBaseDidomiJob(ABC):
    def __init__(self, spark: SparkSession, input_path: str, input_partitions: Tuple[datetime], output_path: str):
        self.log = logging.getLogger(self.__class__.__name__)
        self.spark = spark
        self.input_path = input_path
        if input_partitions is None or isinstance(input_partitions, tuple) and len(input_partitions) == 0:
            self.input_partitions = None
        else:
            self.input_partitions = input_partitions
        self.output_path = output_path

    @abstractmethod
    def do_extraction(self):
        raise NotImplementedError()

    @abstractmethod
    def do_work(self):
        raise NotImplementedError()

    @abstractmethod
    def do_save(self):
        raise NotImplementedError()

    @property
    @abstractmethod
    def input_data(self) -> DataFrame:
        raise NotImplementedError()

    @input_data.setter
    @abstractmethod
    def input_data(self, x: DataFrame):
        raise NotImplementedError()

    def _partition_datetime_to_str(self, partition: datetime):
        return partition.strftime('%Y-%m-%d-%H')

    def _filter_by_partitions(self):
        if self.input_partitions:
            self.log.info('Filtering by partitions %s', str(self.input_partitions))
            partition_filter = None
            for partition in self.input_partitions:
                if partition_filter is None:
                    partition_filter = self.input_data['datehour'] == self._partition_datetime_to_str(partition)
                else:
                    partition_filter = partition_filter | (self.input_data['datehour'] == self._partition_datetime_to_str(partition))
            self.input_data = self.input_data.filter(partition_filter)

    def _extract(self):
        self.log.info('Extracting data from %', self.input_path)
        self.do_extraction()
        self._filter_by_partitions()

    def _work(self):
        self.log.info("Executing job's main process")
        self.do_work()

    def _save(self):
        self.log.info('Saving data')
        self.do_save()

    def execute(self):
        self._extract()
        self._work()
        self._save()
