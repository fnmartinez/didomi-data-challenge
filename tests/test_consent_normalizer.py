from datetime import datetime
from pathlib import Path
from shutil import rmtree

import pytest

from pyspark.sql import functions as f

from didomi import etls, schemas


INPUT_PATH = str(Path.cwd() / 'data' / 'input')
OUTPUT_PATH = str(Path.cwd() / 'data' / 'consents')

CONSENT_NORMALIZER_PARTITIONS = (
    ((datetime(2021, 1, 23, 10), ), 'first_partition'),
    ((datetime(2021, 1, 23, 11), ), 'second_partition'),
    ((datetime(2021, 1, 23, 10), datetime(2021, 1, 23, 11)), 'both_partitions'),
    ((), 'all_partitions_1'),
    (None, 'all_partitions_2'),
)


@pytest.fixture(params=list(zip(*CONSENT_NORMALIZER_PARTITIONS))[0],
                ids=list(zip(*CONSENT_NORMALIZER_PARTITIONS))[1])
def consent_normalizer(spark, request):
    input_partitions = request.param
    consent_normalizer = etls.ConsentNormalization(spark=spark,
                                                   input_path=INPUT_PATH,
                                                   input_partitions=input_partitions,
                                                   output_path=OUTPUT_PATH)
    return consent_normalizer


@pytest.fixture
def normalized_consents(spark, consent_normalizer):
    if Path(consent_normalizer.output_path).exists():
        rmtree(consent_normalizer.output_path)
    consent_normalizer.execute()
    normalized_consents = spark.read \
        .option('basePath', consent_normalizer.output_path) \
        .parquet(consent_normalizer.output_path)
    return normalized_consents


def test_normalized_consents_schema(normalized_consents):
    assert normalized_consents.schema == schemas.normalized_consent


def test_consents_are_unique_by_event_id(normalized_consents):
    duplicated_normalized_consents = normalized_consents \
        .groupby('id').agg(f.count('*').alias('count')) \
        .filter(f.col('count') > 1)
    assert duplicated_normalized_consents.count() == 0
