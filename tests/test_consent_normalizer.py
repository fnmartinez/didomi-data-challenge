from pathlib import Path
from shutil import rmtree

import pytest

from pyspark.sql import functions as f

from didomi import schemas


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
