from pathlib import Path
from shutil import rmtree

import pytest

from didomi import schemas


@pytest.fixture
def statistics(spark, consent_normalizer, stats_generator):
    if Path(consent_normalizer.output_path).exists():
        rmtree(consent_normalizer.output_path)
    consent_normalizer.execute()
    if Path(stats_generator.output_path).exists():
        rmtree(stats_generator.output_path)
    stats_generator.execute()
    statistics = spark.read.parquet(stats_generator.output_path)
    return statistics


def test_stats_schema(statistics):
    assert statistics.schema == schemas.stats
