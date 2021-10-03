from datetime import datetime
from pathlib import Path

import pytest

from didomi import etls, stats
from didomi.cli import SparkResourceManager

INPUT_PATH = str(Path.cwd() / 'data' / 'input')
CONSENTS_PATH = str(Path.cwd() / 'data' / 'consents')
STATS_PATH = str(Path.cwd() / 'data' / 'stats')

PARTITIONS = (
    ((datetime(2021, 1, 23, 10), ), 'first_partition'),
    ((datetime(2021, 1, 23, 11), ), 'second_partition'),
    ((datetime(2021, 1, 23, 10), datetime(2021, 1, 23, 11)), 'both_partitions'),
    ((), 'all_partitions_1'),
    (None, 'all_partitions_2'),
)


@pytest.fixture
def spark(request):
    with SparkResourceManager(spark_config=None, app_name=request.node.name) as spark:
        yield spark


@pytest.fixture(params=list(zip(*PARTITIONS))[0],
                ids=list(zip(*PARTITIONS))[1])
def consent_normalizer(spark, request):
    input_partitions = request.param
    consent_normalizer = etls.ConsentNormalization(spark=spark,
                                                   input_path=INPUT_PATH,
                                                   input_partitions=input_partitions,
                                                   output_path=CONSENTS_PATH)
    return consent_normalizer


@pytest.fixture
def stats_generator(spark, consent_normalizer):
    stats_generator = stats.StatsGenerator(spark=spark,
                                           input_path=CONSENTS_PATH,
                                           input_partitions=consent_normalizer.input_partitions,
                                           output_path=STATS_PATH)
    return stats_generator


