import pytest

from didomi.cli import SparkResourceManager


@pytest.fixture
def spark(request):
    with SparkResourceManager(spark_config=None, app_name=request.node.name) as spark:
        yield spark
