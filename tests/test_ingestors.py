from pathlib import Path
import sys
path = str(Path(Path(__file__).parent.absolute()).parent.absolute())
sys.path.insert(0, path)

import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

import datetime
from unittest.mock import patch, mock_open

import pytest

from ingestors import DataIngestor, TickerDataIngestor
from writers import DataWriter


@pytest.fixture
@patch("ingestors.DataIngestor.__abstractmethods__", set())
def data_ingestor_fixture():
    return TickerDataIngestor(
            writer=DataWriter,
            ticker=["aapl"],
            interval="1d"
        )

@patch("ingestors.DataIngestor.__abstractmethods__", set())
class TestIngestors:
    def test_checkpoint_filename(self, data_ingestor_fixture):
        actual = data_ingestor_fixture._checkpoint_filename
        expected = "checkpoints/TickerDataIngestor.checkpoint"
        assert actual == expected




@pytest.mark.parametrize("api, expected", [
    [TickerDataIngestor(writer=None, ticker=['foo'], interval='bar'), DataIngestor],
])
def test_parent_class_inheritance(api, expected):
    actual = type(api).__bases__[0]
    assert actual == expected