import json
import logging
from pathlib import Path

import pytest
from tasks.extract import extract
from tasks.validate import validate
from tasks.transform import transform
from tasks.load import load
from tasks.helpers.data_models import Metadata

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

TEST_DATE = "2022-12-01"
# TEST_DATETIME = "2022-08-14T22:45:00+00:00"
TEST_BASE_PATH = Path(__file__).parent
TEST_DATA_DIR = TEST_BASE_PATH / "test_data"
TEST_PATH = TEST_BASE_PATH / "tado_api_response.json"


@pytest.mark.extract
def test_extract():
    """Check that extract method works OK,
    Including API auth and request etc"""
    metadata = Metadata(base_path=TEST_DATA_DIR, date=TEST_DATE)
    metadata = extract(metadata)

    historic_data_paths = [Path(x.path) for x in metadata.extract.zones]
    zones_data_path = metadata.extract.zones_path

    assert [p.is_file() for p in historic_data_paths]
    assert zones_data_path.is_file()

@pytest.mark.validate
def test_validate():
    """Check that validate method works OK"""
    metadata = Metadata.parse_file(TEST_BASE_PATH / 'metadata_example.json')
    metadata = validate(metadata)


@pytest.mark.transform
def test_transform():
    """Check that transform method works OK"""
    metadata = Metadata.parse_file(TEST_BASE_PATH / 'metadata_example.json')
    metadata = transform(metadata)
    interior_csvs = [x.interior_path for x in metadata.transform.zones]
    days_csvs = [x.days_path for x in metadata.transform.zones]
    weather_csvs = [x.weather_path for x in metadata.transform.zones]
    assert [f.is_file() for f in interior_csvs]
    assert [f.is_file() for f in days_csvs]
    assert [f.is_file() for f in weather_csvs]
    assert metadata.transform.interior_all_path.is_file()
    assert metadata.transform.days_all_path.is_file()
    assert metadata.transform.weather_all_path.is_file()

@pytest.mark.transform
def test_partial_transform(tado_data, tmp_path):
    """Check that partial data is handled properly, should fail and allow retry"""
    assert False

@pytest.mark.load
def test_load():
    """Check that load method works OK"""
    metadata = Metadata.parse_file(TEST_BASE_PATH / 'metadata_example.json')
    metadata = transform(metadata)
    metadata = load(metadata)
    assert False

