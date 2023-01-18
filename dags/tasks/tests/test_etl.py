import json
import logging
from pathlib import Path

import pytest
from tasks.extract import extract
from tasks.validate import validate
from tasks.transform import transform
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

    historic_data_paths = [Path(x.path) for x in metadata.extract.historic_data]
    zone_data_path = metadata.extract.zone_data

    assert [p.is_file() for p in historic_data_paths]
    assert zone_data_path.is_file()

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
    climate_csvs = [x.climate_path for x in metadata.transform.paths]
    days_csvs = [x.days_path for x in metadata.transform.paths]

    assert [f.is_file() for f in climate_csvs]
    assert [f.is_file() for f in days_csvs]


@pytest.mark.transform
def test_partial_transform(tado_data, tmp_path):
    """Check that partial data is handled properly, should fail and allow retry"""
    assert False
