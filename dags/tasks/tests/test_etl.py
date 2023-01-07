import json
import logging
from pathlib import Path

import pytest

from dags.tasks.extract import extract
from dags.tasks.transform import transform

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

TEST_DATE = "2022-08-15"
TEST_DATETIME = "2022-08-14T22:45:00+00:00"
TEST_PATH = Path(__file__).parent / "tado_api_response.json"


@pytest.fixture
def tado_data():
    return json.loads(TEST_PATH.read_text(encoding="utf-8"))


@pytest.mark.extract
def test_extract(tmp_path):
    """Check that extract method works OK,
    Including API auth and request etc"""
    result, metadata = extract(tmp_path, TEST_DATE)
    save_path = Path(metadata["extracted_path"])
    assert isinstance(result, dict)
    assert save_path.is_file()


@pytest.mark.transform
def test_transform(tado_data, tmp_path):
    """Check that transform method works OK"""
    metadata = {
        "extracted_path": tmp_path,
        "zone_id": "zone_id",
        "date": TEST_DATE,
    }
    tado_data = transform(tado_data, TEST_DATE, metadata)
    assert isinstance(tado_data, dict)


@pytest.mark.transform
def test_partial_transform(tado_data, tmp_path):
    """Check that partial data is handled properly, should fail and allow retry"""
    assert False
