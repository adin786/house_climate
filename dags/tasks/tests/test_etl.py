import pytest
from dags.tasks.extract import extract
from dags.tasks.transform import transform
import logging
from pathlib import Path
import json


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

TEST_DATE = '2022-08-15'
TEST_DATE = '2022-12-07'
TEST_PATH = Path(__file__).parent / "tado_api_response.json"

@pytest.fixture
def tado_data():
    return json.loads(TEST_PATH.read_text(encoding='utf-8'))


@pytest.mark.extract
def test_extract(tmp_path):
    result, save_path = extract(tmp_path, TEST_DATE)
    save_path = Path(save_path)
    assert isinstance(result, dict)
    assert save_path.is_file()


@pytest.mark.transform
def test_transform(tado_data):
    tado_data = transform(tado_data)
    assert isinstance(tado_data, dict)
    assert False
