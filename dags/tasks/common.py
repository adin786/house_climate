from typing import Optional, Union
from pathlib import Path
import logging
import pendulum
import json

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def generate_save_path(
    base_path: Union[str, Path],
    zone_id: str,
    date: str,
    ext: str,
    suffix: Optional[str] = None,
) -> str:
    save_path = (Path(base_path) / f"tado_zone{zone_id}_{date}{suffix}").with_suffix(
        ext
    )
    return str(save_path)


def read_extracted_historic(metadata: dict) -> dict:
    """Read each historic data file as a generator"""
    for path in metadata["extracted"]["historic_data"]:
        path = Path(path)
        tado_data = json.loads(path.read_text(encoding='utf-8'))
        yield tado_data


def read_extracted_zone(metadata: dict) -> list:
    """Read zone metadatadata file"""
    path = Path(metadata["extracted"]["zone_data"])
    return json.loads(path.read_text(encoding='utf-8'))
