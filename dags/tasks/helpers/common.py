import json
import logging
from pathlib import Path
from typing import Optional, Union

from tasks.helpers.data_models import Metadata

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def generate_save_path(
    metadata: Metadata,
    zone_id: str,
    ext: str,
    suffix: Optional[str] = None,
) -> str:
    """Generates a save file path based on a base path, zone number and"""
    base_path = metadata.base_path
    date = metadata.date
    save_path = (Path(base_path) / f"tado_zone{zone_id}_{date}{suffix}").with_suffix(
        ext
    )
    return str(save_path)


def read_extracted_zone(metadata: Metadata) -> list:
    """Read zone metadatadata file"""
    # TODO: probably refactor with pydantic
    path = Path(metadata.extract.zone_data)
    return json.loads(path.read_text(encoding="utf-8"))
