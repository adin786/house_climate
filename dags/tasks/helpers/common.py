import logging
from pathlib import Path
from typing import Optional, Union

from tasks.helpers.data_models import Metadata, TransformedZoneItem

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def generate_save_path(
    metadata: Metadata,
    zone_id: str,
    ext: str,
    suffix: Optional[str] = None,
) -> Path:
    """Generates a save file path based on a base path, zone number and"""
    base_path = metadata.base_path
    date = metadata.date
    save_path = (Path(base_path) / f"tado_zone{zone_id}_{date}{suffix}").with_suffix(
        ext
    )
    return save_path


def read_text_file(path: Union[str, Path]) -> str:
    path = Path(path)
    return path.read_text(encoding='utf-8')


def get_zone_item_by_id(zones: list[TransformedZoneItem], zone_id):
    for zone in zones:
        if zone.zone_id == zone_id:
            return zone
    raise ValueError(f"No matching zone_id found: {zone_id}")

