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
    """Generates a save file path based on a base path, zone number and """
    save_path = (Path(base_path) / f"tado_zone{zone_id}_{date}{suffix}").with_suffix(
        ext
    )
    return str(save_path)

