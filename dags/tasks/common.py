from typing import Optional, Union
from pathlib import Path
import logging
import pendulum

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


def validate_raw_data(tado_data: dict):
    """Run some checks on the tado response"""
    logger.info("Running tado validation ")
    try:
        assert isinstance(tado_data, dict)

        assert "hoursInDay" in tado_data
        assert tado_data["hoursInDay"] == 24

        assert "interval" in tado_data
        start_date = pendulum.parse(tado_data["interval"]["from"])
        end_date = pendulum.parse(tado_data["interval"]["to"])
        delta = end_date.diff(start_date)
        logger.debug(f"{start_date=}")
        logger.debug(f"{end_date=}")
        logger.debug(f"Data duration {delta.in_hours()} H ({delta.in_minutes()} min)")
        assert delta.in_hours() >= 24

    except AssertionError as err:
        logger.error("Tado data failed validation checks")
        raise err
    logger.info("Tado validation passed")
