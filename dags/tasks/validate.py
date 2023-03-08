import json
import logging
from pathlib import Path

import pendulum
from pydantic.error_wrappers import ValidationError
from tasks.helpers.data_models import Metadata, ValidateField
from tasks.helpers.exceptions import IncompleteDataDuration
from tasks.helpers.logs import make_logger
from tasks.helpers.tado_data_models import TadoDataModel

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
SHORT_DAYS = [
    pendulum.datetime(2022, 3, 27),
]


def validate(metadata: Metadata) -> Metadata:
    """Validates that the saved JSON data parses properly according
    to the expected schema using Pydantic.  Also checks that the
    data covers a full 24 hour duration"""

    # Validate that the saved tado_data parses to pydantic datamodel properly
    validated_zones = []
    for hist_data in metadata.extract.zones:
        logger.info("Validating json data for zone %s", hist_data.zone_id)

        try:
            # Parse using pydantic to validate JSON schema
            tado_data = TadoDataModel.parse_file(hist_data.path)
        except (ValidationError, KeyError) as exc:
            logger.error("Error parsing into pydantic data model.")
            # raise exc
            # except KeyError as exc:
            json_data = hist_data.path.read_text("utf-8")
            json_dict = json.loads(json_data)
            logger.error("Json: %s", json_dict)
            if "errors" in json_dict:
                error_codes = [e["code"] for e in json_dict["errors"]]
                if "beforeZoneCreation" in error_codes:
                    logger.error(
                        "Zone not created yet, remove this zone from downstream tasks"
                    )
                    continue
            else:
                raise exc

        # Check computed data duration is for a full day
        this_zone_duration = int(round(tado_data.computed_duration, 0))
        if this_zone_duration < 24:

            this_date = pendulum.parse(metadata.date).at(0, 0, 0)
            if this_zone_duration == 23 and this_date in SHORT_DAYS:
                logger.warning(
                    "This date is in the SHORT_DAYS list, must be a clock change. Allowing this to proceed."
                )
            else:
                raise IncompleteDataDuration(
                    f"Less than 24 hrs in this dataset: {tado_data.computed_duration} hours"
                )
        logger.debug(
            "TadoDataModel parsed successfully for zone_id: %s, duration: %s",
            hist_data.zone_id,
            tado_data.computed_duration,
        )

        # Save a backup copy and overwrite the original with the parsed json data
        validated_path = hist_data.path.parent / (
            hist_data.path.stem + "_validated.json"
        )
        validated_path.write_text(
            tado_data.json(by_alias=True, indent=4, sort_keys=True), "utf-8"
        )
        validated_zones.append({"path": validated_path, "zone_id": hist_data.zone_id})

    logger.debug("Updating metadata")
    metadata.validate_ = ValidateField(
        zones=validated_zones, zones_path=metadata.extract.zones_path
    )
    # # Remove any bad zones from metadata
    # logger.info("Number of bad zones to remove: %s", len(bad_zones))
    # for hist_data in bad_zones:
    #     logger.info("Removing zone %s", hist_data.zone_id)
    #     metadata.extract.zones.remove(hist_data)

    logger.debug(metadata.dict(by_alias=True))
    return metadata
