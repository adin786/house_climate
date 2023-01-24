import json
from pathlib import Path

from tasks.helpers.data_models import Metadata
from tasks.helpers.logs import make_logger
from tasks.helpers.tado_data_models import TadoDataModel
from tasks.helpers.exceptions import IncompleteDataDuration


logger = make_logger(__name__, add_handler=True, level="debug")


def validate(metadata: Metadata) -> Metadata:
    """Validates that the saved JSON data parses properly according
    to the expected schema using Pydantic.  Also checks that the 
    data covers a full 24 hour duration"""

    # Validate that the saved tado_data parses to
    for hist_data in metadata.extract.zones:
        # Parse using pydantic to validate JSON schema
        tado_data = TadoDataModel.parse_file(hist_data.path)

        # Check computed data duration is for a full day
        if tado_data.computed_duration < 24:
            raise IncompleteDataDuration(
                f'Less than 24 hrs in this dataset: {tado_data.computed_duration} hours'
            )
        logger.debug(
            "TadoDataModel parsed successfully for zone_id: %s, duration: %s", 
            hist_data.zone_id,
            tado_data.computed_duration,
        )


    logger.debug("Updating metadata")
    return metadata
