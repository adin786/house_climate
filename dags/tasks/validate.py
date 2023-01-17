from tasks.helpers.tado_data_models import TadoDataModel
from tasks.helpers.logs import make_logger
from tasks.helpers.common import read_extracted_historic
from tasks.helpers.data_models import Metadata
import json
from pathlib import Path


logger = make_logger(__name__, add_handler=True, level='debug')


def validate(metadata: Metadata) -> Metadata:

    # Validate that the saved tado_data parses to 
    for json_path, zone_id in metadata.extract.historic_data:
        # Parse using pydantic to validate JSON schema
        TadoDataModel.parse_file(json_path)
        logger.debug('TadoDataModel parsed successfully for zone_id: %s', zone_id)

    logger.debug('Updating metadata')
    return metadata