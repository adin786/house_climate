from typing import Optional

from pydantic import BaseModel, DirectoryPath, Path
from pathlib import Path

class HistoricDataItem(BaseModel):
    path: Path
    zone_id: int


class ExtractField(BaseModel):
    historic_data: list[Optional[HistoricDataItem]]
    zone_data: Path


class TransformedZoneItem(BaseModel):
    climate_path: Path
    days_path: Path
    weather_path: Path


class TransformField(BaseModel):
    paths: list[Optional[TransformedZoneItem]]


class Metadata(BaseModel):
    base_path: DirectoryPath
    date: str
    extract: Optional[ExtractField]
    transform: Optional[TransformField]
    # duration: float

    # class Config:
    #     allow_mutation = False
