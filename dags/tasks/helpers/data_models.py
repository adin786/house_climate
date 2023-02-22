from pathlib import Path
from typing import Optional

from pydantic import BaseModel, DirectoryPath


class HistoricDataItem(BaseModel):
    path: Path
    zone_id: int


class ExtractField(BaseModel):
    zones: list[Optional[HistoricDataItem]]
    zones_path: Path


class TransformedZoneItem(BaseModel):
    interior_path: Path
    days_path: Path
    weather_path: Path
    call_for_heat_path: Path
    zone_id: int


class TransformField(BaseModel):
    zones: list[Optional[TransformedZoneItem]]
    zones_path: Optional[Path]
    interior_all_path: Optional[Path]
    days_all_path: Optional[Path]
    weather_all_path: Optional[Path]
    call_for_heat_all_path: Optional[Path]


class Metadata(BaseModel):
    base_path: DirectoryPath
    date: str
    extract: Optional[ExtractField]
    transform: Optional[TransformField]
    # duration: float

    # class Config:
    #     allow_mutation = False
