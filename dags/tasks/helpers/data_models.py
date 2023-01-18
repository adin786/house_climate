from typing import Optional

from pydantic import BaseModel, DirectoryPath, FilePath


class HistoricDataItem(BaseModel):
    path: FilePath
    zone_id: int


class ExtractField(BaseModel):
    historic_data: list[HistoricDataItem]
    zone_data: FilePath


class TransformedZoneItem(BaseModel):
    climate_path: FilePath
    days_path: FilePath


class TransformField(BaseModel):
    paths: list[TransformedZoneItem]


class Metadata(BaseModel):
    base_path: DirectoryPath
    date: str
    extract: Optional[ExtractField]
    transform: Optional[TransformField]
    # duration: float

    # class Config:
    #     allow_mutation = False
