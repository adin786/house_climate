from pydantic import BaseModel
from typing import Optional



class TransformedField(BaseModel):
    climate_path: str
    days_path: str


class HistoricDataItem(BaseModel):
    path: str
    zone_id: int

class ExtractField(BaseModel):
    historic_data: list[HistoricDataItem]
    zone_data: str

class Metadata(BaseModel):
    base_path: str
    date: str
    extract: Optional[ExtractField]
    transformed: Optional[TransformedField]