from __future__ import annotations

from typing import Any, List, Optional

from pydantic import BaseModel, Field


class DataInterval(BaseModel):
    from_: str = Field(..., alias='from')
    to: str
    value: str


class CallForHeat(BaseModel):
    dataIntervals: List[DataInterval]
    timeSeriesType: str
    valueType: str


class Interval(BaseModel):
    from_: str = Field(..., alias='from')
    to: str


class DataPoint(BaseModel):
    timestamp: str
    value: float


class Humidity(BaseModel):
    dataPoints: List[DataPoint]
    max: float
    min: float
    percentageUnit: str
    timeSeriesType: str
    valueType: str


class Value(BaseModel):
    celsius: float
    fahrenheit: float


class DataPoint1(BaseModel):
    timestamp: str
    value: Value


class Max(BaseModel):
    celsius: float
    fahrenheit: float


class Min(BaseModel):
    celsius: float
    fahrenheit: float


class InsideTemperature(BaseModel):
    dataPoints: List[DataPoint1]
    max: Max
    min: Min
    timeSeriesType: str
    valueType: str


class DataInterval1(BaseModel):
    from_: str = Field(..., alias='from')
    to: str
    value: bool


class MeasuringDeviceConnected(BaseModel):
    dataIntervals: List[DataInterval1]
    timeSeriesType: str
    valueType: str


class MeasuredData(BaseModel):
    humidity: Humidity
    insideTemperature: InsideTemperature
    measuringDeviceConnected: MeasuringDeviceConnected


class Value1(BaseModel):
    power: str
    temperature: Any
    type: str


class DataInterval2(BaseModel):
    from_: str = Field(..., alias='from')
    to: str
    value: Value1


class Settings(BaseModel):
    dataIntervals: List[DataInterval2]
    timeSeriesType: str
    valueType: str


class Setting(BaseModel):
    power: str
    temperature: Any
    type: str


class Value2(BaseModel):
    setting: Setting
    stripeType: str


class DataInterval3(BaseModel):
    from_: str = Field(..., alias='from')
    to: str
    value: Value2


class Stripes(BaseModel):
    dataIntervals: List[DataInterval3]
    timeSeriesType: str
    valueType: str


class Temperature(BaseModel):
    celsius: float
    fahrenheit: float


class Value3(BaseModel):
    state: str
    temperature: Temperature


class DataInterval4(BaseModel):
    from_: str = Field(..., alias='from')
    to: str
    value: Value3


class Condition(BaseModel):
    dataIntervals: List[DataInterval4]
    timeSeriesType: str
    valueType: str


class Temperature1(BaseModel):
    celsius: float
    fahrenheit: float


class Field0400(BaseModel):
    state: str
    temperature: Temperature1


class Temperature2(BaseModel):
    celsius: float
    fahrenheit: float


class Field0800(BaseModel):
    state: str
    temperature: Temperature2


class Temperature3(BaseModel):
    celsius: float
    fahrenheit: float


class Field1200(BaseModel):
    state: str
    temperature: Temperature3


class Temperature4(BaseModel):
    celsius: float
    fahrenheit: float


class Field1600(BaseModel):
    state: str
    temperature: Temperature4


class Temperature5(BaseModel):
    celsius: float
    fahrenheit: float


class Field2000(BaseModel):
    state: str
    temperature: Temperature5


class Slots1(BaseModel):
    field_04_00: Field0400 = Field(..., alias='04:00')
    field_08_00: Field0800 = Field(..., alias='08:00')
    field_12_00: Field1200 = Field(..., alias='12:00')
    field_16_00: Field1600 = Field(..., alias='16:00')
    field_20_00: Field2000 = Field(..., alias='20:00')


class Slots(BaseModel):
    slots: Slots1
    timeSeriesType: str
    valueType: str


class DataInterval5(BaseModel):
    from_: str = Field(..., alias='from')
    to: str
    value: bool


class Sunny(BaseModel):
    dataIntervals: List[DataInterval5]
    timeSeriesType: str
    valueType: str


class Weather(BaseModel):
    condition: Condition
    slots: Slots
    sunny: Sunny


class Model(BaseModel):
    callForHeat: CallForHeat
    hoursInDay: int
    interval: Interval
    measuredData: MeasuredData
    settings: Settings
    stripes: Stripes
    weather: Weather
    zoneType: str
