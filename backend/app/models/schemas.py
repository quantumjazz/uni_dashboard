from typing import Any

from pydantic import BaseModel, Field


class IndicatorDefinition(BaseModel):
    id: str
    title: str
    strategic_question: str
    panel: str
    source: str
    dataset: str
    description: str
    unit: str | None = None
    frequency: str | None = None
    default_countries: list[str] = Field(default_factory=list)
    dimensions: dict[str, Any] = Field(default_factory=dict)
    aggregate_dimension: str | None = None
    notes: str | None = None


class DataPoint(BaseModel):
    source: str
    dataset: str
    indicator: str
    country: str
    year: int
    value: float
    unit: str | None = None
    note: str | None = None


class DataResponse(BaseModel):
    indicator: IndicatorDefinition
    rows: list[DataPoint]
    summary: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class CountryOption(BaseModel):
    code: str
    label: str

