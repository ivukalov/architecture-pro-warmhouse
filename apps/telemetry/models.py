from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field


class TelemetryEvent(BaseModel):
    """Событие телеметрии из Kafka"""
    device_id: str
    metric: str
    value: float
    unit: str | None = None
    timestamp: datetime
    tags: dict[str, Any] | None = None


class TelemetryOut(TelemetryEvent):
    record_id: int


class AggregateOut(BaseModel):
    device_id: str
    metric: str
    count: int
    avg: float
    min: float
    max: float