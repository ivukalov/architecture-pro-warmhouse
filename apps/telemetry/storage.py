from datetime import datetime
from threading import Lock

from models import TelemetryEvent, TelemetryOut


class InMemoryStorage:
    """Заглушка"""

    def __init__(self) -> None:
        self._records: list[TelemetryOut] = []
        self._next_id = 1
        self._lock = Lock()

    def save(self, event: TelemetryEvent) -> TelemetryOut:
        with self._lock:
            record = TelemetryOut(record_id=self._next_id, **event.model_dump())
            self._next_id += 1
            self._records.append(record)
            return record

    def query(
        self,
        device_id: str,
        metric: str,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
        limit: int = 100,
    ) -> list[TelemetryOut]:
        result = [
            r for r in self._records
            if r.device_id == device_id and r.metric == metric
        ]
        if from_ts:
            result = [r for r in result if r.timestamp >= from_ts]
        if to_ts:
            result = [r for r in result if r.timestamp <= to_ts]
        result.sort(key=lambda r: r.timestamp, reverse=True)
        return result[:limit]

    def aggregate(self, device_id: str, metric: str) -> dict | None:
        values = [
            r.value for r in self._records
            if r.device_id == device_id and r.metric == metric
        ]
        if not values:
            return None
        return {
            "device_id": device_id,
            "metric": metric,
            "count": len(values),
            "avg": sum(values) / len(values),
            "min": min(values),
            "max": max(values),
        }