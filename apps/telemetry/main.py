import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query

from consumer import run_consumer
from models import AggregateOut, TelemetryOut
from storage import InMemoryStorage

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("telemetry")

storage = InMemoryStorage()
consumer_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Запускаем Kafka consumer"""
    global consumer_task
    consumer_task = asyncio.create_task(run_consumer(storage))
    log.info("Сервис телеметрии запущен")
    yield
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="Telemetry Service",
    version="1.0.0",
    description="Сервис телеметрии",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/telemetry", response_model=list[TelemetryOut])
def query(
    device_id: str = Query(..., description="UUID устройства"),
    metric: str = Query(..., description="Метрика"),
    from_ts: datetime | None = Query(None, alias="from"),
    to_ts: datetime | None = Query(None, alias="to"),
    limit: int = Query(100, ge=1, le=1000),
) -> list[TelemetryOut]:
    """Получить временной ряд по устройству и метрике."""
    return storage.query(device_id, metric, from_ts, to_ts, limit)


@app.get("/telemetry/aggregate", response_model=AggregateOut)
def aggregate(
    device_id: str = Query(...),
    metric: str = Query(...),
) -> AggregateOut:
    """Получить агрегат по метрике."""
    result = storage.aggregate(device_id, metric)
    if result is None:
        raise HTTPException(status_code=404, detail="Данных нет")
    return AggregateOut(**result)