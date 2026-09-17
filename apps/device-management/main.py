import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, status

from home_client import HomeManagementClient
from kafka_producer import CommandPublisher
from models import (
    BindRequest,
    CommandAccepted,
    CommandEvent,
    CommandRequest,
    Device,
    DeviceCreate,
    DeviceStatus,
    DeviceUpdate,
)
from storage import InMemoryStorage

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("device")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
HOME_SERVICE_URL = os.getenv("HOME_SERVICE_URL", "http://localhost:8081")

storage = InMemoryStorage()
publisher = CommandPublisher(KAFKA_BOOTSTRAP)
home_client = HomeManagementClient(HOME_SERVICE_URL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await publisher.start()
    log.info("Device Management Service запущен")
    yield
    await publisher.stop()


app = FastAPI(
    title="Device Management Service",
    version="1.0.0",
    description="Сервис управления устройствами",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


# ─── Регистрация устройств ───

@app.post("/devices", response_model=Device, status_code=status.HTTP_201_CREATED)
async def create_device(data: DeviceCreate) -> Device:
    """Зарегистрировать новое устройство."""
    home = await home_client.get_home(data.home_id)
    if home is None:
        raise HTTPException(status_code=400, detail="Дом не найден")

    return storage.create(data)


@app.get("/devices", response_model=list[Device])
def list_devices(
    home_id: str | None = Query(None),
    room_id: str | None = Query(None),
    status_filter: DeviceStatus | None = Query(None, alias="status"),
) -> list[Device]:
    """Список устройств"""
    return storage.list(home_id, room_id, status_filter)


@app.get("/devices/{device_id}", response_model=Device)
def get_device(device_id: str) -> Device:
    device = storage.get(device_id)
    if not device:
        raise HTTPException(status_code=404, detail="Устройство не найдено")
    return device


@app.patch("/devices/{device_id}", response_model=Device)
def update_device(device_id: str, data: DeviceUpdate) -> Device:
    device = storage.update(device_id, data)
    if not device:
        raise HTTPException(status_code=404, detail="Устройство не найдено")
    return device


@app.delete("/devices/{device_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_device(device_id: str) -> None:
    if not storage.delete(device_id):
        raise HTTPException(status_code=404, detail="Устройство не найдено")


@app.post("/devices/{device_id}/bind", response_model=Device)
async def bind_device(device_id: str, data: BindRequest) -> Device:
    """Привязать устройство к комнате."""
    device = storage.get(device_id)
    if not device:
        raise HTTPException(status_code=404, detail="Устройство не найдено")

    room = await home_client.get_room(device.home_id, data.room_id)
    if room is None:
        raise HTTPException(status_code=400, detail="Комната не найдена в этом доме")

    result = storage.bind(device_id, data.room_id)
    return result


@app.post("/devices/{device_id}/unbind", response_model=Device)
def unbind_device(device_id: str) -> Device:
    """Отвязать устройство от комнаты."""
    result = storage.unbind(device_id)
    if not result:
        raise HTTPException(status_code=404, detail="Устройство не найдено")
    return result


@app.post(
    "/devices/{device_id}/commands",
    response_model=CommandAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def send_command(device_id: str, data: CommandRequest) -> CommandAccepted:
    """Отправить команду устройству"""
    device = storage.get(device_id)
    if not device:
        raise HTTPException(status_code=404, detail="Устройство не найдено")

    command_id = str(uuid4())

    event = CommandEvent(
        command_id=command_id,
        device_id=device_id,
        command=data.command,
        params=data.params,
        issued_at=datetime.utcnow(),
    )

    await publisher.publish_command(event)

    desired = {data.command: data.params or True}
    storage.update_state(device_id, desired)

    return CommandAccepted(command_id=command_id, device_id=device_id)