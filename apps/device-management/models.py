from datetime import datetime
from enum import Enum
from typing import Any
from pydantic import BaseModel, Field


class DeviceStatus(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    ERROR = "error"
    UNKNOWN = "unknown"


class DeviceCreate(BaseModel):
    """Регистрация устройства."""
    type_id: str
    home_id: str
    name: str
    serial_number: str


class DeviceUpdate(BaseModel):
    """Обновление устройства."""
    name: str | None = None
    room_id: str | None = None


class Device(BaseModel):
    """Устройство"""
    device_id: str
    type_id: str
    home_id: str
    room_id: str | None = None
    name: str
    serial_number: str
    status: DeviceStatus = DeviceStatus.UNKNOWN
    state: dict[str, Any] = Field(default_factory=dict)
    desired_state: dict[str, Any] = Field(default_factory=dict)
    last_seen_at: datetime | None = None
    registered_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class BindRequest(BaseModel):
    """Привязка устройства к комнате."""
    room_id: str


class CommandRequest(BaseModel):
    """Команда устройству."""
    command: str
    params: dict[str, Any] | None = None


class CommandAccepted(BaseModel):
    """Ответ устройства."""
    command_id: str
    device_id: str
    status: str = "accepted"


class CommandEvent(BaseModel):
    """Событие команды"""
    command_id: str
    device_id: str
    command: str
    params: dict[str, Any] | None = None
    issued_at: datetime
    issued_by: str | None = None