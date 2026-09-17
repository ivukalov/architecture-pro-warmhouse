from datetime import datetime
from threading import Lock
from uuid import uuid4

from models import Device, DeviceCreate, DeviceStatus, DeviceUpdate


class InMemoryStorage:
    """Заглушка"""

    def __init__(self) -> None:
        self._devices: dict[str, Device] = {}
        self._lock = Lock()

    def create(self, data: DeviceCreate) -> Device:
        with self._lock:
            device = Device(
                device_id=str(uuid4()),
                type_id=data.type_id,
                home_id=data.home_id,
                name=data.name,
                serial_number=data.serial_number,
                status=DeviceStatus.UNKNOWN,
            )
            self._devices[device.device_id] = device
            return device

    def get(self, device_id: str) -> Device | None:
        return self._devices.get(device_id)

    def list(
        self,
        home_id: str | None = None,
        room_id: str | None = None,
        status: DeviceStatus | None = None,
    ) -> list[Device]:
        result = list(self._devices.values())
        if home_id:
            result = [d for d in result if d.home_id == home_id]
        if room_id:
            result = [d for d in result if d.room_id == room_id]
        if status:
            result = [d for d in result if d.status == status]
        return result

    def update(self, device_id: str, data: DeviceUpdate) -> Device | None:
        with self._lock:
            device = self._devices.get(device_id)
            if not device:
                return None
            if data.name is not None:
                device.name = data.name
            if data.room_id is not None:
                device.room_id = data.room_id
            device.updated_at = datetime.utcnow()
            return device

    def bind(self, device_id: str, room_id: str) -> Device | None:
        with self._lock:
            device = self._devices.get(device_id)
            if not device:
                return None
            device.room_id = room_id
            device.updated_at = datetime.utcnow()
            return device

    def unbind(self, device_id: str) -> Device | None:
        with self._lock:
            device = self._devices.get(device_id)
            if not device:
                return None
            device.room_id = None
            device.updated_at = datetime.utcnow()
            return device

    def delete(self, device_id: str) -> bool:
        with self._lock:
            return self._devices.pop(device_id, None) is not None

    def update_state(self, device_id: str, desired_state: dict) -> Device | None:
        with self._lock:
            device = self._devices.get(device_id)
            if not device:
                return None
            device.desired_state = desired_state
            device.updated_at = datetime.utcnow()
            return device