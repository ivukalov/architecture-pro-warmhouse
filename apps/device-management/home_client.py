import logging

import httpx

log = logging.getLogger("device.home")


class HomeManagementClient:
    """HTTP-клиент к home_management для валидации дома/комнаты."""

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")

    async def get_home(self, home_id: str) -> dict | None:
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                r = await client.get(f"{self._base_url}/homes/{home_id}")
                if r.status_code == 200:
                    return r.json()
                return None
        except httpx.HTTPError as e:
            log.warning("home_management недоступен: %s", e)
            return None

    async def get_room(self, home_id: str, room_id: str) -> dict | None:
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                r = await client.get(f"{self._base_url}/homes/{home_id}/rooms")
                if r.status_code != 200:
                    return None
                rooms = r.json()
                for room in rooms:
                    if room.get("room_id") == room_id:
                        return room
                return None
        except httpx.HTTPError as e:
            log.warning("home_management недоступен: %s", e)
            return None