import json
import logging

from aiokafka import AIOKafkaProducer

from models import CommandEvent

log = logging.getLogger("device.kafka")

TOPIC_COMMANDS = "device.commands"


class CommandPublisher:
    """Публикует команды устройствам в Kafka"""

    def __init__(self, bootstrap: str) -> None:
        self._bootstrap = bootstrap
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
        )
        await self._producer.start()
        log.info("Kafka producer запущен: %s", self._bootstrap)

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            log.info("Kafka producer остановлен")

    async def publish_command(self, event: CommandEvent) -> None:
        if not self._producer:
            raise RuntimeError("producer не запущен")

        await self._producer.send_and_wait(
            TOPIC_COMMANDS,
            key=event.device_id,
            value=event.model_dump(),
        )
        log.debug("Команда опубликована: device=%s command=%s",
                  event.device_id, event.command)