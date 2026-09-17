import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from models import TelemetryEvent
from storage import InMemoryStorage

log = logging.getLogger("telemetry.consumer")

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "telemetry.events"
GROUP_ID = "telemetry-service"


async def run_consumer(storage: InMemoryStorage) -> None:
    """Читает события телеметрии из Kafka и сохраняет их."""
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    await consumer.start()
    log.info("Kafka consumer запущен, топик=%s, group=%s", TOPIC, GROUP_ID)

    try:
        async for msg in consumer:
            try:
                event = TelemetryEvent(**msg.value)
                storage.save(event)
                log.debug("Сохранено: device=%s metric=%s value=%s",
                          event.device_id, event.metric, event.value)
            except Exception as e:
                log.exception("Ошибка обработки сообщения: %s", e)
    finally:
        await consumer.stop()
        log.info("Kafka consumer остановлен")