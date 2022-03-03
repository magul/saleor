import math
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from django.conf import settings
from django.core.cache import cache
from kombu import Connection, Exchange, Queue
from kombu.exceptions import ChannelError
from kombu.simple import SimpleQueue

if TYPE_CHECKING:
    from kombu.message import Message

OBSERVABILITY_EXCHANGE_NAME = "observability_exchange"
CACHE_KEY = "observability_queue_"
EXCHANGE = Exchange(OBSERVABILITY_EXCHANGE_NAME, type="direct")


class ObservabilityBuffer(SimpleQueue):
    BATCH_SIZE: int = settings.OBSERVABILITY_QUEUE_BATCH_SIZE

    def qsize(self) -> int:
        try:
            return super().qsize()
        except ChannelError:
            # Let's suppose that queue is empty if it not exists
            return 0

    def batches_count(self) -> int:
        return math.ceil(self.qsize() / self.BATCH_SIZE)

    def get_messages_batch(self, batch_size: int) -> list["Message"]:
        messages = []
        for _ in range(batch_size):
            try:
                # TODO add prefetch_count
                messages.append(self.get(block=True, timeout=10))
            except self.Empty:
                break
        return messages


def queue_name(event_type: str) -> str:
    return cache.make_key(CACHE_KEY + event_type)


def queue_routing_key(event_type: str) -> str:
    return f"{OBSERVABILITY_EXCHANGE_NAME}.{cache.make_key(event_type)}"


@contextmanager
def get_buffer(event_type: str) -> Generator[ObservabilityBuffer, None, None]:
    queue = Queue(
        queue_name(event_type), EXCHANGE, routing_key=queue_routing_key(event_type)
    )
    with Connection(
        settings.OBSERVABILITY_BROKER_URL,
        transport_options=settings.OBSERVABILITY_BROKER_TRANSPORT_OPTIONS,
    ) as conn:
        with ObservabilityBuffer(conn, queue) as buffer:
            yield buffer


def observability_buffer_event(event_type: str, data: dict):
    with get_buffer(event_type) as buffer:
        if buffer.qsize() < settings.OBSERVABILITY_QUEUE_MAX_LENGTH:
            buffer.put(data)
        # TODO logging
