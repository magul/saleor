from collections.abc import Generator
from contextlib import contextmanager

from django.conf import settings
from django.core.cache import cache
from kombu import Connection, Exchange, Queue
from kombu.exceptions import ChannelError
from kombu.simple import SimpleQueue

OBSERVABILITY_EXCHANGE_NAME = "observability_exchange"
CACHE_KEY = "observability_queue_"
EXCHANGE = Exchange(OBSERVABILITY_EXCHANGE_NAME, type="direct")


def get_queue_name(event_type: str):
    return cache.make_key(CACHE_KEY + event_type)


@contextmanager
def get_buffer(event_type: str) -> Generator[SimpleQueue, None, None]:
    queue_name = get_queue_name(event_type)
    routing_key = f"{OBSERVABILITY_EXCHANGE_NAME}.{queue_name}"
    queue = Queue(queue_name, EXCHANGE, routing_key=routing_key)
    with Connection(
        settings.OBSERVABILITY_BROKER_URL,
        transport_options=settings.OBSERVABILITY_BROKER_TRANSPORT_OPTIONS,
    ) as conn:
        with conn.SimpleQueue(queue) as buffer:
            yield buffer


def buffer_event(event_type: str, data: dict):
    with get_buffer(event_type) as buffer:
        try:
            size = buffer.qsize()
        except ChannelError:
            size = 0
        if size < settings.OBSERVABILITY_QUEUE_MAX_LENGTH:
            buffer.put(data)
        # TODO logging
