from __future__ import annotations

"""RabbitMQ-backed implementation of the *Publisher* abstraction."""

from typing import Optional

from protocol.rabbit_protocol import RabbitMQ

from ..domain.publisher import Publisher

class RabbitPublisher(Publisher):
    """Simple adapter that forwards ``publish`` calls to a RabbitMQ producer."""

    def __init__(self, producer: RabbitMQ) -> None:
        self._producer = producer

    def publish(self, data: bytes, *, routing_key: Optional[str] = None) -> None:  # noqa: D401
        # The underlying helper already defaults to the routing key configured
        # at construction time; we only override if caller provides one.
        if routing_key is None:
            self._producer.publish(data)
        else:
            self._producer.publish(data, routing_key=routing_key) 