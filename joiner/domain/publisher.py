from __future__ import annotations

"""Domain-level abstraction for publishing bytes downstream.

The rest of the codebase should depend on this interface rather than the
concrete RabbitMQ client so that messaging can be replaced without touching
business logic.
"""

from abc import ABC, abstractmethod
from typing import Optional

class Publisher(ABC):
    """Minimal interface required by joiner use-cases for sending data.

    Only one primitive is exposed; higher-level helpers (batchers, utilities)
    build on top of it.
    """

    @abstractmethod
    def publish(self, data: bytes, *, routing_key: Optional[str] = None) -> None:
        """Send *data* to the downstream transport.

        `routing_key` is transport-specific (e.g. RabbitMQ) and therefore kept
        optional; implementations that do not use routing keys can ignore it.
        """ 