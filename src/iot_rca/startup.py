"""Shared startup readiness helper for backing Redis and Neo4j stores."""

import asyncio
import logging

from iot_rca.neo4j_repo import Neo4jTopologyRepo
from iot_rca.redis_store import RedisStateStore

logger = logging.getLogger(__name__)


async def wait_for_stores(
    redis_store: RedisStateStore,
    neo4j_repo: Neo4jTopologyRepo,
    *,
    service_name: str,
    max_attempts: int = 20,
    delay_seconds: float = 2.0,
) -> None:
    """Wait for Redis and Neo4j readiness, raising RuntimeError on timeout."""
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            if await redis_store.ping() and await neo4j_repo.ping():
                return
        except Exception as exc:
            last_error = exc
            logger.info(
                "%s waiting for Redis/Neo4j (%d/%d): %s",
                service_name,
                attempt,
                max_attempts,
                exc,
            )
        await asyncio.sleep(delay_seconds)

    raise RuntimeError(
        f"{service_name} timed out waiting for Redis/Neo4j readiness after {max_attempts} attempts"
    ) from last_error
