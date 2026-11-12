"""MQTT telemetry ingester, Redis expiry sweeper, and Neo4j RCA orchestrator."""

import asyncio
import json
import logging
import os
import socket
from datetime import UTC, datetime
from pathlib import Path

import aiomqtt
from neo4j import AsyncGraphDatabase
from pydantic import ValidationError
from redis.asyncio import Redis

from iot_rca.config import Settings, get_settings
from iot_rca.models import RcaEvaluation, TelemetryPayload, TopologySeed
from iot_rca.neo4j_repo import Neo4jTopologyRepo
from iot_rca.rca_engine import correlate_failures
from iot_rca.redis_store import RedisStateStore
from iot_rca.startup import wait_for_stores

logger = logging.getLogger(__name__)

TELEMETRY_TOPIC_FILTER = "iot/telemetry/#"


def parse_telemetry_message(raw_payload: bytes | str) -> TelemetryPayload:
    """Parse and validate incoming MQTT JSON payload into a TelemetryPayload."""
    text = raw_payload.decode("utf-8") if isinstance(raw_payload, bytes) else raw_payload
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid telemetry JSON: {exc}") from exc

    try:
        return TelemetryPayload.model_validate(data)
    except ValidationError as exc:
        raise ValueError(f"Telemetry schema validation failed: {exc}") from exc


async def run_rca_cycle(
    redis_store: RedisStateStore,
    neo4j_repo: Neo4jTopologyRepo,
    settings: Settings,
) -> RcaEvaluation:
    """Fetch live Redis state and Neo4j topology candidates, correlate, and sync incidents."""
    (
        offline_sensors,
        offline_infra,
        online_infra,
        _all_states,
    ) = await redis_store.get_offline_and_online_sets()

    candidates = await neo4j_repo.fetch_rca_candidates(online_infra)
    evaluation = correlate_failures(
        offline_sensor_ids=offline_sensors,
        offline_infra_ids=offline_infra,
        candidates=candidates,
        gateway_threshold=settings.gateway_coverage_threshold,
        passive_panel_threshold=settings.passive_panel_coverage_threshold,
        evaluated_at=datetime.now(UTC),
    )
    open_incidents = await neo4j_repo.sync_incidents(evaluation)
    await redis_store.publish_live_update(
        {
            "type": "rca_update",
            "evaluated_at": evaluation.evaluated_at.isoformat(),
            "offline_sensors": evaluation.offline_sensor_count,
            "offline_infra": evaluation.offline_infra_count,
            "open_incidents": len(open_incidents),
            "noise_reduction_pct": evaluation.noise_reduction_pct,
        }
    )
    return evaluation


class MqttIngesterService:
    """Consumes MQTT heartbeats into Redis and periodically sweeps expirations + runs RCA."""

    def __init__(
        self,
        settings: Settings,
        redis_store: RedisStateStore,
        neo4j_repo: Neo4jTopologyRepo,
        *,
        consumer_name: str | None = None,
    ) -> None:
        self._settings = settings
        self._redis = redis_store
        self._neo4j = neo4j_repo
        self._dirty_event = asyncio.Event()
        self._consumer_name = consumer_name or f"{socket.gethostname()}-{os.getpid()}"

    async def handle_raw_message(self, raw_payload: bytes | str) -> bool:
        """Validate and persist a single telemetry payload. Returns True if accepted."""
        try:
            payload = parse_telemetry_message(raw_payload)
        except ValueError as exc:
            logger.warning("Dropping invalid MQTT message: %s", exc)
            return False

        accepted, transitioned = await self._redis.record_heartbeat(
            payload,
            ttl_seconds=self._settings.heartbeat_ttl_seconds,
            dedup_ttl_seconds=self._settings.dedup_ttl_seconds,
        )
        if transitioned:
            self._dirty_event.set()
        return accepted

    async def mqtt_consumer_loop(self, stop_event: asyncio.Event) -> None:
        """Maintain resilient MQTT subscription and ingest heartbeats."""
        while not stop_event.is_set():
            try:
                async with aiomqtt.Client(
                    hostname=self._settings.mqtt_host,
                    port=self._settings.mqtt_port,
                    identifier="iot-rca-ingester",
                ) as client:
                    await client.subscribe(TELEMETRY_TOPIC_FILTER, qos=1)
                    logger.info(
                        "Connected to MQTT broker %s:%s",
                        self._settings.mqtt_host,
                        self._settings.mqtt_port,
                    )
                    async for message in client.messages:
                        if stop_event.is_set():
                            break
                        if isinstance(message.payload, (bytes, bytearray)):
                            await self.handle_raw_message(bytes(message.payload))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("MQTT connection interrupted (%s), retrying in 2s...", exc)
                await asyncio.sleep(2.0)

    async def expiry_and_rca_loop(self, stop_event: asyncio.Event) -> None:
        """Sweep expired device heartbeats, consume stream events, and run Neo4j RCA."""
        last_rca_ts = 0.0
        loop = asyncio.get_running_loop()

        while not stop_event.is_set():
            try:
                expired = await self._redis.sweep_expired_heartbeats()
                if expired:
                    logger.info("Expired %d overdue devices: %s", len(expired), expired)

                stream_events = await self._redis.consume_state_events(
                    consumer_name=self._consumer_name
                )
                if stream_events:
                    self._dirty_event.set()

                now = loop.time()
                should_run_rca = self._dirty_event.is_set() or (
                    now - last_rca_ts >= self._settings.rca_interval_seconds * 2
                )
                if should_run_rca:
                    token = await self._redis.acquire_rca_lock(ttl_seconds=10)
                    if token is not None:
                        try:
                            self._dirty_event.clear()
                            await run_rca_cycle(self._redis, self._neo4j, self._settings)
                            last_rca_ts = now
                        finally:
                            await self._redis.release_rca_lock(token)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error during expiry sweep / RCA cycle")

            await asyncio.sleep(self._settings.expiry_sweep_interval_seconds)

    async def run(self) -> None:
        """Start MQTT ingestion and expiry/RCA loops concurrently."""
        stop_event = asyncio.Event()
        await self._redis.ensure_stream_group()
        await asyncio.gather(
            self.mqtt_consumer_loop(stop_event),
            self.expiry_and_rca_loop(stop_event),
        )


def load_topology_seed(path: Path) -> TopologySeed:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return TopologySeed.model_validate(raw)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    settings = get_settings()
    redis_client = Redis.from_url(settings.redis_url, decode_responses=False)
    neo4j_driver = AsyncGraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )

    redis_store = RedisStateStore(redis_client)
    neo4j_repo = Neo4jTopologyRepo(neo4j_driver)

    try:
        seed = load_topology_seed(settings.topology_seed_path)
        await wait_for_stores(redis_store, neo4j_repo, service_name="ingester")
        await neo4j_repo.ensure_schema_and_seed(seed)
        service = MqttIngesterService(settings, redis_store, neo4j_repo)
        await service.run()
    finally:
        await neo4j_driver.close()
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
