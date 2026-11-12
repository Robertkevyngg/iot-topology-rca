"""FastAPI application exposing REST endpoints, WebSocket live updates, and Topology UI."""

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiomqtt
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from neo4j import AsyncDriver, AsyncGraphDatabase
from pydantic import BaseModel
from redis.asyncio import Redis

from iot_rca.config import Settings, get_settings
from iot_rca.models import (
    BlastRadiusReport,
    ChaosScenario,
    DeviceStatus,
    NodeType,
    StoredIncident,
    TopologyNodeView,
    TopologySnapshot,
)
from iot_rca.mqtt_ingester import load_topology_seed, run_rca_cycle
from iot_rca.neo4j_repo import Neo4jTopologyRepo
from iot_rca.redis_store import RedisStateStore
from iot_rca.simulator import CHAOS_COMMAND_TOPIC

logger = logging.getLogger(__name__)
STATIC_DIR = Path(__file__).resolve().parent / "static"


class ChaosCommandRequest(BaseModel):
    scenario: ChaosScenario


class ChaosCommandResponse(BaseModel):
    scenario: ChaosScenario
    message: str


async def build_topology_snapshot(
    redis_store: RedisStateStore,
    neo4j_repo: Neo4jTopologyRepo,
) -> TopologySnapshot:
    """Join static Neo4j topology structure with live Redis states and open incidents."""
    (node_rows, edges), states, open_incidents, scenario = await asyncio.gather(
        neo4j_repo.fetch_graph_structure(),
        redis_store.get_all_device_states(),
        neo4j_repo.list_incidents(status="OPEN", limit=100),
        redis_store.get_active_chaos_scenario(),
    )

    rc_incident_by_node = {inc.root_cause_id: inc.id for inc in open_incidents}

    nodes: list[TopologyNodeView] = []
    online_count = 0
    offline_count = 0
    sensor_total = 0
    sensor_online = 0

    for row in node_rows:
        node_id = row["id"]
        node_type = NodeType(row["node_type"])
        is_passive = bool(row["is_passive"])
        state = states.get(node_id)

        if is_passive:
            status = (
                DeviceStatus.OFFLINE
                if node_id in rc_incident_by_node
                else DeviceStatus.ONLINE
            )
        elif state is not None:
            status = state.status
        else:
            status = DeviceStatus.UNKNOWN

        if not is_passive:
            if status == DeviceStatus.ONLINE:
                online_count += 1
            elif status == DeviceStatus.OFFLINE:
                offline_count += 1

        if node_type == NodeType.SENSOR:
            sensor_total += 1
            if status == DeviceStatus.ONLINE:
                sensor_online += 1

        nodes.append(
            TopologyNodeView(
                id=node_id,
                node_type=node_type,
                name=row["name"],
                zone_id=row.get("zone_id"),
                status=status,
                is_passive=is_passive,
                criticality=row.get("criticality"),
                last_seen=state.last_seen if state else None,
                battery_pct=state.battery_pct if state else None,
                metrics=state.metrics if state else {},
                root_cause_incident_id=rc_incident_by_node.get(node_id),
            )
        )

    raw_alerts = sum(inc.raw_alerts_equivalent for inc in open_incidents)
    suppressed = sum(inc.suppressed_alerts_count for inc in open_incidents)
    noise_reduction = round((suppressed / raw_alerts) * 100.0, 1) if raw_alerts > 0 else 0.0

    return TopologySnapshot(
        generated_at=datetime.now(UTC),
        active_scenario=scenario,
        nodes=tuple(nodes),
        edges=tuple(edges),
        open_incidents=tuple(open_incidents),
        stats={
            "total_nodes": len(nodes),
            "active_devices_online": online_count,
            "active_devices_offline": offline_count,
            "sensors_total": sensor_total,
            "sensors_online": sensor_online,
            "open_incidents_count": len(open_incidents),
            "raw_alerts_equivalent": raw_alerts,
            "suppressed_alerts": suppressed,
            "noise_reduction_pct": noise_reduction,
        },
    )


def create_app(
    *,
    settings: Settings | None = None,
    redis_client: Redis | None = None,
    neo4j_driver: AsyncDriver | None = None,
) -> FastAPI:
    """Create configured FastAPI app with optional injected clients for testing."""
    app_settings = settings or get_settings()
    owns_clients = redis_client is None or neo4j_driver is None

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        r_client = redis_client or Redis.from_url(app_settings.redis_url, decode_responses=False)
        n_driver = neo4j_driver or AsyncGraphDatabase.driver(
            app_settings.neo4j_uri,
            auth=(app_settings.neo4j_user, app_settings.neo4j_password),
        )
        redis_store = RedisStateStore(r_client)
        neo4j_repo = Neo4jTopologyRepo(n_driver)

        if owns_clients:
            for attempt in range(1, 21):
                try:
                    if await redis_store.ping() and await neo4j_repo.ping():
                        break
                except Exception as exc:
                    logger.info("API waiting for Redis/Neo4j (%d/20): %s", attempt, exc)
                    await asyncio.sleep(2.0)

            seed = load_topology_seed(app_settings.topology_seed_path)
            await neo4j_repo.ensure_schema_and_seed(seed)

        app.state.settings = app_settings
        app.state.redis_store = redis_store
        app.state.neo4j_repo = neo4j_repo
        app.state.redis_client = r_client

        try:
            yield
        finally:
            if owns_clients:
                await n_driver.close()
                await r_client.aclose()

    app = FastAPI(
        title="IoT Topology RCA",
        description="Fault Correlation & Root Cause Analysis using Redis + Neo4j + MQTT",
        version="0.1.0",
        lifespan=lifespan,
    )

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/health")
    async def health() -> dict[str, Any]:
        redis_ok = await app.state.redis_store.ping()
        neo4j_ok = await app.state.neo4j_repo.ping()
        if not (redis_ok and neo4j_ok):
            raise HTTPException(status_code=503, detail="Backing stores unhealthy")
        return {"status": "ok", "redis": redis_ok, "neo4j": neo4j_ok}

    @app.get("/api/v1/topology", response_model=TopologySnapshot)
    async def get_topology() -> TopologySnapshot:
        return await build_topology_snapshot(app.state.redis_store, app.state.neo4j_repo)

    @app.get("/api/v1/incidents", response_model=list[StoredIncident])
    async def list_incidents(
        status: str | None = Query(default=None, pattern="^(OPEN|RESOLVED)$"),
        limit: int = Query(default=50, ge=1, le=200),
    ) -> list[StoredIncident]:
        return await app.state.neo4j_repo.list_incidents(status=status, limit=limit)

    @app.post("/api/v1/rca/evaluate")
    async def trigger_rca_evaluation() -> dict[str, Any]:
        evaluation = await run_rca_cycle(
            app.state.redis_store,
            app.state.neo4j_repo,
            app.state.settings,
        )
        return evaluation.model_dump(mode="json")

    @app.get("/api/v1/blast-radius/{node_id}", response_model=BlastRadiusReport)
    async def blast_radius(node_id: str) -> BlastRadiusReport:
        report = await app.state.neo4j_repo.get_blast_radius(node_id)
        if report is None:
            raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found in topology")
        return report

    @app.get("/api/v1/events/stream")
    async def recent_state_events(
        count: int = Query(default=30, ge=1, le=100),
    ) -> list[dict[str, str]]:
        return await app.state.redis_store.get_recent_state_events(count=count)

    @app.post("/api/v1/chaos", response_model=ChaosCommandResponse)
    async def set_chaos_scenario(body: ChaosCommandRequest) -> ChaosCommandResponse:
        await app.state.redis_store.set_active_chaos_scenario(body.scenario)
        try:
            async with aiomqtt.Client(
                hostname=app.state.settings.mqtt_host,
                port=app.state.settings.mqtt_port,
                identifier="iot-rca-api-chaos",
            ) as client:
                await client.publish(
                    CHAOS_COMMAND_TOPIC,
                    json.dumps({"scenario": body.scenario.value}),
                    qos=1,
                    retain=True,
                )
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to dispatch chaos command to MQTT broker: {exc}",
            ) from exc

        await app.state.redis_store.publish_live_update(
            {"type": "chaos_change", "scenario": body.scenario.value}
        )
        return ChaosCommandResponse(
            scenario=body.scenario,
            message=f"Dispatched chaos scenario '{body.scenario.value}' to simulator.",
        )

    @app.websocket("/ws/live")
    async def websocket_live(websocket: WebSocket) -> None:
        await websocket.accept()
        pubsub = app.state.redis_client.pubsub()
        await pubsub.subscribe(app.state.redis_store.live_pubsub_channel)
        trigger_event = asyncio.Event()
        trigger_event.set()

        async def _listen_pubsub() -> None:
            async for msg in pubsub.listen():
                if msg.get("type") == "message":
                    trigger_event.set()

        async def _sender_loop() -> None:
            while True:
                with suppress(TimeoutError):
                    await asyncio.wait_for(trigger_event.wait(), timeout=2.0)
                trigger_event.clear()
                snapshot = await build_topology_snapshot(
                    app.state.redis_store,
                    app.state.neo4j_repo,
                )
                await websocket.send_text(snapshot.model_dump_json())

        listen_task = asyncio.create_task(_listen_pubsub())
        sender_task = asyncio.create_task(_sender_loop())
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            listen_task.cancel()
            sender_task.cancel()
            with suppress(asyncio.CancelledError):
                await listen_task
            with suppress(asyncio.CancelledError):
                await sender_task
            await pubsub.unsubscribe(app.state.redis_store.live_pubsub_channel)
            await pubsub.aclose()

    return app


def main() -> None:
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        create_app(settings=settings),
        host=settings.api_host,
        port=settings.api_port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
