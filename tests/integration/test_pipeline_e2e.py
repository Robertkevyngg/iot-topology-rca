"""End-to-end integration tests against real Redis and Neo4j instances."""

import os
from datetime import UTC, datetime

import httpx
import pytest
from neo4j import AsyncGraphDatabase
from redis.asyncio import Redis

from iot_rca.api import create_app
from iot_rca.config import Settings
from iot_rca.models import ChaosScenario, NodeType, TelemetryPayload
from iot_rca.mqtt_ingester import load_topology_seed, run_rca_cycle
from iot_rca.neo4j_repo import Neo4jTopologyRepo
from iot_rca.redis_store import RedisStateStore
from iot_rca.simulator import compute_muted_devices

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION_TESTS") != "1",
    reason="Set RUN_INTEGRATION_TESTS=1 with live Redis and Neo4j to execute E2E tests.",
)


@pytest.mark.asyncio
async def test_full_redis_neo4j_rca_escalation_and_recovery_lifecycle() -> None:
    settings = Settings(
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        neo4j_uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_user=os.getenv("NEO4J_USER", "neo4j"),
        neo4j_password=os.environ["NEO4J_PASSWORD"],
    )
    seed = load_topology_seed(settings.topology_seed_path)

    redis_client = Redis.from_url(settings.redis_url, decode_responses=False)
    neo4j_driver = AsyncGraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )

    try:
        await redis_client.flushdb()
        async with neo4j_driver.session() as session:
            await session.run("MATCH (n) DETACH DELETE n")

        redis_store = RedisStateStore(redis_client)
        neo4j_repo = Neo4jTopologyRepo(neo4j_driver)
        await redis_store.ensure_stream_group()
        await neo4j_repo.ensure_schema_and_seed(seed)

        # 1. Verify Blast Radius queries on passive PowerPanel and physical Zone
        blast = await neo4j_repo.get_blast_radius("pp-main-a")
        assert blast is not None
        assert blast.affected_sensor_count == 18
        assert blast.affected_infra_count == 4

        zone_blast = await neo4j_repo.get_blast_radius("zone-cold-vault")
        assert zone_blast is not None
        assert zone_blast.target_type == NodeType.ZONE
        assert zone_blast.affected_sensor_count == 4

        nodes_raw, edges_raw = await neo4j_repo.fetch_graph_structure()
        assert len(nodes_raw) == 39
        assert len(edges_raw) > 30

        # 2. Record heartbeats for the whole fleet at t=1000
        base_epoch = 1000.0
        ttl = 8.0
        seq = 1

        async def send_heartbeats(excluded: frozenset[str], at_epoch: float, seq_num: int) -> None:
            now_dt = datetime.fromtimestamp(at_epoch, tz=UTC)
            for sw in seed.switches:
                if sw.id not in excluded:
                    await redis_store.record_heartbeat(
                        TelemetryPayload(
                            device_id=sw.id,
                            device_type=NodeType.NETWORK_SWITCH,
                            seq=seq_num,
                            ts=now_dt,
                        ),
                        ttl_seconds=ttl,
                        now_epoch=at_epoch,
                    )
            for gw in seed.gateways:
                if gw.id not in excluded:
                    await redis_store.record_heartbeat(
                        TelemetryPayload(
                            device_id=gw.id,
                            device_type=NodeType.GATEWAY,
                            seq=seq_num,
                            ts=now_dt,
                        ),
                        ttl_seconds=ttl,
                        now_epoch=at_epoch,
                    )
            for sns in seed.sensors:
                if sns.id not in excluded:
                    await redis_store.record_heartbeat(
                        TelemetryPayload(
                            device_id=sns.id,
                            device_type=NodeType.SENSOR,
                            seq=seq_num,
                            ts=now_dt,
                        ),
                        ttl_seconds=ttl,
                        now_epoch=at_epoch,
                    )

        await send_heartbeats(frozenset(), base_epoch, seq)
        # Verify idempotency: duplicate (device_id, seq) must be rejected
        dup_accepted, _ = await redis_store.record_heartbeat(
            TelemetryPayload(
                device_id="sns-temp-01",
                device_type=NodeType.SENSOR,
                seq=seq,
            ),
            ttl_seconds=ttl,
            now_epoch=base_epoch,
        )
        assert dup_accepted is False

        eval_healthy = await run_rca_cycle(redis_store, neo4j_repo, settings)
        assert eval_healthy.incidents == ()

        # 3. Gateway failure: mute gw-lora-01 + 8 sensors at t=1006, sweep at t=1009
        seq += 1
        gw_muted = compute_muted_devices(seed, ChaosScenario.GATEWAY_FAILURE)
        await send_heartbeats(gw_muted, 1006.0, seq)
        expired_gw = await redis_store.sweep_expired_heartbeats(now_epoch=1009.0)
        assert set(expired_gw) == set(gw_muted)

        eval_gw = await run_rca_cycle(redis_store, neo4j_repo, settings)
        assert len(eval_gw.incidents) == 1
        assert eval_gw.incidents[0].root_cause_id == "gw-lora-01"
        assert eval_gw.incidents[0].suppressed_alerts_count == 8

        # 4. Escalate to switch failure (gw-lte-01 stays alive!)
        seq += 1
        sw_muted = compute_muted_devices(seed, ChaosScenario.SWITCH_FAILURE)
        await send_heartbeats(sw_muted, 1012.0, seq)
        await redis_store.sweep_expired_heartbeats(now_epoch=1016.0)

        eval_sw = await run_rca_cycle(redis_store, neo4j_repo, settings)
        assert len(eval_sw.incidents) == 1
        assert eval_sw.incidents[0].root_cause_id == "sw-core-01"
        assert len(eval_sw.incidents[0].affected_sensor_ids) == 14

        resolved_list = await neo4j_repo.list_incidents(status="RESOLVED")
        assert any(
            i.root_cause_id == "gw-lora-01"
            and i.resolution_reason == "escalated_to_upstream_root_cause"
            for i in resolved_list
        )

        # 5. Escalate to passive PowerPanel failure (pp-main-a)
        seq += 1
        panel_muted = compute_muted_devices(seed, ChaosScenario.POWER_PANEL_FAILURE)
        await send_heartbeats(panel_muted, 1018.0, seq)
        await redis_store.sweep_expired_heartbeats(now_epoch=1022.0)

        eval_panel = await run_rca_cycle(redis_store, neo4j_repo, settings)
        assert len(eval_panel.incidents) == 1
        assert eval_panel.incidents[0].root_cause_id == "pp-main-a"
        assert eval_panel.incidents[0].suppressed_alerts_count == 21
        assert eval_panel.noise_reduction_pct >= 95.0

        # 6. Full fleet recovery
        seq += 1
        await send_heartbeats(frozenset(), 1025.0, seq)
        eval_recovered = await run_rca_cycle(redis_store, neo4j_repo, settings)
        assert eval_recovered.incidents == ()
        assert await neo4j_repo.list_incidents(status="OPEN") == []

        stream_events = await redis_store.get_recent_state_events(count=50)
        assert len(stream_events) > 0

        await redis_store.set_active_chaos_scenario(ChaosScenario.POWER_PANEL_FAILURE)
        assert await redis_store.get_active_chaos_scenario() == ChaosScenario.POWER_PANEL_FAILURE
        await redis_store.set_active_chaos_scenario(ChaosScenario.HEALTHY)

        app = create_app(
            settings=settings,
            redis_client=redis_client,
            neo4j_driver=neo4j_driver,
        )
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                assert (await client.get("/")).status_code == 200
                assert (await client.get("/health")).status_code == 200
                topo_res = await client.get("/api/v1/topology")
                assert topo_res.status_code == 200
                assert len(topo_res.json()["nodes"]) == 39
                assert (await client.post("/api/v1/rca/evaluate")).status_code == 200
                assert (await client.get("/api/v1/incidents?status=RESOLVED")).status_code == 200
                assert (await client.get("/api/v1/events/stream?count=5")).status_code == 200
    finally:
        await neo4j_driver.close()
        await redis_client.aclose()
