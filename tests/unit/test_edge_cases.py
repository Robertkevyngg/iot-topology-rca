"""Additional unit tests covering edge cases in RCA, simulator metrics, ingester, and API."""

import asyncio
import random
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from iot_rca.api import build_topology_snapshot, create_app
from iot_rca.config import Settings
from iot_rca.models import (
    CandidateExplanation,
    ChaosScenario,
    Criticality,
    DeviceRuntimeState,
    DeviceStatus,
    NodeType,
    StoredIncident,
    TopologyEdgeView,
)
from iot_rca.mqtt_ingester import MqttIngesterService
from iot_rca.rca_engine import correlate_failures
from iot_rca.simulator import FleetSimulator, build_sensor_metrics


def test_correlate_failures_handles_orphan_devices_not_in_candidates() -> None:
    result = correlate_failures(
        offline_sensor_ids=frozenset({"orphan-sns-1"}),
        offline_infra_ids=frozenset({"orphan-gw-1"}),
        candidates=(),
    )
    assert len(result.incidents) == 2
    ids = {i.root_cause_id for i in result.incidents}
    assert ids == {"orphan-sns-1", "orphan-gw-1"}


def test_correlate_failures_handles_zero_sensor_gateway_offline() -> None:
    candidates = [
        CandidateExplanation(
            node_id="gw-empty",
            node_type=NodeType.GATEWAY,
            name="Empty Gateway",
            downstream_sensor_ids=frozenset(),
            highest_criticality=Criticality.LOW,
        )
    ]
    result = correlate_failures(
        offline_sensor_ids=frozenset(),
        offline_infra_ids=frozenset({"gw-empty"}),
        candidates=candidates,
    )
    assert len(result.incidents) == 1
    assert result.incidents[0].root_cause_id == "gw-empty"
    assert result.incidents[0].suppressed_alerts_count == 0


def test_build_sensor_metrics_for_all_known_and_fallback_types() -> None:
    rng = random.Random(1)
    assert "temperature_c" in build_sensor_metrics("temperature", rng)
    assert build_sensor_metrics("cryo_temp", rng)["temperature_c"] < 0
    assert "humidity_pct" in build_sensor_metrics("humidity", rng)
    assert "power_kw" in build_sensor_metrics("power_meter", rng)
    assert "vibration_rms" in build_sensor_metrics("vibration", rng)
    assert build_sensor_metrics("door_contact", rng)["door_state"] == "closed"
    assert "value" in build_sensor_metrics("unknown_custom", rng)


@pytest.mark.asyncio
async def test_ingester_handle_raw_message_drops_invalid_and_marks_dirty_on_transition() -> None:
    settings = Settings(neo4j_password="test-password")
    redis_store = AsyncMock()
    neo4j_repo = AsyncMock()
    service = MqttIngesterService(settings, redis_store, neo4j_repo)

    assert await service.handle_raw_message(b"not-json") is False
    redis_store.record_heartbeat.assert_not_called()

    redis_store.record_heartbeat = AsyncMock(return_value=(True, True))
    valid_msg = (
        '{"device_id":"sns-temp-01","device_type":"Sensor","seq":1,"metrics":{"temperature_c":22}}'
    )
    assert await service.handle_raw_message(valid_msg) is True
    assert service._dirty_event.is_set() is True


@pytest.mark.asyncio
async def test_build_topology_snapshot_joins_neo4j_and_redis_state() -> None:
    redis_store = AsyncMock()
    neo4j_repo = AsyncMock()

    neo4j_repo.fetch_graph_structure = AsyncMock(
        return_value=(
            [
                {
                    "id": "pp-main-a",
                    "node_type": "PowerPanel",
                    "name": "Main Panel A",
                    "zone_id": "zone-utilities",
                    "is_passive": True,
                    "criticality": None,
                },
                {
                    "id": "gw-lora-01",
                    "node_type": "Gateway",
                    "name": "LoRa GW",
                    "zone_id": "zone-assembly-a",
                    "is_passive": False,
                    "criticality": None,
                },
                {
                    "id": "sns-temp-01",
                    "node_type": "Sensor",
                    "name": "sns-temp-01",
                    "zone_id": "zone-assembly-a",
                    "is_passive": False,
                    "criticality": "high",
                },
            ],
            [
                TopologyEdgeView(
                    source="sns-temp-01", target="gw-lora-01", relationship="CONNECTED_TO"
                ),
                TopologyEdgeView(
                    source="gw-lora-01", target="pp-main-a", relationship="POWERED_BY"
                ),
            ],
        )
    )
    now = datetime.now(UTC)
    redis_store.get_all_device_states = AsyncMock(
        return_value={
            "gw-lora-01": DeviceRuntimeState(
                device_id="gw-lora-01",
                device_type=NodeType.GATEWAY,
                status=DeviceStatus.OFFLINE,
                last_seen=now,
            ),
            "sns-temp-01": DeviceRuntimeState(
                device_id="sns-temp-01",
                device_type=NodeType.SENSOR,
                status=DeviceStatus.OFFLINE,
                last_seen=now,
                battery_pct=92.0,
            ),
        }
    )
    neo4j_repo.list_incidents = AsyncMock(
        return_value=[
            StoredIncident(
                id="INC-12345678",
                status="OPEN",
                root_cause_id="gw-lora-01",
                root_cause_type="Gateway",
                root_cause_name="LoRa GW",
                severity="high",
                affected_sensor_ids=("sns-temp-01",),
                affected_infra_ids=("gw-lora-01",),
                raw_alerts_equivalent=2,
                suppressed_alerts_count=1,
                coverage_ratio=1.0,
                confidence=1.0,
                summary="Gateway failed",
                opened_at=now,
            )
        ]
    )
    redis_store.get_active_chaos_scenario = AsyncMock(return_value=ChaosScenario.GATEWAY_FAILURE)

    snapshot = await build_topology_snapshot(redis_store, neo4j_repo)
    assert snapshot.active_scenario == ChaosScenario.GATEWAY_FAILURE
    assert snapshot.stats["open_incidents_count"] == 1
    assert snapshot.stats["suppressed_alerts"] == 1
    assert snapshot.stats["noise_reduction_pct"] == 50.0


def test_api_incidents_and_events_endpoints() -> None:
    settings = Settings(neo4j_password="test-password")
    app = create_app(
        settings=settings,
        redis_client=AsyncMock(),
        neo4j_driver=AsyncMock(),
    )
    with TestClient(app) as client:
        app.state.neo4j_repo.list_incidents = AsyncMock(return_value=[])
        app.state.redis_store.get_recent_state_events = AsyncMock(
            return_value=[{"device_id": "sns-temp-01", "event": "ONLINE"}]
        )
        res_inc = client.get("/api/v1/incidents?status=OPEN")
        assert res_inc.status_code == 200
        assert res_inc.json() == []

        res_ev = client.get("/api/v1/events/stream?count=10")
        assert res_ev.status_code == 200
        assert res_ev.json()[0]["device_id"] == "sns-temp-01"

        app.state.redis_store.ping = AsyncMock(return_value=False)
        app.state.neo4j_repo.ping = AsyncMock(return_value=True)
        assert client.get("/health").status_code == 503

        app.state.redis_store.set_active_chaos_scenario = AsyncMock()
        app.state.redis_store.publish_live_update = AsyncMock()
        mock_mqtt_client = AsyncMock()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_mqtt_client)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("iot_rca.api.aiomqtt.Client", return_value=mock_cm):
            chaos_res = client.post("/api/v1/chaos", json={"scenario": "gateway_failure"})
        assert chaos_res.status_code == 200
        assert chaos_res.json()["scenario"] == "gateway_failure"
        mock_mqtt_client.publish.assert_awaited_once()

        failing_cm = MagicMock()
        failing_cm.__aenter__ = AsyncMock(side_effect=OSError("broker down"))
        failing_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("iot_rca.api.aiomqtt.Client", return_value=failing_cm):
            err_res = client.post("/api/v1/chaos", json={"scenario": "healthy"})
        assert err_res.status_code == 502


@pytest.mark.asyncio
async def test_simulator_publish_cycle_and_command_handling() -> None:
    from iot_rca.mqtt_ingester import load_topology_seed

    settings = Settings(neo4j_password="test-password", simulator_interval_seconds=0.3)
    seed = load_topology_seed(settings.topology_seed_path)
    sim = FleetSimulator(settings, seed)
    sim.set_scenario(ChaosScenario.GATEWAY_FAILURE)
    assert "gw-lora-01" in sim._muted
    assert len(sim._muted) == 9

    mock_client = AsyncMock()
    with (
        patch("iot_rca.simulator.asyncio.sleep", side_effect=asyncio.CancelledError),
        pytest.raises(asyncio.CancelledError),
    ):
        await sim._publish_loop(mock_client)
    # 37 total active devices - 9 muted = 28 published telemetry messages
    assert mock_client.publish.await_count == 28

    # Also test command listener parsing
    msg_ok = MagicMock()
    msg_ok.topic = "iot/chaos/command"
    msg_ok.payload = b'{"scenario": "switch_failure"}'
    msg_other = MagicMock()
    msg_other.topic = "iot/other"
    msg_other.payload = b"{}"

    async def _fake_messages():
        yield msg_other
        yield msg_ok

    cmd_client = AsyncMock()
    cmd_client.messages = _fake_messages()
    await sim._listen_commands(cmd_client)
    assert sim._scenario == ChaosScenario.SWITCH_FAILURE

