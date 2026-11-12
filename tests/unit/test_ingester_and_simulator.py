"""Unit tests for MQTT telemetry parsing, chaos simulator muting, and API health/routes."""

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from iot_rca.api import create_app
from iot_rca.config import Settings
from iot_rca.models import (
    BlastRadiusReport,
    ChaosScenario,
    NodeType,
    TopologySeed,
)
from iot_rca.mqtt_ingester import parse_telemetry_message
from iot_rca.simulator import compute_muted_devices


def _load_seed() -> TopologySeed:
    seed_path = Path(__file__).resolve().parents[2] / "topology" / "seed_topology.json"
    return TopologySeed.model_validate(json.loads(seed_path.read_text(encoding="utf-8")))


def test_parse_telemetry_message_valid_and_invalid() -> None:
    raw = json.dumps(
        {
            "device_id": "sns-temp-01",
            "device_type": "Sensor",
            "seq": 7,
            "battery_pct": 91.5,
            "metrics": {"temperature_c": 23.1},
        }
    ).encode("utf-8")
    parsed = parse_telemetry_message(raw)
    assert parsed.device_id == "sns-temp-01"
    assert parsed.seq == 7

    with pytest.raises(ValueError, match="Invalid telemetry JSON"):
        parse_telemetry_message(b"{not-json")

    with pytest.raises(ValueError, match="Telemetry schema validation failed"):
        parse_telemetry_message(b'{"device_id": "sns-temp-01", "seq": -5}')


def test_compute_muted_devices_across_chaos_scenarios() -> None:
    seed = _load_seed()
    assert compute_muted_devices(seed, ChaosScenario.HEALTHY) == frozenset()
    assert compute_muted_devices(seed, ChaosScenario.SINGLE_SENSOR) == frozenset({"sns-temp-01"})

    gw_muted = compute_muted_devices(seed, ChaosScenario.GATEWAY_FAILURE)
    assert "gw-lora-01" in gw_muted
    assert len(gw_muted) == 9  # 1 gateway + 8 sensors

    sw_muted = compute_muted_devices(seed, ChaosScenario.SWITCH_FAILURE)
    assert "sw-core-01" in sw_muted
    assert "gw-lte-01" not in sw_muted
    assert len(sw_muted) == 17  # 1 switch + 2 gateways + 14 sensors

    panel_muted = compute_muted_devices(seed, ChaosScenario.POWER_PANEL_FAILURE)
    assert "gw-lte-01" in panel_muted
    assert "sw-core-01" in panel_muted
    assert len(panel_muted) == 22  # 1 switch + 3 gateways + 18 sensors


def test_api_blast_radius_and_health_endpoints_with_mocked_stores() -> None:
    settings = Settings(neo4j_password="test-only-password")
    mock_redis = AsyncMock()
    mock_neo4j = AsyncMock()

    app = create_app(
        settings=settings,
        redis_client=mock_redis,
        neo4j_driver=mock_neo4j,
    )
    with TestClient(app) as client:
        app.state.redis_store.ping = AsyncMock(return_value=True)
        app.state.neo4j_repo.ping = AsyncMock(return_value=True)
        app.state.neo4j_repo.get_blast_radius = AsyncMock(
            return_value=BlastRadiusReport(
                target_id="gw-lora-01",
                target_type=NodeType.GATEWAY,
                target_name="LoRaWAN Gateway North",
                affected_infra_count=0,
                affected_sensor_count=8,
                criticality_breakdown={"critical": 0, "high": 1, "medium": 3, "low": 4},
                affected_nodes=(),
            )
        )

        health_res = client.get("/health")
        assert health_res.status_code == 200
        assert health_res.json() == {"status": "ok", "redis": True, "neo4j": True}

        blast_res = client.get("/api/v1/blast-radius/gw-lora-01")
        assert blast_res.status_code == 200
        assert blast_res.json()["affected_sensor_count"] == 8

        app.state.neo4j_repo.get_blast_radius = AsyncMock(return_value=None)
        missing_res = client.get("/api/v1/blast-radius/does-not-exist")
        assert missing_res.status_code == 404
