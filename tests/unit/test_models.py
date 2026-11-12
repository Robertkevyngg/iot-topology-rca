"""Unit tests for domain models and seed topology integrity."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from iot_rca.models import NodeType, TelemetryPayload, TopologySeed


def test_seed_topology_file_is_valid_and_referentially_consistent() -> None:
    seed_path = Path(__file__).resolve().parents[2] / "topology" / "seed_topology.json"
    raw = json.loads(seed_path.read_text(encoding="utf-8"))
    seed = TopologySeed.model_validate(raw)

    zone_ids = {z.id for z in seed.zones}
    panel_ids = {p.id for p in seed.power_panels}
    switch_ids = {s.id for s in seed.switches}
    gateway_ids = {g.id for g in seed.gateways}

    assert len(seed.sensors) == 30
    assert len(seed.gateways) == 5
    assert len(seed.switches) == 2
    assert len(seed.power_panels) == 2

    for panel in seed.power_panels:
        assert panel.zone_id in zone_ids
    for sw in seed.switches:
        assert sw.powered_by in panel_ids
        assert sw.zone_id in zone_ids
    for gw in seed.gateways:
        assert gw.powered_by in panel_ids
        assert gw.zone_id in zone_ids
        if gw.uplink_via is not None:
            assert gw.uplink_via in switch_ids
    for sensor in seed.sensors:
        assert sensor.gateway_id in gateway_ids
        assert sensor.zone_id in zone_ids


def test_telemetry_payload_is_frozen_and_validates_bounds() -> None:
    payload = TelemetryPayload(
        device_id="sns-temp-01",
        device_type=NodeType.SENSOR,
        seq=42,
        battery_pct=88.5,
        metrics={"temperature_c": 24.3},
    )
    assert payload.device_id == "sns-temp-01"

    with pytest.raises(ValidationError):
        payload.seq = 43  # type: ignore[misc]

    with pytest.raises(ValidationError):
        TelemetryPayload(
            device_id="sns-temp-01",
            device_type=NodeType.SENSOR,
            seq=-1,
        )

    with pytest.raises(ValidationError):
        TelemetryPayload(
            device_id="sns-temp-01",
            device_type=NodeType.SENSOR,
            seq=1,
            battery_pct=120.0,
        )
