"""Unit tests for the pure RCA fault correlation engine."""

from iot_rca.models import CandidateExplanation, Criticality, NodeType
from iot_rca.rca_engine import correlate_failures


def _build_factory_candidates(
    *,
    online_infra: set[str] | None = None,
) -> list[CandidateExplanation]:
    """Construct realistic candidate graph matching seed_topology.json Wing A."""
    online = online_infra or set()

    gw_lora_sensors = frozenset(f"sns-lora-{i}" for i in range(1, 9))
    gw_modbus_sensors = frozenset(f"sns-modbus-{i}" for i in range(1, 7))
    gw_lte_sensors = frozenset(f"sns-lte-{i}" for i in range(1, 5))

    wing_a_sensors = gw_lora_sensors | gw_modbus_sensors | gw_lte_sensors
    sw_01_sensors = gw_lora_sensors | gw_modbus_sensors

    candidates = [
        CandidateExplanation(
            node_id="pp-main-a",
            node_type=NodeType.POWER_PANEL,
            name="Main Panel A",
            is_passive=True,
            downstream_sensor_ids=wing_a_sensors,
            downstream_infra_ids=frozenset(
                {"sw-core-01", "gw-lora-01", "gw-modbus-01", "gw-lte-01"}
            ),
            online_child_infra_count=len(
                {"sw-core-01", "gw-lora-01", "gw-modbus-01", "gw-lte-01"} & online
            ),
            highest_criticality=Criticality.CRITICAL,
        ),
        CandidateExplanation(
            node_id="sw-core-01",
            node_type=NodeType.NETWORK_SWITCH,
            name="Industrial Switch 01",
            downstream_sensor_ids=sw_01_sensors,
            downstream_infra_ids=frozenset({"gw-lora-01", "gw-modbus-01"}),
            online_child_infra_count=len({"gw-lora-01", "gw-modbus-01"} & online),
            highest_criticality=Criticality.CRITICAL,
        ),
        CandidateExplanation(
            node_id="gw-lora-01",
            node_type=NodeType.GATEWAY,
            name="LoRaWAN Gateway North",
            downstream_sensor_ids=gw_lora_sensors,
            highest_criticality=Criticality.HIGH,
        ),
        CandidateExplanation(
            node_id="gw-modbus-01",
            node_type=NodeType.GATEWAY,
            name="Modbus TCP Concentrator",
            downstream_sensor_ids=gw_modbus_sensors,
            highest_criticality=Criticality.CRITICAL,
        ),
        CandidateExplanation(
            node_id="gw-lte-01",
            node_type=NodeType.GATEWAY,
            name="Cold Vault Cellular Gateway",
            downstream_sensor_ids=gw_lte_sensors,
            highest_criticality=Criticality.CRITICAL,
        ),
    ]

    for s_id in sorted(wing_a_sensors):
        candidates.append(
            CandidateExplanation(
                node_id=s_id,
                node_type=NodeType.SENSOR,
                name=s_id,
                downstream_sensor_ids=frozenset({s_id}),
                highest_criticality=Criticality.MEDIUM,
            )
        )
    return candidates


def test_healthy_topology_produces_zero_incidents() -> None:
    result = correlate_failures(
        offline_sensor_ids=frozenset(),
        offline_infra_ids=frozenset(),
        candidates=_build_factory_candidates(
            online_infra={"sw-core-01", "gw-lora-01", "gw-modbus-01", "gw-lte-01"}
        ),
    )
    assert result.incidents == ()
    assert result.total_raw_alerts == 0
    assert result.noise_reduction_pct == 0.0


def test_single_sensor_fault_is_not_escalated_to_healthy_gateway() -> None:
    candidates = _build_factory_candidates(
        online_infra={"sw-core-01", "gw-lora-01", "gw-modbus-01", "gw-lte-01"}
    )
    result = correlate_failures(
        offline_sensor_ids=frozenset({"sns-lora-1"}),
        offline_infra_ids=frozenset(),
        candidates=candidates,
    )
    assert len(result.incidents) == 1
    inc = result.incidents[0]
    assert inc.root_cause_id == "sns-lora-1"
    assert inc.root_cause_type == NodeType.SENSOR
    assert inc.suppressed_alerts_count == 0


def test_gateway_failure_collapses_all_child_sensors_into_one_incident() -> None:
    candidates = _build_factory_candidates(
        online_infra={"sw-core-01", "gw-modbus-01", "gw-lte-01"}
    )
    offline_sensors = frozenset(f"sns-lora-{i}" for i in range(1, 9))
    result = correlate_failures(
        offline_sensor_ids=offline_sensors,
        offline_infra_ids=frozenset({"gw-lora-01"}),
        candidates=candidates,
    )
    assert len(result.incidents) == 1
    inc = result.incidents[0]
    assert inc.root_cause_id == "gw-lora-01"
    assert inc.root_cause_type == NodeType.GATEWAY
    assert len(inc.affected_sensor_ids) == 8
    assert inc.raw_alerts_equivalent == 9
    assert inc.suppressed_alerts_count == 8
    assert result.noise_reduction_pct > 85.0


def test_switch_failure_distinguishes_switch_from_power_panel_using_lte_gateway() -> None:
    # gw-lte-01 is still online, proving pp-main-a has power while sw-core-01 failed!
    candidates = _build_factory_candidates(online_infra={"gw-lte-01"})
    offline_sensors = frozenset(
        [f"sns-lora-{i}" for i in range(1, 9)] + [f"sns-modbus-{i}" for i in range(1, 7)]
    )
    result = correlate_failures(
        offline_sensor_ids=offline_sensors,
        offline_infra_ids=frozenset({"sw-core-01", "gw-lora-01", "gw-modbus-01"}),
        candidates=candidates,
    )
    assert len(result.incidents) == 1
    inc = result.incidents[0]
    assert inc.root_cause_id == "sw-core-01"
    assert inc.root_cause_type == NodeType.NETWORK_SWITCH
    assert len(inc.affected_sensor_ids) == 14
    assert inc.suppressed_alerts_count == 16  # 14 sensors + 2 gateways + 1 switch - 1


def test_passive_power_panel_failure_pinpoints_panel_without_heartbeat() -> None:
    candidates = _build_factory_candidates(online_infra=set())
    offline_sensors = frozenset(
        [f"sns-lora-{i}" for i in range(1, 9)]
        + [f"sns-modbus-{i}" for i in range(1, 7)]
        + [f"sns-lte-{i}" for i in range(1, 5)]
    )
    result = correlate_failures(
        offline_sensor_ids=offline_sensors,
        offline_infra_ids=frozenset({"sw-core-01", "gw-lora-01", "gw-modbus-01", "gw-lte-01"}),
        candidates=candidates,
    )
    assert len(result.incidents) == 1
    inc = result.incidents[0]
    assert inc.root_cause_id == "pp-main-a"
    assert inc.root_cause_type == NodeType.POWER_PANEL
    assert inc.severity == Criticality.CRITICAL
    assert len(inc.affected_sensor_ids) == 18
    assert inc.suppressed_alerts_count == 21


def test_ttl_jitter_tolerance_correlates_gateway_at_75_pct_coverage() -> None:
    # 6 of 8 sensors expired first while gw-lora-01 is offline -> immediately blames gw-lora-01
    candidates = _build_factory_candidates(
        online_infra={"sw-core-01", "gw-modbus-01", "gw-lte-01"}
    )
    result = correlate_failures(
        offline_sensor_ids=frozenset(f"sns-lora-{i}" for i in range(1, 7)),
        offline_infra_ids=frozenset({"gw-lora-01"}),
        candidates=candidates,
    )
    assert len(result.incidents) == 1
    assert result.incidents[0].root_cause_id == "gw-lora-01"


def test_concurrent_gateway_and_unrelated_sensor_produce_two_clean_incidents() -> None:
    candidates = _build_factory_candidates(
        online_infra={"sw-core-01", "gw-modbus-01", "gw-lte-01"}
    )
    offline_sensors = frozenset([f"sns-lora-{i}" for i in range(1, 9)] + ["sns-modbus-1"])
    result = correlate_failures(
        offline_sensor_ids=offline_sensors,
        offline_infra_ids=frozenset({"gw-lora-01"}),
        candidates=candidates,
    )
    ids = {inc.root_cause_id for inc in result.incidents}
    assert ids == {"gw-lora-01", "sns-modbus-1"}


def test_active_gateway_online_blocks_false_gateway_blame_even_if_sensors_die() -> None:
    # Even if 7/8 sensors fail, if gw-lora-01 is still online, blame individual sensors
    candidates = _build_factory_candidates(
        online_infra={"sw-core-01", "gw-lora-01", "gw-modbus-01", "gw-lte-01"}
    )
    offline_sensors = frozenset(f"sns-lora-{i}" for i in range(1, 8))
    result = correlate_failures(
        offline_sensor_ids=offline_sensors,
        offline_infra_ids=frozenset(),
        candidates=candidates,
    )
    assert len(result.incidents) == 7
    assert all(inc.root_cause_type == NodeType.SENSOR for inc in result.incidents)
