"""Immutable Pydantic domain models for topology, telemetry, RCA, and incidents."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class NodeType(StrEnum):
    POWER_PANEL = "PowerPanel"
    NETWORK_SWITCH = "NetworkSwitch"
    GATEWAY = "Gateway"
    SENSOR = "Sensor"
    ZONE = "Zone"


class DeviceStatus(StrEnum):
    ONLINE = "online"
    OFFLINE = "offline"
    UNKNOWN = "unknown"


class Criticality(StrEnum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ChaosScenario(StrEnum):
    HEALTHY = "healthy"
    SINGLE_SENSOR = "single_sensor"
    GATEWAY_FAILURE = "gateway_failure"
    SWITCH_FAILURE = "switch_failure"
    POWER_PANEL_FAILURE = "power_panel_failure"


class ZoneSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    floor: str


class PowerPanelSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    circuit: str
    zone_id: str


class SwitchSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    ip: str
    powered_by: str
    zone_id: str


class GatewaySpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    protocol: str
    powered_by: str
    uplink_via: str | None = None
    zone_id: str


class SensorSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    sensor_type: str
    criticality: Criticality
    firmware: str
    gateway_id: str
    zone_id: str


class TopologySeed(BaseModel):
    model_config = ConfigDict(frozen=True)

    zones: tuple[ZoneSpec, ...]
    power_panels: tuple[PowerPanelSpec, ...]
    switches: tuple[SwitchSpec, ...]
    gateways: tuple[GatewaySpec, ...]
    sensors: tuple[SensorSpec, ...]


class TelemetryPayload(BaseModel):
    """Validated MQTT heartbeat/telemetry message published by IoT devices."""

    model_config = ConfigDict(frozen=True)

    device_id: str = Field(min_length=1, max_length=128)
    device_type: NodeType
    seq: int = Field(ge=0)
    ts: datetime = Field(default_factory=lambda: datetime.now(UTC))
    battery_pct: float | None = Field(default=None, ge=0.0, le=100.0)
    metrics: dict[str, float | int | str | bool] = Field(default_factory=dict)


class DeviceRuntimeState(BaseModel):
    """Live device heartbeat snapshot stored in Redis."""

    model_config = ConfigDict(frozen=True)

    device_id: str
    device_type: NodeType
    status: DeviceStatus
    last_seen: datetime | None = None
    battery_pct: float | None = None
    seq: int | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)


class CandidateExplanation(BaseModel):
    """Topology node candidate evaluated by the RCA engine."""

    model_config = ConfigDict(frozen=True)

    node_id: str
    node_type: NodeType
    name: str
    is_passive: bool = False
    downstream_sensor_ids: frozenset[str] = Field(default_factory=frozenset)
    downstream_infra_ids: frozenset[str] = Field(default_factory=frozenset)
    online_child_infra_count: int = Field(default=0, ge=0)
    highest_criticality: Criticality = Criticality.MEDIUM


class RootCauseIncident(BaseModel):
    """Root cause incident computed by correlating Redis offline state with Neo4j topology."""

    model_config = ConfigDict(frozen=True)

    root_cause_id: str
    root_cause_type: NodeType
    root_cause_name: str
    severity: Criticality
    affected_sensor_ids: tuple[str, ...]
    affected_infra_ids: tuple[str, ...]
    raw_alerts_equivalent: int = Field(ge=1)
    suppressed_alerts_count: int = Field(ge=0)
    coverage_ratio: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    summary: str


class RcaEvaluation(BaseModel):
    """Full evaluation pass produced by the RCA engine."""

    model_config = ConfigDict(frozen=True)

    evaluated_at: datetime
    offline_sensor_count: int
    offline_infra_count: int
    incidents: tuple[RootCauseIncident, ...]
    total_raw_alerts: int
    total_suppressed_alerts: int
    noise_reduction_pct: float


class StoredIncident(BaseModel):
    """Incident persisted in Neo4j for active/historical tracking."""

    model_config = ConfigDict(frozen=True)

    id: str
    status: str
    root_cause_id: str
    root_cause_type: str
    root_cause_name: str
    severity: str
    affected_sensor_ids: tuple[str, ...]
    affected_infra_ids: tuple[str, ...]
    raw_alerts_equivalent: int
    suppressed_alerts_count: int
    coverage_ratio: float
    confidence: float
    summary: str
    opened_at: datetime
    resolved_at: datetime | None = None
    resolution_reason: str | None = None


class BlastRadiusItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    node_type: NodeType
    name: str
    criticality: Criticality | None = None
    zone_id: str | None = None
    hops: int = Field(ge=1)


class BlastRadiusReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    target_id: str
    target_type: NodeType
    target_name: str
    affected_infra_count: int
    affected_sensor_count: int
    criticality_breakdown: dict[str, int]
    affected_nodes: tuple[BlastRadiusItem, ...]


class TopologyNodeView(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    node_type: NodeType
    name: str
    zone_id: str | None = None
    status: DeviceStatus
    is_passive: bool = False
    criticality: Criticality | None = None
    last_seen: datetime | None = None
    battery_pct: float | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)
    root_cause_incident_id: str | None = None


class TopologyEdgeView(BaseModel):
    model_config = ConfigDict(frozen=True)

    source: str
    target: str
    relationship: str


class TopologySnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    generated_at: datetime
    active_scenario: ChaosScenario
    nodes: tuple[TopologyNodeView, ...]
    edges: tuple[TopologyEdgeView, ...]
    open_incidents: tuple[StoredIncident, ...]
    stats: dict[str, Any]
