"""Pure deterministic Root Cause Analysis (RCA) correlation engine.

Combines live offline device sets (from Redis) with topology ancestor candidates
(from Neo4j) using a greedy set-cover algorithm to collapse alert storms into
minimal root-cause incidents.
"""

from collections.abc import Sequence
from datetime import UTC, datetime

from iot_rca.models import (
    CandidateExplanation,
    Criticality,
    NodeType,
    RcaEvaluation,
    RootCauseIncident,
)

_HIERARCHY_RANK: dict[NodeType, int] = {
    NodeType.POWER_PANEL: 4,
    NodeType.NETWORK_SWITCH: 3,
    NodeType.GATEWAY: 2,
    NodeType.SENSOR: 1,
    NodeType.ZONE: 0,
}

_CRITICALITY_ORDER: dict[Criticality, int] = {
    Criticality.CRITICAL: 4,
    Criticality.HIGH: 3,
    Criticality.MEDIUM: 2,
    Criticality.LOW: 1,
}


def _is_plausible_candidate(
    candidate: CandidateExplanation,
    offline_sensor_ids: frozenset[str],
    offline_infra_ids: frozenset[str],
    gateway_threshold: float,
    passive_panel_threshold: float,
) -> tuple[bool, frozenset[str], float]:
    """Return (is_plausible, covered_offline_sensors, coverage_ratio)."""
    covered_sensors = candidate.downstream_sensor_ids & offline_sensor_ids
    total_sensors = len(candidate.downstream_sensor_ids)
    coverage = (
        len(covered_sensors) / total_sensors
        if total_sensors > 0
        else (1.0 if candidate.node_id in offline_infra_ids else 0.0)
    )

    if candidate.node_type == NodeType.SENSOR:
        is_off = candidate.node_id in offline_sensor_ids
        return is_off, (frozenset({candidate.node_id}) if is_off else frozenset()), 1.0

    if candidate.node_type in (NodeType.GATEWAY, NodeType.NETWORK_SWITCH):
        self_offline = candidate.node_id in offline_infra_ids
        if not self_offline:
            return False, frozenset(), coverage
        plausible = total_sensors == 0 or coverage >= gateway_threshold
        return plausible, covered_sensors, coverage

    if candidate.node_type == NodeType.POWER_PANEL:
        if candidate.online_child_infra_count > 0:
            return False, frozenset(), coverage
        plausible = len(covered_sensors) >= 2 and coverage >= passive_panel_threshold
        return plausible, covered_sensors, coverage

    return False, frozenset(), 0.0


def _compute_severity(
    candidate: CandidateExplanation,
    affected_sensor_count: int,
) -> Criticality:
    if candidate.node_type == NodeType.POWER_PANEL or affected_sensor_count >= 10:
        return Criticality.CRITICAL
    if candidate.node_type in (NodeType.NETWORK_SWITCH, NodeType.GATEWAY):
        return (
            Criticality.CRITICAL
            if _CRITICALITY_ORDER[candidate.highest_criticality]
            >= _CRITICALITY_ORDER[Criticality.CRITICAL]
            else Criticality.HIGH
        )
    return candidate.highest_criticality


def _build_summary(
    candidate: CandidateExplanation,
    affected_sensor_ids: tuple[str, ...],
    suppressed_count: int,
    coverage: float,
) -> str:
    pct = round(coverage * 100.0, 1)
    if candidate.node_type == NodeType.SENSOR:
        return f"Sensor {candidate.node_id} stopped sending heartbeats (isolated device fault)."
    return (
        f"{candidate.node_type.value} {candidate.node_id} ({candidate.name}) identified as root "
        f"cause ({pct}% downstream offline): {len(affected_sensor_ids)} sensors impacted, "
        f"{suppressed_count} redundant alerts suppressed."
    )


def correlate_failures(
    *,
    offline_sensor_ids: frozenset[str],
    offline_infra_ids: frozenset[str],
    candidates: Sequence[CandidateExplanation],
    gateway_threshold: float = 0.75,
    passive_panel_threshold: float = 0.85,
    evaluated_at: datetime | None = None,
) -> RcaEvaluation:
    """Compute minimal set of root-cause incidents explaining all offline devices."""
    now = evaluated_at or datetime.now(UTC)
    if not offline_sensor_ids and not offline_infra_ids:
        return RcaEvaluation(
            evaluated_at=now,
            offline_sensor_count=0,
            offline_infra_count=0,
            incidents=(),
            total_raw_alerts=0,
            total_suppressed_alerts=0,
            noise_reduction_pct=0.0,
        )

    plausible_items: list[
        tuple[CandidateExplanation, frozenset[str], frozenset[str], float]
    ] = []
    for candidate in candidates:
        plausible, covered_sensors, coverage = _is_plausible_candidate(
            candidate,
            offline_sensor_ids,
            offline_infra_ids,
            gateway_threshold,
            passive_panel_threshold,
        )
        if not plausible:
            continue
        covered_infra = (candidate.downstream_infra_ids & offline_infra_ids) | (
            frozenset({candidate.node_id})
            if candidate.node_id in offline_infra_ids
            else frozenset()
        )
        if covered_sensors or covered_infra:
            plausible_items.append((candidate, covered_sensors, covered_infra, coverage))

    remaining_sensors = set(offline_sensor_ids)
    remaining_infra = set(offline_infra_ids)
    selected_incidents: list[RootCauseIncident] = []

    while remaining_sensors or remaining_infra:
        scored: list[
            tuple[int, int, int, float, CandidateExplanation, frozenset[str], frozenset[str], float]
        ] = []
        for candidate, cov_sensors, cov_infra, coverage in plausible_items:
            newly_sensors = len(cov_sensors & remaining_sensors)
            newly_infra = len(cov_infra & remaining_infra)
            newly_total = newly_sensors + newly_infra
            if newly_total == 0:
                continue
            scored.append(
                (
                    newly_sensors,
                    newly_total,
                    _HIERARCHY_RANK[candidate.node_type],
                    coverage,
                    candidate,
                    cov_sensors,
                    cov_infra,
                    coverage,
                )
            )

        if not scored:
            # Fallback for any orphan offline sensors/infra missing from topology candidates
            for orphan_sensor in sorted(remaining_sensors):
                selected_incidents.append(
                    RootCauseIncident(
                        root_cause_id=orphan_sensor,
                        root_cause_type=NodeType.SENSOR,
                        root_cause_name=orphan_sensor,
                        severity=Criticality.MEDIUM,
                        affected_sensor_ids=(orphan_sensor,),
                        affected_infra_ids=(),
                        raw_alerts_equivalent=1,
                        suppressed_alerts_count=0,
                        coverage_ratio=1.0,
                        confidence=1.0,
                        summary=(
                            f"Sensor {orphan_sensor} stopped sending heartbeats "
                            "(isolated device fault)."
                        ),
                    )
                )
            for orphan_infra in sorted(remaining_infra):
                selected_incidents.append(
                    RootCauseIncident(
                        root_cause_id=orphan_infra,
                        root_cause_type=NodeType.GATEWAY,
                        root_cause_name=orphan_infra,
                        severity=Criticality.HIGH,
                        affected_sensor_ids=(),
                        affected_infra_ids=(orphan_infra,),
                        raw_alerts_equivalent=1,
                        suppressed_alerts_count=0,
                        coverage_ratio=1.0,
                        confidence=0.8,
                        summary=f"Infrastructure {orphan_infra} stopped sending heartbeats.",
                    )
                )
            break

        scored.sort(key=lambda item: (item[0], item[1], item[2], item[3]), reverse=True)
        _, _, _, _, best, cov_sensors, cov_infra, coverage = scored[0]

        explained_sensors = tuple(sorted(cov_sensors & remaining_sensors))
        explained_infra = tuple(sorted(cov_infra & remaining_infra))
        raw_alerts = max(1, len(explained_sensors) + len(explained_infra))
        suppressed = max(0, raw_alerts - 1)

        if best.node_type == NodeType.SENSOR and not explained_sensors:
            explained_sensors = (best.node_id,)

        severity = _compute_severity(best, len(explained_sensors))
        confidence = round(min(1.0, 0.75 + 0.25 * coverage), 3)

        selected_incidents.append(
            RootCauseIncident(
                root_cause_id=best.node_id,
                root_cause_type=best.node_type,
                root_cause_name=best.name,
                severity=severity,
                affected_sensor_ids=explained_sensors,
                affected_infra_ids=explained_infra,
                raw_alerts_equivalent=raw_alerts,
                suppressed_alerts_count=suppressed,
                coverage_ratio=round(coverage, 4),
                confidence=confidence,
                summary=_build_summary(best, explained_sensors, suppressed, coverage),
            )
        )

        remaining_sensors -= set(explained_sensors)
        remaining_infra -= set(explained_infra)

    total_raw = sum(inc.raw_alerts_equivalent for inc in selected_incidents)
    total_suppressed = sum(inc.suppressed_alerts_count for inc in selected_incidents)
    noise_reduction = round((total_suppressed / total_raw) * 100.0, 1) if total_raw > 0 else 0.0

    return RcaEvaluation(
        evaluated_at=now,
        offline_sensor_count=len(offline_sensor_ids),
        offline_infra_count=len(offline_infra_ids),
        incidents=tuple(selected_incidents),
        total_raw_alerts=total_raw,
        total_suppressed_alerts=total_suppressed,
        noise_reduction_pct=noise_reduction,
    )
