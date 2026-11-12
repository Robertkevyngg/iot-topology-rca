"""Async Neo4j repository for topology graph seeding, RCA queries, Blast Radius, and Incidents."""

import asyncio
import json
import random
import uuid
from datetime import UTC, datetime
from typing import Any

from neo4j import AsyncDriver
from neo4j.exceptions import TransientError

from iot_rca.models import (
    BlastRadiusItem,
    BlastRadiusReport,
    CandidateExplanation,
    Criticality,
    NodeType,
    RcaEvaluation,
    StoredIncident,
    TopologyEdgeView,
    TopologySeed,
)

_CRITICALITY_RANK: dict[str, int] = {
    Criticality.CRITICAL.value: 4,
    Criticality.HIGH.value: 3,
    Criticality.MEDIUM.value: 2,
    Criticality.LOW.value: 1,
}


def _highest_criticality(values: list[str | None]) -> Criticality:
    best_val = Criticality.MEDIUM.value
    best_rank = 0
    for val in values:
        if val and _CRITICALITY_RANK.get(val, 0) > best_rank:
            best_rank = _CRITICALITY_RANK[val]
            best_val = val
    return Criticality(best_val)


def _to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if hasattr(value, "to_native"):
        native = value.to_native()
        if isinstance(native, datetime):
            return native if native.tzinfo else native.replace(tzinfo=UTC)
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return datetime.now(UTC)


class Neo4jTopologyRepo:
    """Encapsulates all Cypher queries against the Neo4j topology and incident graph."""

    def __init__(self, driver: AsyncDriver) -> None:
        self._driver = driver

    async def ping(self) -> bool:
        async with self._driver.session() as session:
            result = await session.run("RETURN 1 AS ok")
            record = await result.single()
            return bool(record and record["ok"] == 1)

    async def ensure_schema_and_seed(self, seed: TopologySeed) -> None:
        """Create uniqueness constraints and idempotently merge the IoT topology graph."""
        for attempt in range(1, 6):
            try:
                await self._apply_schema_and_seed(seed)
                return
            except TransientError:
                if attempt == 5:
                    raise
                await asyncio.sleep(0.25 * attempt + random.uniform(0.05, 0.35))

    async def _apply_schema_and_seed(self, seed: TopologySeed) -> None:
        constraints = [
            "CREATE CONSTRAINT zone_id IF NOT EXISTS FOR (n:Zone) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT panel_id IF NOT EXISTS FOR (n:PowerPanel) REQUIRE n.id IS UNIQUE",
            (
                "CREATE CONSTRAINT switch_id IF NOT EXISTS "
                "FOR (n:NetworkSwitch) REQUIRE n.id IS UNIQUE"
            ),
            "CREATE CONSTRAINT gateway_id IF NOT EXISTS FOR (n:Gateway) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT sensor_id IF NOT EXISTS FOR (n:Sensor) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT incident_id IF NOT EXISTS FOR (n:Incident) REQUIRE n.id IS UNIQUE",
        ]

        async with self._driver.session() as session:
            for stmt in constraints:
                await session.run(stmt)

            await session.run(
                """
                UNWIND $zones AS z
                MERGE (n:Zone {id: z.id})
                SET n.name = z.name, n.floor = z.floor
                """,
                zones=[z.model_dump() for z in seed.zones],
            )

            await session.run(
                """
                UNWIND $panels AS p
                MERGE (n:PowerPanel:Infrastructure {id: p.id})
                SET n.name = p.name, n.circuit = p.circuit, n.is_passive = true
                WITH n, p
                MATCH (z:Zone {id: p.zone_id})
                MERGE (n)-[:LOCATED_IN]->(z)
                """,
                panels=[p.model_dump() for p in seed.power_panels],
            )

            await session.run(
                """
                UNWIND $switches AS sw
                MERGE (n:NetworkSwitch:Infrastructure:Device {id: sw.id})
                SET n.name = sw.name, n.ip = sw.ip, n.is_passive = false
                WITH n, sw
                MATCH (p:PowerPanel {id: sw.powered_by})
                MERGE (n)-[:POWERED_BY]->(p)
                WITH n, sw
                MATCH (z:Zone {id: sw.zone_id})
                MERGE (n)-[:LOCATED_IN]->(z)
                """,
                switches=[sw.model_dump() for sw in seed.switches],
            )

            await session.run(
                """
                UNWIND $gateways AS gw
                MERGE (n:Gateway:Infrastructure:Device {id: gw.id})
                SET n.name = gw.name, n.protocol = gw.protocol, n.is_passive = false
                WITH n, gw
                MATCH (p:PowerPanel {id: gw.powered_by})
                MERGE (n)-[:POWERED_BY]->(p)
                WITH n, gw
                MATCH (z:Zone {id: gw.zone_id})
                MERGE (n)-[:LOCATED_IN]->(z)
                WITH n, gw
                WHERE gw.uplink_via IS NOT NULL
                MATCH (sw:NetworkSwitch {id: gw.uplink_via})
                MERGE (n)-[:UPLINK_VIA]->(sw)
                """,
                gateways=[gw.model_dump() for gw in seed.gateways],
            )

            await session.run(
                """
                UNWIND $sensors AS s
                MERGE (n:Sensor:Device {id: s.id})
                SET n.name = s.id,
                    n.sensor_type = s.sensor_type,
                    n.criticality = s.criticality,
                    n.firmware = s.firmware,
                    n.is_passive = false
                WITH n, s
                MATCH (gw:Gateway {id: s.gateway_id})
                MERGE (n)-[:CONNECTED_TO]->(gw)
                WITH n, s
                MATCH (z:Zone {id: s.zone_id})
                MERGE (n)-[:LOCATED_IN]->(z)
                """,
                sensors=[s.model_dump(mode="json") for s in seed.sensors],
            )

    async def fetch_rca_candidates(
        self, online_infra_ids: frozenset[str]
    ) -> list[CandidateExplanation]:
        """Load topology candidate explanations with downstream sensor/infra memberships."""
        query = """
        MATCH (n)
        WHERE n:PowerPanel OR n:NetworkSwitch OR n:Gateway OR n:Sensor
        OPTIONAL MATCH (n)<-[:CONNECTED_TO|UPLINK_VIA|POWERED_BY*0..5]-(s:Sensor)
        WITH n,
             collect(DISTINCT s.id) AS sensor_ids,
             collect(DISTINCT s.criticality) AS criticalities
        OPTIONAL MATCH (n)<-[:UPLINK_VIA|POWERED_BY*1..4]-(infra:Device)
        RETURN n.id AS node_id,
               CASE
                 WHEN n:PowerPanel THEN 'PowerPanel'
                 WHEN n:NetworkSwitch THEN 'NetworkSwitch'
                 WHEN n:Gateway THEN 'Gateway'
                 ELSE 'Sensor'
               END AS node_type,
               coalesce(n.name, n.id) AS name,
               coalesce(n.is_passive, false) AS is_passive,
               sensor_ids,
               collect(DISTINCT CASE WHEN infra:Gateway OR infra:NetworkSwitch THEN infra.id END)
                 AS child_infra_ids,
               criticalities
        """
        async with self._driver.session() as session:
            result = await session.run(query)
            records = await result.data()

        candidates: list[CandidateExplanation] = []
        for row in records:
            child_infra = frozenset(x for x in row["child_infra_ids"] if x)
            online_children = len(child_infra & online_infra_ids)
            candidates.append(
                CandidateExplanation(
                    node_id=row["node_id"],
                    node_type=NodeType(row["node_type"]),
                    name=row["name"],
                    is_passive=bool(row["is_passive"]),
                    downstream_sensor_ids=frozenset(x for x in row["sensor_ids"] if x),
                    downstream_infra_ids=child_infra,
                    online_child_infra_count=online_children,
                    highest_criticality=_highest_criticality(row["criticalities"]),
                )
            )
        return candidates

    async def get_blast_radius(self, target_id: str) -> BlastRadiusReport | None:
        """Compute downstream blast radius if `target_id` fails or enters maintenance."""
        target_query = """
        MATCH (t {id: $target_id})
        WHERE t:PowerPanel OR t:NetworkSwitch OR t:Gateway OR t:Sensor OR t:Zone
        RETURN t.id AS id,
               coalesce(t.name, t.id) AS name,
               CASE
                 WHEN t:PowerPanel THEN 'PowerPanel'
                 WHEN t:NetworkSwitch THEN 'NetworkSwitch'
                 WHEN t:Gateway THEN 'Gateway'
                 WHEN t:Zone THEN 'Zone'
                 ELSE 'Sensor'
               END AS node_type
        """
        async with self._driver.session() as session:
            target_res = await session.run(target_query, target_id=target_id)
            target_row = await target_res.single()
            if not target_row:
                return None

            target_type = NodeType(target_row["node_type"])
            if target_type == NodeType.ZONE:
                impact_query = """
                MATCH (t:Zone {id: $target_id})<-[:LOCATED_IN]-(affected)
                RETURN affected.id AS id,
                       coalesce(affected.name, affected.id) AS name,
                       CASE
                         WHEN affected:PowerPanel THEN 'PowerPanel'
                         WHEN affected:NetworkSwitch THEN 'NetworkSwitch'
                         WHEN affected:Gateway THEN 'Gateway'
                         ELSE 'Sensor'
                       END AS node_type,
                       affected.criticality AS criticality,
                       $target_id AS zone_id,
                       1 AS hops
                """
            else:
                impact_query = """
                MATCH path = (t {id: $target_id})
                    <-[:CONNECTED_TO|UPLINK_VIA|POWERED_BY*1..5]-(affected)
                OPTIONAL MATCH (affected)-[:LOCATED_IN]->(z:Zone)
                WITH affected, z, min(length(path)) AS hops
                RETURN affected.id AS id,
                       coalesce(affected.name, affected.id) AS name,
                       CASE
                         WHEN affected:PowerPanel THEN 'PowerPanel'
                         WHEN affected:NetworkSwitch THEN 'NetworkSwitch'
                         WHEN affected:Gateway THEN 'Gateway'
                         ELSE 'Sensor'
                       END AS node_type,
                       affected.criticality AS criticality,
                       z.id AS zone_id,
                       hops
                ORDER BY hops ASC, node_type ASC, id ASC
                """
            impact_res = await session.run(impact_query, target_id=target_id)
            rows = await impact_res.data()

        items = tuple(
            BlastRadiusItem(
                id=r["id"],
                node_type=NodeType(r["node_type"]),
                name=r["name"],
                criticality=Criticality(r["criticality"]) if r["criticality"] else None,
                zone_id=r["zone_id"],
                hops=int(r["hops"]),
            )
            for r in rows
        )
        infra_count = sum(1 for i in items if i.node_type != NodeType.SENSOR)
        sensor_count = sum(1 for i in items if i.node_type == NodeType.SENSOR)
        breakdown: dict[str, int] = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        for item in items:
            if item.node_type == NodeType.SENSOR and item.criticality:
                breakdown[item.criticality.value] = breakdown.get(item.criticality.value, 0) + 1

        return BlastRadiusReport(
            target_id=target_row["id"],
            target_type=target_type,
            target_name=target_row["name"],
            affected_infra_count=infra_count,
            affected_sensor_count=sensor_count,
            criticality_breakdown=breakdown,
            affected_nodes=items,
        )

    async def sync_incidents(self, evaluation: RcaEvaluation) -> list[StoredIncident]:
        """Reconcile Neo4j :Incident nodes with the latest RCA evaluation."""
        now_iso = evaluation.evaluated_at.astimezone(UTC).isoformat()
        desired_by_rc = {inc.root_cause_id: inc for inc in evaluation.incidents}

        async with self._driver.session() as session:
            open_res = await session.run(
                """
                MATCH (i:Incident {status: 'OPEN'})
                RETURN i.id AS id, i.root_cause_id AS root_cause_id
                """
            )
            existing_open = await open_res.data()
            existing_by_rc = {r["root_cause_id"]: r["id"] for r in existing_open}

            # 1. Resolve open incidents no longer in the active root cause set
            new_covering_sensors: set[str] = set()
            for inc in evaluation.incidents:
                new_covering_sensors.update(inc.affected_sensor_ids)

            for old_rc, old_inc_id in existing_by_rc.items():
                if old_rc not in desired_by_rc:
                    reason = (
                        "escalated_to_upstream_root_cause"
                        if old_rc in new_covering_sensors
                        or any(old_rc in inc.affected_infra_ids for inc in evaluation.incidents)
                        else "recovered"
                    )
                    await session.run(
                        """
                        MATCH (i:Incident {id: $inc_id})
                        SET i.status = 'RESOLVED',
                            i.resolved_at = $resolved_at,
                            i.resolution_reason = $reason
                        """,
                        inc_id=old_inc_id,
                        resolved_at=now_iso,
                        reason=reason,
                    )

            # 2. Upsert active incidents
            for rc_id, inc in desired_by_rc.items():
                inc_id = existing_by_rc.get(rc_id) or f"INC-{uuid.uuid4().hex[:8].upper()}"
                await session.run(
                    """
                    MERGE (i:Incident {id: $inc_id})
                    ON CREATE SET i.opened_at = $now_iso
                    SET i.status = 'OPEN',
                        i.root_cause_id = $rc_id,
                        i.root_cause_type = $rc_type,
                        i.root_cause_name = $rc_name,
                        i.severity = $severity,
                        i.affected_sensor_ids_json = $sensors_json,
                        i.affected_infra_ids_json = $infra_json,
                        i.raw_alerts_equivalent = $raw_alerts,
                        i.suppressed_alerts_count = $suppressed,
                        i.coverage_ratio = $coverage,
                        i.confidence = $confidence,
                        i.summary = $summary
                    WITH i
                    OPTIONAL MATCH (target {id: $rc_id})
                    WHERE target:PowerPanel
                       OR target:NetworkSwitch
                       OR target:Gateway
                       OR target:Sensor
                    FOREACH (_ IN CASE WHEN target IS NOT NULL THEN [1] ELSE [] END |
                        MERGE (i)-[:ROOT_CAUSE]->(target)
                    )
                    WITH i
                    OPTIONAL MATCH (i)-[old_rel:IMPACTS]->(:Sensor)
                    DELETE old_rel
                    WITH i
                    UNWIND $sensor_ids AS sid
                    MATCH (s:Sensor {id: sid})
                    MERGE (i)-[:IMPACTS]->(s)
                    """,
                    inc_id=inc_id,
                    now_iso=now_iso,
                    rc_id=inc.root_cause_id,
                    rc_type=inc.root_cause_type.value,
                    rc_name=inc.root_cause_name,
                    severity=inc.severity.value,
                    sensors_json=json.dumps(list(inc.affected_sensor_ids)),
                    infra_json=json.dumps(list(inc.affected_infra_ids)),
                    raw_alerts=inc.raw_alerts_equivalent,
                    suppressed=inc.suppressed_alerts_count,
                    coverage=inc.coverage_ratio,
                    confidence=inc.confidence,
                    summary=inc.summary,
                    sensor_ids=list(inc.affected_sensor_ids),
                )

        return await self.list_incidents(status="OPEN")

    async def list_incidents(
        self, *, status: str | None = None, limit: int = 50
    ) -> list[StoredIncident]:
        where_clause = "WHERE i.status = $status" if status else ""
        query = f"""
        MATCH (i:Incident)
        {where_clause}
        RETURN i
        ORDER BY i.opened_at DESC
        LIMIT $limit
        """
        async with self._driver.session() as session:
            result = await session.run(query, status=status, limit=limit)
            rows = await result.data()

        incidents: list[StoredIncident] = []
        for row in rows:
            node = row["i"]
            incidents.append(
                StoredIncident(
                    id=node["id"],
                    status=node["status"],
                    root_cause_id=node["root_cause_id"],
                    root_cause_type=node["root_cause_type"],
                    root_cause_name=node["root_cause_name"],
                    severity=node["severity"],
                    affected_sensor_ids=tuple(
                        json.loads(node.get("affected_sensor_ids_json", "[]"))
                    ),
                    affected_infra_ids=tuple(
                        json.loads(node.get("affected_infra_ids_json", "[]"))
                    ),
                    raw_alerts_equivalent=int(node.get("raw_alerts_equivalent", 1)),
                    suppressed_alerts_count=int(node.get("suppressed_alerts_count", 0)),
                    coverage_ratio=float(node.get("coverage_ratio", 1.0)),
                    confidence=float(node.get("confidence", 1.0)),
                    summary=node.get("summary", ""),
                    opened_at=_to_datetime(node.get("opened_at")),
                    resolved_at=(
                        _to_datetime(node["resolved_at"]) if node.get("resolved_at") else None
                    ),
                    resolution_reason=node.get("resolution_reason"),
                )
            )
        return incidents

    async def fetch_graph_structure(self) -> tuple[list[dict[str, Any]], list[TopologyEdgeView]]:
        """Fetch nodes and dependency edges for topology visualization."""
        node_query = """
        MATCH (n)
        WHERE n:PowerPanel OR n:NetworkSwitch OR n:Gateway OR n:Sensor
        OPTIONAL MATCH (n)-[:LOCATED_IN]->(z:Zone)
        RETURN n.id AS id,
               CASE
                 WHEN n:PowerPanel THEN 'PowerPanel'
                 WHEN n:NetworkSwitch THEN 'NetworkSwitch'
                 WHEN n:Gateway THEN 'Gateway'
                 ELSE 'Sensor'
               END AS node_type,
               coalesce(n.name, n.id) AS name,
               z.id AS zone_id,
               coalesce(n.is_passive, false) AS is_passive,
               n.criticality AS criticality
        ORDER BY node_type, id
        """
        edge_query = """
        MATCH (a)-[r:CONNECTED_TO|UPLINK_VIA|POWERED_BY]->(b)
        RETURN a.id AS source, b.id AS target, type(r) AS relationship
        """
        async with self._driver.session() as session:
            node_rows = await (await session.run(node_query)).data()
            edge_rows = await (await session.run(edge_query)).data()

        edges = [
            TopologyEdgeView(
                source=r["source"],
                target=r["target"],
                relationship=r["relationship"],
            )
            for r in edge_rows
        ]
        return node_rows, edges
