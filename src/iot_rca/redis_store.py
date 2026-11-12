"""Async Redis store for live device heartbeats, TTL expiration, deduplication, and streams."""

import json
import time
import uuid
from datetime import UTC, datetime

from redis.asyncio import Redis

from iot_rca.models import (
    ChaosScenario,
    DeviceRuntimeState,
    DeviceStatus,
    NodeType,
    TelemetryPayload,
)

_DEVICE_KEY_PREFIX = "iot:device:"
_DEVICE_INDEX_KEY = "iot:devices:index"
_EXPIRY_ZSET_KEY = "iot:heartbeats:expiry"
_DEDUP_KEY_PREFIX = "iot:dedup:"
_STATE_STREAM_KEY = "iot:stream:state_changes"
_CHAOS_SCENARIO_KEY = "iot:chaos:scenario"
_LIVE_PUBSUB_CHANNEL = "iot:pubsub:live"
_RCA_LOCK_KEY = "iot:lock:rca_cycle"

# Atomic pop of expired members from the ZSET so concurrent heartbeats are never lost.
_POP_EXPIRED_LUA = """
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if #expired > 0 then
    redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
end
return expired
"""

# Atomic lock release only if the caller owns the lock token.
_RELEASE_LOCK_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


def _decode_str(value: bytes | str | None) -> str | None:
    if value is None:
        return None
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _device_key(device_id: str) -> str:
    return f"{_DEVICE_KEY_PREFIX}{device_id}"


def _dedup_key(device_id: str, seq: int) -> str:
    return f"{_DEDUP_KEY_PREFIX}{device_id}:{seq}"


class RedisStateStore:
    """Manages hot-path IoT device state and heartbeat expiration in Redis."""

    def __init__(self, client: Redis) -> None:
        self._redis = client
        self._pop_expired_script = self._redis.register_script(_POP_EXPIRED_LUA)
        self._release_lock_script = self._redis.register_script(_RELEASE_LOCK_LUA)

    async def ping(self) -> bool:
        return bool(await self._redis.ping())

    async def ensure_stream_group(self, group_name: str = "rca_workers") -> None:
        """Create the Redis Stream consumer group idempotently."""
        try:
            await self._redis.xgroup_create(
                name=_STATE_STREAM_KEY,
                groupname=group_name,
                id="0",
                mkstream=True,
            )
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def record_heartbeat(
        self,
        payload: TelemetryPayload,
        *,
        ttl_seconds: float,
        dedup_ttl_seconds: int = 300,
        now_epoch: float | None = None,
    ) -> tuple[bool, bool]:
        """Store heartbeat with atomic idempotency check.

        Returns (accepted, transitioned_to_online).
        """
        acquired = await self._redis.set(
            _dedup_key(payload.device_id, payload.seq),
            "1",
            ex=dedup_ttl_seconds,
            nx=True,
        )
        if not acquired:
            return False, False

        now = now_epoch if now_epoch is not None else time.time()
        expiry_at = now + ttl_seconds
        dev_key = _device_key(payload.device_id)
        prev_status = _decode_str(await self._redis.hget(dev_key, "status"))
        transitioned = prev_status != DeviceStatus.ONLINE.value

        mapping: dict[str, str] = {
            "device_id": payload.device_id,
            "device_type": payload.device_type.value,
            "status": DeviceStatus.ONLINE.value,
            "last_seen": payload.ts.astimezone(UTC).isoformat(),
            "seq": str(payload.seq),
            "metrics_json": json.dumps(payload.metrics, sort_keys=True),
        }
        if payload.battery_pct is not None:
            mapping["battery_pct"] = str(payload.battery_pct)

        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.hset(dev_key, mapping=mapping)
            pipe.sadd(_DEVICE_INDEX_KEY, payload.device_id)
            pipe.zadd(_EXPIRY_ZSET_KEY, {payload.device_id: expiry_at})
            if transitioned:
                pipe.xadd(
                    _STATE_STREAM_KEY,
                    {
                        "device_id": payload.device_id,
                        "device_type": payload.device_type.value,
                        "event": "ONLINE",
                        "ts": datetime.now(UTC).isoformat(),
                    },
                    maxlen=5000,
                    approximate=True,
                )
            await pipe.execute()

        return True, transitioned

    async def sweep_expired_heartbeats(self, now_epoch: float | None = None) -> list[str]:
        """Atomically expire overdue devices from the ZSET and mark them offline."""
        now = now_epoch if now_epoch is not None else time.time()
        raw_expired = await self._pop_expired_script(keys=[_EXPIRY_ZSET_KEY], args=[str(now)])
        if not raw_expired:
            return []

        expired_ids = [
            item.decode("utf-8") if isinstance(item, bytes) else str(item)
            for item in raw_expired
        ]
        now_iso = datetime.now(UTC).isoformat()

        # Fetch device types before updating status
        async with self._redis.pipeline(transaction=False) as read_pipe:
            for dev_id in expired_ids:
                read_pipe.hget(_device_key(dev_id), "device_type")
            types = await read_pipe.execute()

        async with self._redis.pipeline(transaction=False) as write_pipe:
            for dev_id, raw_type in zip(expired_ids, types, strict=True):
                dev_type = (
                    raw_type.decode("utf-8")
                    if isinstance(raw_type, bytes)
                    else (raw_type or NodeType.SENSOR.value)
                )
                write_pipe.hset(_device_key(dev_id), "status", DeviceStatus.OFFLINE.value)
                write_pipe.xadd(
                    _STATE_STREAM_KEY,
                    {
                        "device_id": dev_id,
                        "device_type": dev_type,
                        "event": "OFFLINE",
                        "ts": now_iso,
                    },
                    maxlen=5000,
                    approximate=True,
                )
            await write_pipe.execute()

        return expired_ids

    async def get_all_device_states(self) -> dict[str, DeviceRuntimeState]:
        """Return runtime state snapshots for all indexed devices."""
        raw_ids = await self._redis.smembers(_DEVICE_INDEX_KEY)
        if not raw_ids:
            return {}

        device_ids = sorted(
            item.decode("utf-8") if isinstance(item, bytes) else str(item) for item in raw_ids
        )
        async with self._redis.pipeline(transaction=False) as pipe:
            for dev_id in device_ids:
                pipe.hgetall(_device_key(dev_id))
            rows = await pipe.execute()

        states: dict[str, DeviceRuntimeState] = {}
        for dev_id, row in zip(device_ids, rows, strict=True):
            if not row:
                continue
            decoded = {
                (k.decode("utf-8") if isinstance(k, bytes) else k): (
                    v.decode("utf-8") if isinstance(v, bytes) else v
                )
                for k, v in row.items()
            }
            last_seen_raw = decoded.get("last_seen")
            battery_raw = decoded.get("battery_pct")
            seq_raw = decoded.get("seq")
            metrics_raw = decoded.get("metrics_json", "{}")
            states[dev_id] = DeviceRuntimeState(
                device_id=dev_id,
                device_type=NodeType(decoded.get("device_type", NodeType.SENSOR.value)),
                status=DeviceStatus(decoded.get("status", DeviceStatus.UNKNOWN.value)),
                last_seen=datetime.fromisoformat(last_seen_raw) if last_seen_raw else None,
                battery_pct=float(battery_raw) if battery_raw is not None else None,
                seq=int(seq_raw) if seq_raw is not None else None,
                metrics=json.loads(metrics_raw),
            )
        return states

    async def get_offline_and_online_sets(
        self,
    ) -> tuple[frozenset[str], frozenset[str], frozenset[str], dict[str, DeviceRuntimeState]]:
        """Return (offline_sensors, offline_infra, online_infra, all_states)."""
        states = await self.get_all_device_states()
        offline_sensors: set[str] = set()
        offline_infra: set[str] = set()
        online_infra: set[str] = set()

        for dev_id, st in states.items():
            if st.device_type == NodeType.SENSOR:
                if st.status == DeviceStatus.OFFLINE:
                    offline_sensors.add(dev_id)
            elif st.device_type in (NodeType.GATEWAY, NodeType.NETWORK_SWITCH):
                if st.status == DeviceStatus.OFFLINE:
                    offline_infra.add(dev_id)
                elif st.status == DeviceStatus.ONLINE:
                    online_infra.add(dev_id)

        return (
            frozenset(offline_sensors),
            frozenset(offline_infra),
            frozenset(online_infra),
            states,
        )

    async def get_recent_state_events(self, count: int = 30) -> list[dict[str, str]]:
        """Read latest state transition events from the Redis Stream."""
        entries = await self._redis.xrevrange(_STATE_STREAM_KEY, count=count)
        events: list[dict[str, str]] = []
        for entry_id, fields in entries:
            decoded = {
                (k.decode("utf-8") if isinstance(k, bytes) else k): (
                    v.decode("utf-8") if isinstance(v, bytes) else v
                )
                for k, v in fields.items()
            }
            decoded["stream_id"] = (
                entry_id.decode("utf-8") if isinstance(entry_id, bytes) else str(entry_id)
            )
            events.append(decoded)
        return events

    async def set_active_chaos_scenario(self, scenario: ChaosScenario) -> None:
        await self._redis.set(_CHAOS_SCENARIO_KEY, scenario.value)

    async def get_active_chaos_scenario(self) -> ChaosScenario:
        raw = await self._redis.get(_CHAOS_SCENARIO_KEY)
        if not raw:
            return ChaosScenario.HEALTHY
        val = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        try:
            return ChaosScenario(val)
        except ValueError:
            return ChaosScenario.HEALTHY

    async def consume_state_events(
        self,
        *,
        group_name: str = "rca_workers",
        consumer_name: str = "worker-1",
        count: int = 100,
    ) -> list[dict[str, str]]:
        """Read and acknowledge unconsumed state-change events via XREADGROUP."""
        response = await self._redis.xreadgroup(
            groupname=group_name,
            consumername=consumer_name,
            streams={_STATE_STREAM_KEY: ">"},
            count=count,
        )
        if not response:
            return []

        ack_ids: list[bytes | str] = []
        events: list[dict[str, str]] = []
        for _stream_name, entries in response:
            for entry_id, fields in entries:
                ack_ids.append(entry_id)
                decoded = {
                    (_decode_str(k) or ""): (_decode_str(v) or "")
                    for k, v in fields.items()
                }
                decoded["stream_id"] = _decode_str(entry_id) or ""
                events.append(decoded)

        if ack_ids:
            await self._redis.xack(_STATE_STREAM_KEY, group_name, *ack_ids)
        return events

    async def acquire_rca_lock(self, ttl_seconds: int = 5) -> str | None:
        """Acquire distributed RCA lock. Returns lock token if acquired, else None."""
        token = uuid.uuid4().hex
        acquired = await self._redis.set(_RCA_LOCK_KEY, token, ex=ttl_seconds, nx=True)
        return token if acquired else None

    async def release_rca_lock(self, token: str) -> None:
        """Release distributed RCA lock only if held by `token`."""
        await self._release_lock_script(keys=[_RCA_LOCK_KEY], args=[token])

    async def publish_live_update(self, payload: dict[str, object]) -> None:
        await self._redis.publish(_LIVE_PUBSUB_CHANNEL, json.dumps(payload))

    @property
    def live_pubsub_channel(self) -> str:
        return _LIVE_PUBSUB_CHANNEL
