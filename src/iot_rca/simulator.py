"""Virtual IoT device fleet simulator publishing MQTT heartbeats and chaos fault scenarios."""

import asyncio
import json
import logging
import random
import time
from datetime import UTC, datetime

import aiomqtt

from iot_rca.config import Settings, SimulatorSettings, get_simulator_settings
from iot_rca.models import ChaosScenario, NodeType, TelemetryPayload, TopologySeed
from iot_rca.mqtt_ingester import load_topology_seed

logger = logging.getLogger(__name__)

CHAOS_COMMAND_TOPIC = "iot/chaos/command"


def compute_muted_devices(seed: TopologySeed, scenario: ChaosScenario) -> frozenset[str]:
    """Return the set of device IDs that stop publishing under a given chaos scenario."""
    if scenario == ChaosScenario.HEALTHY:
        return frozenset()

    if scenario == ChaosScenario.SINGLE_SENSOR:
        return frozenset({"sns-temp-01"})

    if scenario == ChaosScenario.GATEWAY_FAILURE:
        failed_gw = "gw-lora-01"
        child_sensors = {s.id for s in seed.sensors if s.gateway_id == failed_gw}
        return frozenset({failed_gw} | child_sensors)

    if scenario == ChaosScenario.SWITCH_FAILURE:
        failed_sw = "sw-core-01"
        failed_gws = {g.id for g in seed.gateways if g.uplink_via == failed_sw}
        child_sensors = {s.id for s in seed.sensors if s.gateway_id in failed_gws}
        return frozenset({failed_sw} | failed_gws | child_sensors)

    if scenario == ChaosScenario.POWER_PANEL_FAILURE:
        failed_panel = "pp-main-a"
        failed_switches = {sw.id for sw in seed.switches if sw.powered_by == failed_panel}
        failed_gws = {
            g.id
            for g in seed.gateways
            if g.powered_by == failed_panel or g.uplink_via in failed_switches
        }
        child_sensors = {s.id for s in seed.sensors if s.gateway_id in failed_gws}
        return frozenset(failed_switches | failed_gws | child_sensors)

    return frozenset()


def build_sensor_metrics(sensor_type: str, rng: random.Random) -> dict[str, float | int | str]:
    """Generate realistic telemetry metrics based on sensor domain type."""
    if sensor_type == "temperature":
        return {"temperature_c": round(rng.uniform(21.2, 25.4), 2)}
    if sensor_type == "cryo_temp":
        return {"temperature_c": round(rng.uniform(-20.8, -17.9), 2)}
    if sensor_type == "humidity":
        return {"humidity_pct": round(rng.uniform(41.0, 58.0), 1)}
    if sensor_type == "power_meter":
        return {
            "power_kw": round(rng.uniform(11.5, 19.8), 2),
            "voltage_v": round(rng.uniform(378.0, 383.0), 1),
        }
    if sensor_type == "vibration":
        return {"vibration_rms": round(rng.uniform(0.7, 2.3), 3)}
    if sensor_type == "door_contact":
        return {"door_state": "closed"}
    return {"value": round(rng.uniform(0.0, 100.0), 2)}


class FleetSimulator:
    """Publishes realistic heartbeats for all active infrastructure and sensors over MQTT."""

    def __init__(self, settings: Settings | SimulatorSettings, seed: TopologySeed) -> None:
        self._settings = settings
        self._seed = seed
        self._scenario = ChaosScenario.HEALTHY
        self._muted: frozenset[str] = frozenset()
        self._seq_base = time.time_ns() // 1_000_000
        self._seq_counters: dict[str, int] = {}
        self._rng = random.Random()

    def set_scenario(self, scenario: ChaosScenario) -> None:
        self._scenario = scenario
        self._muted = compute_muted_devices(self._seed, scenario)
        logger.info(
            "Simulator scenario -> %s (muted %d devices)",
            scenario.value,
            len(self._muted),
        )

    def _next_seq(self, device_id: str) -> int:
        seq = self._seq_counters.get(device_id, self._seq_base) + 1
        self._seq_counters[device_id] = seq
        return seq

    async def _listen_commands(self, client: aiomqtt.Client) -> None:
        await client.subscribe(CHAOS_COMMAND_TOPIC, qos=1)
        async for msg in client.messages:
            if str(msg.topic) != CHAOS_COMMAND_TOPIC:
                continue
            try:
                raw = (
                    msg.payload.decode("utf-8")
                    if isinstance(msg.payload, bytes)
                    else str(msg.payload)
                )
                data = json.loads(raw)
                self.set_scenario(ChaosScenario(data["scenario"]))
            except Exception as exc:
                logger.warning("Ignoring invalid chaos command: %s", exc)

    async def _publish_loop(self, client: aiomqtt.Client) -> None:
        while True:
            now = datetime.now(UTC)

            for sw in self._seed.switches:
                if sw.id in self._muted:
                    continue
                payload = TelemetryPayload(
                    device_id=sw.id,
                    device_type=NodeType.NETWORK_SWITCH,
                    seq=self._next_seq(sw.id),
                    ts=now,
                    metrics={"packet_loss_pct": round(self._rng.uniform(0.0, 0.2), 2)},
                )
                await client.publish(
                    f"iot/telemetry/switch/{sw.id}",
                    payload.model_dump_json(),
                    qos=1,
                )

            for gw in self._seed.gateways:
                if gw.id in self._muted:
                    continue
                payload = TelemetryPayload(
                    device_id=gw.id,
                    device_type=NodeType.GATEWAY,
                    seq=self._next_seq(gw.id),
                    ts=now,
                    metrics={"cpu_load_pct": round(self._rng.uniform(12.0, 38.0), 1)},
                )
                await client.publish(
                    f"iot/telemetry/gateway/{gw.id}",
                    payload.model_dump_json(),
                    qos=1,
                )

            for sensor in self._seed.sensors:
                if sensor.id in self._muted:
                    continue
                payload = TelemetryPayload(
                    device_id=sensor.id,
                    device_type=NodeType.SENSOR,
                    seq=self._next_seq(sensor.id),
                    ts=now,
                    battery_pct=round(self._rng.uniform(76.0, 99.0), 1),
                    metrics=build_sensor_metrics(sensor.sensor_type, self._rng),
                )
                await client.publish(
                    f"iot/telemetry/sensor/{sensor.id}",
                    payload.model_dump_json(),
                    qos=1,
                )

            await asyncio.sleep(self._settings.simulator_interval_seconds)

    async def run(self) -> None:
        while True:
            try:
                async with aiomqtt.Client(
                    hostname=self._settings.mqtt_host,
                    port=self._settings.mqtt_port,
                    identifier="iot-rca-simulator",
                ) as client:
                    logger.info(
                        "Simulator connected to MQTT %s:%s",
                        self._settings.mqtt_host,
                        self._settings.mqtt_port,
                    )
                    await asyncio.gather(
                        self._listen_commands(client),
                        self._publish_loop(client),
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Simulator MQTT error (%s), retrying in 2s...", exc)
                await asyncio.sleep(2.0)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    settings = get_simulator_settings()
    seed = load_topology_seed(settings.topology_seed_path)
    sim = FleetSimulator(settings, seed)
    await sim.run()


if __name__ == "__main__":
    asyncio.run(main())
