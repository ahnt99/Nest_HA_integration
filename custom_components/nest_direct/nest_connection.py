"""
Nest API connection handler.

Data sources:
  - REST app_launch  → structure, topaz (Protect), where
  - Protobuf gRPC observe  → thermostats, locks, temp sensors
"""

import asyncio
import json
import time
import traceback
from typing import Any, Callable, Optional

import aiohttp

from .const import (
    API_AUTH_FAIL_RETRY_DELAY_SECONDS,
    API_MODE_CHANGE_DELAY_SECONDS,
    API_PUSH_DEBOUNCE_SECONDS,
    API_RETRY_DELAY_SECONDS,
    API_TIMEOUT_SECONDS,
    DEFAULT_HOT_WATER_DURATION_MINUTES,
    ENDPOINT_PUT,
    ENDPOINT_UPDATE,
    NEST_API_HOSTNAME,
    OBSERVE_HOST,
    URL_NEST_AUTH,
    USER_AGENT_STRING,
)
from .protobuf_observer import ProtobufObserver
from .protobuf_observer import (
    encode_set_hvac_mode, encode_set_eco_mode,
    encode_set_temperature, encode_set_fan, encode_set_fan_timer,
)

POLL_INTERVAL_SECONDS = 30

def get_unix_time() -> int:
    return int(time.time())

def clone_object(obj: Any) -> Any:
    return json.loads(json.dumps(obj))

def create_api_object(node_id: str, value: dict) -> dict:
    return {"object_key": node_id, "op": "MERGE", "value": clone_object(value)}

class NestConnection:

    def __init__(self, config: dict):
        self.config = config
        self.token: Optional[str] = None
        self._access_token: Optional[str] = None  # original config token for cookie auth
        self.transport_url: Optional[str] = None
        self.userid: Optional[str] = None
        self.connected = False

        # REST state (structure, topaz, where)
        self.rest_state: dict = {}

        # Protobuf state — keyed by legacy uppercase hex device ID
        self.proto_devices: dict = {}    # dev_id -> merged trait dict
        self.proto_where_map: dict = {}  # where_id -> name

        self.pending_updates: list = []
        self.merge_updates: list = []
        self.last_mode_change_time: Optional[float] = None
        self.failed_push_api_calls = 0
        self._session: Optional[aiohttp.ClientSession] = None
        self._observer: Optional[ProtobufObserver] = None
        self._observe_task: Optional[asyncio.Task] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._observer:
            self._observer.stop()
        if self._observe_task:
            self._observe_task.cancel()
        if self._session and not self._session.closed:
            await self._session.close()

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    async def auth(self, preemptive: bool = False) -> bool:
        if not preemptive:
            self.connected = False
            self.token = None

        access_token = self.config.get("access_token")
        if not access_token:
            return False

        session = await self._get_session()
        try:
            async with session.get(
                URL_NEST_AUTH,
                headers={
                    "Authorization": f"Basic {access_token}",
                    "User-Agent": USER_AGENT_STRING,
                    "cookie": (
                        f"G_ENABLED_IDPS=google; eu_cookie_accepted=1; "
                        f"viewer-volume=0.5; cztoken={access_token}"
                    ),
                },
                timeout=aiohttp.ClientTimeout(total=API_TIMEOUT_SECONDS),
            ) as resp:
                if resp.status == 200:
                    body = await resp.json(content_type=None)
                    self.connected = True
                    self.token = body.get("access_token")
                    self.transport_url = body.get("urls", {}).get("transport_url")
                    self.userid = body.get("userid")
                    self._access_token = access_token  # original token for cookie auth
                    return True
                else:
                    text = await resp.text()
                    return False
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Protobuf observe — background task
    # ------------------------------------------------------------------

    def _start_observer(self):
        if self._observe_task and not self._observe_task.done():
            return
        self._observer = ProtobufObserver(self.token, self.userid)
        self._observe_task = asyncio.ensure_future(
            self._observer.observe(self._on_proto_traits)
        )
        pass

    def _on_proto_traits(self, traits: list) -> None:
        """Ingest decoded protobuf trait data into proto_devices."""
        for trait_name, device_id, value in traits:

            if trait_name == "located_annotations":
                # Build proto_where_map: UUID -> room name (e.g. "Living Room")
                # Decoder already converts ANNOTATION_xxx IDs to UUIDs and extracts clean labels
                for ann in (value.get("annotations", []) + value.get("customAnnotations", [])):
                    wid = ann.get("whereId", "")
                    label = ann.get("label", "")
                    if wid and label:
                        self.proto_where_map[wid] = label

            elif trait_name == "peer_devices":
                thermostat_types = {
                    "nest.resource.NestLearningThermostat3Resource",
                    "nest.resource.NestAgateDisplayResource",
                    "nest.resource.NestOnyxResource",
                    "google.resource.GoogleZirconium1Resource",
                    "nest.resource.NestLearningThermostat3v2Resource",
                    "nest.resource.NestThermostat3Resource",
                    "nest.resource.NestAmber2DisplayResource",
                }
                for dev in value.get("devices", []):
                    raw_dev_id = dev.get("deviceId", {}).get("value", "")
                    dev_type = dev.get("deviceType", {}).get("value", "")
                    fw = dev.get("fwVersion", "")
                    if not raw_dev_id:
                        continue
                    # Strip prefix like "DEVICE_", keep just the hex MAC ID
                    # e.g. "DEVICE_18B4300000CDA601" -> "18B4300000CDA601"
                    if "_" in raw_dev_id:
                        dev_id = raw_dev_id.split("_", 1)[-1].upper()
                    else:
                        dev_id = raw_dev_id.upper()
                    existing = self.proto_devices.setdefault(dev_id, {"device_id": dev_id})
                    existing["current_version"] = fw
                    existing["_proto_structure_id"] = device_id  # structure UUID from observe
                    if dev_type in thermostat_types:
                        existing["_bucket"] = "device"
                        existing["_device_type"] = dev_type
                    elif dev_type == "nest.resource.NestKryptoniteResource":
                        existing["_bucket"] = "kryptonite"
                    elif dev_type == "yale.resource.LinusLockResource":
                        existing["_bucket"] = "yale"

            else:
                dev = self.proto_devices.setdefault(device_id, {"device_id": device_id})
                self._apply_trait(dev, trait_name, value)

    def _apply_trait(self, dev: dict, trait_name: str, value: dict) -> None:
        """Apply a decoded trait value to a device dict."""
        if trait_name == "device_identity":
            model = value.get("modelName", {}).get("value", "")
            serial = value.get("serialNumber", "")
            fw = value.get("fwVersion", "")
            if model:
                dev["model_name"] = model
            if serial:
                dev["serial_number"] = serial
            if fw:
                dev["current_version"] = fw

        elif trait_name == "device_located_settings":
            dev["where_id"] = value.get("whereId", {}).get("value", "")

        elif trait_name == "liveness":
            dev["is_online"] = "ONLINE" in value.get("status", "")

        elif trait_name == "hvac_equipment_capabilities":
            dev["can_heat"] = value.get("canHeat", False)
            dev["can_cool"] = value.get("canCool", False)

        elif trait_name == "hvac_control":
            s = value.get("settings", {})
            dev["hvac_heater_state"] = s.get("isHeating", False)
            dev["hvac_ac_state"] = s.get("isCooling", False)

        elif trait_name == "target_temperature_settings":
            s = value.get("settings", {})
            active = value.get("active", {}).get("value", 1)
            mode = s.get("hvacMode", "off").lower() if active else "off"
            dev["target_temperature_type"] = mode
            heat = (s.get("targetTemperatureHeat") or {}).get("value")
            cool = (s.get("targetTemperatureCool") or {}).get("value")
            # Only update setpoints if present — partial updates (mode-only) must not
            # wipe previously stored temperatures
            if heat is not None:
                dev["target_temperature_low"] = heat
            if cool is not None:
                dev["target_temperature_high"] = cool
            # Recompute single-point target from whichever setpoints we have
            heat = dev.get("target_temperature_low")
            cool = dev.get("target_temperature_high")
            if mode == "heat" and heat is not None:
                dev["target_temperature"] = heat
            elif mode == "cool" and cool is not None:
                dev["target_temperature"] = cool
            elif mode == "range" and heat is not None and cool is not None:
                dev["target_temperature"] = (heat + cool) / 2

        elif trait_name == "eco_mode_state":
            eco = value.get("ecoEnabled", "OFF")
            dev["eco"] = {"mode": "manual-eco" if eco != "OFF" else "schedule"}
            dev["has_eco_mode"] = True  # capability: device supports eco

        elif trait_name == "eco_mode_settings":
            low = value.get("low", {})
            high = value.get("high", {})
            dev["away_temperature_low"] = (low.get("temperature") or {}).get("value")
            dev["away_temperature_low_enabled"] = low.get("enabled", False)
            dev["away_temperature_high"] = (high.get("temperature") or {}).get("value")
            dev["away_temperature_high_enabled"] = high.get("enabled", False)
            dev["auto_away_enable"] = value.get("autoEcoEnabled", False)

        elif trait_name == "display_settings":
            dev["temperature_scale"] = "F" if value.get("units") == "DEGREES_F" else "C"

        elif trait_name == "fan_control":
            dev["has_fan"] = True
            speed = value.get("currentSpeed", "")
            dev["hvac_fan_state"] = speed in (
                "FAN_SPEED_SETTING_STAGE1", "FAN_SPEED_SETTING_STAGE2", "FAN_SPEED_SETTING_STAGE3"
            )

        elif trait_name == "fan_control_settings":
            dev["has_fan"] = True
            timeout = (value.get("fanTimerTimeout") or {}).get("value", 0)
            dev["fan_timer_timeout"] = timeout
            dev["fan_timer_active"] = bool(timeout)

        elif trait_name == "current_temperature":
            temp = (value.get("temperature") or {}).get("value", {}).get("value")
            if temp is not None:
                dev["current_temperature"] = temp

        elif trait_name == "backplate_temperature":
            temp = (value.get("temperature") or {}).get("value", {}).get("value")
            if temp is not None:
                dev["backplate_temperature"] = temp

        elif trait_name == "current_humidity":
            hum = (value.get("humidity") or {}).get("value", {}).get("value")
            if hum is not None:
                dev["current_humidity"] = hum

        elif trait_name == "remote_comfort_sensing_settings":
            sensors = []
            for s in value.get("associatedRcsSensors", []):
                rid = (s.get("deviceId") or {}).get("resourceId", "")
                if rid:
                    sensor_id = rid.split("_")[-1].upper()
                    sensors.append(f"kryptonite.{sensor_id}")
            dev["_rcs_sensors"] = sensors

        elif trait_name in ("battery", "battery_power_source"):
            dev["battery_status"] = value.get("replacementIndicator", "")
            dev["battery_voltage"] = value.get("assessedVoltage")

        elif trait_name == "bolt_lock":
            dev["bolt_locked"] = value.get("lockedState") == "BOLT_LOCKED_STATE_LOCKED"
            dev["bolt_moving"] = value.get("actuatorState") != "BOLT_ACTUATOR_STATE_OK"
            dev["_bucket"] = "yale"

        elif trait_name == "structure_mode":
            mode = value.get("structureMode", "STRUCTURE_MODE_HOME")
            dev["protobuf_away"] = mode in (
                "STRUCTURE_MODE_AWAY", "STRUCTURE_MODE_SLEEP", "STRUCTURE_MODE_VACATION"
            )

    # ------------------------------------------------------------------
    # Main subscribe loop
    # ------------------------------------------------------------------

    async def subscribe_loop(self, handler: Callable):
        """Run REST polling + protobuf observe, merge and call handler."""
        self._start_observer()

        # Give the observe stream time to deliver initial device list
        await asyncio.sleep(5)

        while True:
            try:
                rest = await self._fetch_rest_data()
                if rest is not None:
                    data = self._build_device_tree(rest)
                    handler(data)
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
            except aiohttp.ClientResponseError as err:
                await asyncio.sleep(API_AUTH_FAIL_RETRY_DELAY_SECONDS)
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(API_RETRY_DELAY_SECONDS)

    # ------------------------------------------------------------------
    # REST data fetch
    # ------------------------------------------------------------------

    async def _fetch_rest_data(self) -> Optional[dict]:
        if not self.token or not self.connected:
            return None

        session = await self._get_session()
        headers = {
            "User-Agent": USER_AGENT_STRING,
            "Authorization": f"Basic {self.token}",
            "X-nl-user-id": self.userid,
            "X-nl-protocol-version": "1",
        }
        url = f"https://{NEST_API_HOSTNAME}/api/0.1/user/{self.userid}/app_launch"
        body = {
            "known_bucket_types": [
                "buckets", "structure", "shared", "topaz", "device",
                "rcs_settings", "kryptonite", "quartz", "track", "where",
            ],
            "known_bucket_versions": [],
        }

        async with session.post(
            url, json=body, headers=headers,
            timeout=aiohttp.ClientTimeout(total=API_TIMEOUT_SECONDS),
        ) as resp:
            if resp.status in (401, 403):
                self.connected = False
                raise aiohttp.ClientResponseError(
                    resp.request_info, resp.history, status=resp.status
                )
            if resp.status != 200:
                raise aiohttp.ClientResponseError(
                    resp.request_info, resp.history, status=resp.status
                )
            raw = await resp.json(content_type=None)

        objects = raw.get("updated_buckets") or raw.get("objects", [])
        if not objects:
            return None

        state: dict = {}
        for obj in objects:
            obj_key = obj.get("object_key", "")
            value = obj.get("value")
            parts = obj_key.split(".")
            if len(parts) < 2:
                continue
            bucket, item_id = parts[0], parts[1]
            if bucket not in state:
                state[bucket] = {}
            existing = state[bucket].get(item_id)
            if existing is not None and isinstance(value, dict):
                existing.update(value)
            else:
                state[bucket][item_id] = value if value is not None else {}

        self.rest_state = state
        buckets = sorted(set(o.get("object_key", "").split(".")[0] for o in objects))

        return state

    # ------------------------------------------------------------------
    # Build device tree from REST + protobuf data
    # ------------------------------------------------------------------

    def _build_device_tree(self, rest: dict) -> dict:
        structures = rest.get("structure", {})
        topaz      = rest.get("topaz", {})
        where_map  = rest.get("where", {})
        track      = rest.get("track", {})

        result: dict = {
            "devices": {
                "thermostats": {},
                "home_away_sensors": {},
                "temp_sensors": {},
                "smoke_co_alarms": {},
                "locks": {},
            },
            "structures": structures,
        }

        proto_thermostat_ids = [
            did for did, d in self.proto_devices.items()
            if d.get("_bucket") == "device"
        ]

        for structure_id, this_structure in structures.items():
            where_lookup: dict[str, str] = {}
            for w in (where_map.get(structure_id) or {}).get("wheres", []):
                if w.get("where_id") and w.get("name"):
                    where_lookup[w["where_id"]] = w["name"]
            where_lookup.update(self.proto_where_map)

            # Home/Away sensor
            struct_name = this_structure.get("name", "")
            ha_name = (
                f"Home Occupied - {struct_name}" if len(structures) > 1 else "Home Occupied"
            )
            result["devices"]["home_away_sensors"][structure_id] = {
                "structure_id": structure_id,
                "device_id": structure_id,
                "serial_number": structure_id,
                "name": ha_name,
                "model": "Home/Away Control",
                "away": this_structure.get("away", False),
            }

            # Nest Protect from REST swarm
            for unit_str in this_structure.get("swarm", []):
                parts = unit_str.split(".")
                if len(parts) < 2:
                    continue
                dev_type, dev_id = parts[0], parts[1]
                if dev_type == "topaz":
                    self._build_protect(dev_id, structure_id, topaz, where_lookup, result)

        # Thermostats from protobuf observe
        for dev_id, dev in self.proto_devices.items():
            if dev.get("_bucket") != "device":
                continue

            # Resolve structure: try proto structure UUID → legacy REST structure_id
            proto_sid = dev.get("_proto_structure_id", "")
            structure_id = self._match_structure(proto_sid, structures)

            where_lookup = {}
            if structure_id:
                for w in (where_map.get(structure_id) or {}).get("wheres", []):
                    if w.get("where_id") and w.get("name"):
                        where_lookup[w["where_id"]] = w["name"]
            where_lookup.update(self.proto_where_map)

            self._build_thermostat(dev_id, dev, structure_id or "", where_lookup, track, result)

        # Locks from protobuf observe
        for dev_id, dev in self.proto_devices.items():
            if dev.get("_bucket") != "yale":
                continue
            where_lookup = dict(self.proto_where_map)
            self._build_lock(dev_id, dev, "", where_lookup, result)

        for group, devs in result["devices"].items():
            if devs:
                pass

        return result

    def _match_structure(self, proto_sid: str, structures: dict) -> Optional[str]:
        """Match a protobuf structure ID to a REST structure ID.
        Proto IDs may be "015EBA35B6494913" (hex, no dashes) while
        REST IDs are like "cf76de20-a21b-11e7-8ff6-12f618586978" (UUID with dashes)."""
        if not proto_sid:
            return next(iter(structures), None)
        if proto_sid in structures:
            return proto_sid
        # Normalize proto_sid: strip dashes and lowercase
        proto_norm = proto_sid.replace("-", "").lower()
        for rest_id in structures:
            rest_norm = rest_id.replace("-", "").lower()
            if rest_norm == proto_norm or rest_norm.endswith(proto_norm) or proto_norm.endswith(rest_norm):
                return rest_id
        # If only one structure, just use it
        if len(structures) == 1:
            return next(iter(structures))
        return None

    # Map protobuf device type -> human-readable model name
    _DEVICE_TYPE_MODELS = {
        "nest.resource.NestLearningThermostat3Resource":   "Nest Learning Thermostat (3rd Gen)",
        "nest.resource.NestLearningThermostat3v2Resource": "Nest Learning Thermostat (3rd Gen)",
        "nest.resource.NestThermostat3Resource":           "Nest Thermostat",
        "nest.resource.NestAgateDisplayResource":          "Nest Learning Thermostat (4th Gen)",
        "nest.resource.NestOnyxResource":                  "Nest Learning Thermostat E",
        "nest.resource.NestAmber2DisplayResource":         "Nest Thermostat E",
        "google.resource.GoogleZirconium1Resource":        "Nest Thermostat",
    }

    def _build_thermostat(self, dev_id, dev, structure_id, where_lookup, track, result):
        t = dict(dev)
        t["device_id"] = dev_id
        t["structure_id"] = structure_id
        t["where_name"] = where_lookup.get(t.get("where_id", ""))

        base_name = t.get("name") or t.get("where_name") or "Nest"
        t["name"] = f"{base_name} Thermostat"
        t["model_name"] = self._DEVICE_TYPE_MODELS.get(
            t.get("_device_type", ""), "Nest Learning Thermostat"
        )

        raw_mode = (t.get("target_temperature_type") or "off").lower()
        t["previous_hvac_mode"] = raw_mode

        eco = t.get("eco")
        if isinstance(eco, dict):
            t["has_eco_mode"] = True
            t["hvac_mode"] = (
                "eco" if eco.get("mode") in ("manual-eco", "auto-eco") else raw_mode
            )
        else:
            t["has_eco_mode"] = False
            t["hvac_mode"] = raw_mode

        t["hvac_state"] = (
            "heating" if t.get("can_heat") and t.get("hvac_heater_state")
            else "cooling" if t.get("can_cool") and t.get("hvac_ac_state")
            else "off"
        )

        # Convert temperatures to display unit if Fahrenheit
        if t.get("temperature_scale") == "F":
            def _c_to_f(v):
                return round(v * 9/5 + 32, 1) if v is not None else None
            t["current_temperature"] = _c_to_f(t.get("current_temperature"))
            t["backplate_temperature"] = _c_to_f(t.get("backplate_temperature"))
            t["target_temperature"] = _c_to_f(t.get("target_temperature"))
            t["target_temperature_low"] = _c_to_f(t.get("target_temperature_low"))
            t["target_temperature_high"] = _c_to_f(t.get("target_temperature_high"))
            t["away_temperature_low"] = _c_to_f(t.get("away_temperature_low"))
            t["away_temperature_high"] = _c_to_f(t.get("away_temperature_high"))

        # Recompute mode-aware target_temperature after unit conversion
        mode_now = t.get("hvac_mode", "off")
        if mode_now == "heat" and t.get("target_temperature_low") is not None:
            t["target_temperature"] = t["target_temperature_low"]
        elif mode_now == "cool" and t.get("target_temperature_high") is not None:
            t["target_temperature"] = t["target_temperature_high"]
        # range: HA uses target_temperature_low/high directly, not target_temperature

        fan_timeout = t.get("fan_timer_timeout") or 0
        t["fan_timer_active"] = fan_timeout > get_unix_time() or bool(t.get("hvac_fan_state"))
        t["software_version"] = t.get("current_version")

        if dev_id in track:
            t["is_online"] = track[dev_id].get("online", False)
        else:
            t.setdefault("is_online", True)

        result["devices"]["thermostats"][dev_id] = t
        pass

    def _build_protect(self, dev_id, structure_id, topaz, where_lookup, result):
        t = dict(topaz.get(dev_id) or {})
        if not t:
            return
        t["device_id"] = dev_id
        t["where_name"] = where_lookup.get(t.get("where_id", ""))
        t["name"] = t.get("description") or t.get("where_name") or "Nest Protect"
        t["smoke_alarm_state"] = "ok" if t.get("smoke_status", 0) == 0 else "emergency"
        t["co_alarm_state"] = "ok" if t.get("co_status", 0) == 0 else "emergency"
        t["battery_health"] = "ok" if t.get("battery_health_state", 0) == 0 else "low"
        t["is_online"] = bool(t.get("component_wifi_test_passed"))
        result["devices"]["smoke_co_alarms"][dev_id] = t

    def _build_lock(self, dev_id, dev, structure_id, where_lookup, result):
        t = dict(dev)
        t["device_id"] = dev_id
        t["structure_id"] = structure_id
        t["software_version"] = t.get("current_version")
        t["where_name"] = where_lookup.get(t.get("where_id", ""))
        base = t.get("description") or t.get("where_name") or "Nest x Yale"
        t["name"] = f"{base} Lock"
        result["devices"]["locks"][dev_id] = t

    # ------------------------------------------------------------------
    # REST update pushing
    # ------------------------------------------------------------------

    async def update_property(
        self, device: str, prop: str, value: Any, hvac_mode: Optional[str] = None,
        extra: dict | None = None,
    ):
        """Route a device property update through the protobuf gRPC write API."""
        parts = device.split(".", 1)
        device_type = parts[0]
        device_id = (parts[1] if len(parts) > 1 else "").upper()
        resource_id = f"DEVICE_{device_id}"

        if device_type == "structure":
            # Structure away: still use REST (no proto write for structure)
            body = {
                "away": value == "away",
                "away_timestamp": get_unix_time(),
                "away_setter": 0,
            }
            await self._commit_update_rest(f"structure.{device_id.lower()}", body)
            return

        elif device_type == "shared":
            if prop == "hvac_mode":
                if value == "eco":
                    await self._proto_write(encode_set_eco_mode(resource_id, True))
                elif value == "eco-off":
                    await self._proto_write(encode_set_eco_mode(resource_id, False))
                else:
                    # Clear eco and set mode, sending current setpoints to prevent
                    # the server from resetting them to 0°C.
                    # Prefer caller-supplied setpoints (extra=), fall back to proto_devices.
                    raw = self.proto_devices.get(device_id, {})
                    heat_c = (extra or {}).get("heat_c") or raw.get("target_temperature_low")
                    cool_c = (extra or {}).get("cool_c") or raw.get("target_temperature_high")
                    await self._proto_write(encode_set_eco_mode(resource_id, False))
                    await asyncio.sleep(0.3)
                    await self._proto_write(encode_set_hvac_mode(
                        resource_id, value,
                        heat_c=heat_c if value in ("heat", "range") else None,
                        cool_c=cool_c if value in ("cool", "range") else None,
                    ))
            elif prop == "target_temperature_range":
                # Range mode: value is {"high": F_or_C, "low": F_or_C}
                # Send both setpoints in a single write to avoid race conditions.
                dev = self.proto_devices.get(device_id, {})
                scale = dev.get("temperature_scale", "C")
                def to_c(v):
                    return round((v - 32) * 5 / 9, 2) if scale == "F" and v is not None else v
                heat_c = to_c(value.get("low"))
                cool_c = to_c(value.get("high"))
                await self._proto_write(encode_set_hvac_mode(
                    resource_id, "range", heat_c=heat_c, cool_c=cool_c,
                ))
            elif prop in ("target_temperature", "target_temperature_low", "target_temperature_high"):
                dev = self.proto_devices.get(device_id, {})
                scale = dev.get("temperature_scale", "C")
                temp_c = round((value - 32) * 5 / 9, 2) if scale == "F" else value
                # Use the hvac_mode passed by the caller (from climate entity state)
                # rather than proto_devices target_temperature_type which may be stale.
                mode = hvac_mode or dev.get("target_temperature_type", "heat")
                # Always send mode with temperature — missing mode field is interpreted
                # as 0 (OFF) by the Nest server, turning the thermostat off.
                if prop == "target_temperature_low" or (prop == "target_temperature" and mode == "heat"):
                    await self._proto_write(encode_set_hvac_mode(
                        resource_id, mode, heat_c=temp_c,
                    ))
                elif prop == "target_temperature_high" or (prop == "target_temperature" and mode == "cool"):
                    await self._proto_write(encode_set_hvac_mode(
                        resource_id, mode, cool_c=temp_c,
                    ))
                else:
                    await self._proto_write(encode_set_hvac_mode(
                        resource_id, mode, heat_c=temp_c,
                    ))

        elif device_type == "device":
            if prop == "fan_timer_active":
                duration = (extra or {}).get("duration_minutes", 15)
                if bool(value):
                    # Set timer duration first, then start the fan
                    await self._proto_write(encode_set_fan_timer(resource_id, duration))
                    await asyncio.sleep(0.2)
                    await self._proto_write(encode_set_fan(resource_id, True))
                else:
                    # Clear timer then turn off fan
                    await self._proto_write(encode_set_fan_timer(resource_id, 0))
                    await asyncio.sleep(0.2)
                    await self._proto_write(encode_set_fan(resource_id, False))
            elif prop == "eco":
                eco_on = isinstance(value, dict) and value.get("mode") == "manual-eco"
                await self._proto_write(encode_set_eco_mode(resource_id, eco_on))

    async def _proto_write(self, grpc_bytes: bytes) -> None:
        """Send a gRPC-Web encoded protobuf write to BatchUpdateState."""
        session = await self._get_session()
        url = f"https://{OBSERVE_HOST}{ENDPOINT_UPDATE}"
        try:
            async with session.post(
                url,
                data=grpc_bytes,
                headers={
                    "User-Agent": USER_AGENT_STRING,
                    "Authorization": f"Basic {self.token}",
                    "Content-Type": "application/grpc-web+proto",
                    "X-Grpc-Web": "1",
                    "X-User-Id": self.userid,
                },
                timeout=aiohttp.ClientTimeout(total=API_TIMEOUT_SECONDS),
            ) as resp:
                body = await resp.read()
                if resp.status == 200:
                    pass
                else:
                    pass
        except Exception:
            pass

    async def _commit_update_rest(self, node_id: str, body: dict) -> None:
        """Legacy REST write for structure-level properties (away, etc.)."""
        session = await self._get_session()
        url = self.transport_url + ENDPOINT_PUT
        try:
            async with session.post(
                url,
                json={"user_id": self.userid, "objects": [create_api_object(node_id, body)]},
                headers={
                    "User-Agent": USER_AGENT_STRING,
                    "Authorization": f"Basic {self.token}",
                    "X-nl-user-id": self.userid,
                    "X-nl-protocol-version": "1",
                },
                timeout=aiohttp.ClientTimeout(total=API_TIMEOUT_SECONDS),
            ) as resp:
                if resp.status == 200:
                    pass
                else:
                    text = await resp.text()
                    pass
        except Exception:
            pass

    async def _push_updates(self, data: Optional[list] = None):
        updates = clone_object(data or self.pending_updates)
        if not updates or not self.token or not self.connected:
            return

        additional_delay = 0.0
        if not data and self.last_mode_change_time:
            elapsed = time.time() - self.last_mode_change_time
            additional_delay = max(API_MODE_CHANGE_DELAY_SECONDS - elapsed, 0)
        if additional_delay:
            await asyncio.sleep(additional_delay)
        if not data:
            self.pending_updates = []

        session = await self._get_session()
        try:
            # Try /v3/3 (older API) with session_token in body
            put_url = self.transport_url + "/v3/3"
            async with session.post(
                put_url,
                json={
                    "user_id": self.userid,
                    "session_token": self._access_token or self.token,
                    "objects": [u["object"] for u in updates],
                },
                headers={
                    "User-Agent": USER_AGENT_STRING,
                    "Authorization": f"Basic {self.token}",
                    "X-nl-user-id": self.userid,
                    "X-nl-protocol-version": "1",
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=API_TIMEOUT_SECONDS),
            ) as resp:
                body_preview = str({u["object"]["object_key"]: u["object"].get("value") for u in updates})[:200]
                if resp.status == 200:
                    self.failed_push_api_calls = 0
                    pass
                else:
                    resp_text = await resp.text()
                    pass
        except Exception:
            self.failed_push_api_calls += 1
