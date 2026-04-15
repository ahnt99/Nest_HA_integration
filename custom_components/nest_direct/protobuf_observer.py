"""
Nest protobuf observe stream client.

Connects to the Nest gRPC/HTTP2 endpoint and streams thermostat trait data.
Decodes the binary protobuf stream without requiring compiled .proto files
by parsing raw wire format using known field numbers from the schema.
"""

import asyncio
import os
import ssl
import struct
import traceback
import uuid
from typing import Any, Callable, Optional

OBSERVE_HOST = "grpc-web.production.nest.com"
OBSERVE_PORT = 443
OBSERVE_PATH = "/nestlabs.gateway.v2.GatewayService/Observe"

# type_url suffix -> trait name used in our trait decoder
TRAIT_TYPE_MAP = {
    # HVAC / Thermostat
    "nest.trait.hvac.TargetTemperatureSettingsTrait": "target_temperature_settings",
    "nest.trait.hvac.HvacControlTrait": "hvac_control",
    "nest.trait.hvac.EcoModeTrait": "eco_mode",
    "nest.trait.hvac.EcoModeStateTrait": "eco_mode_state",
    "nest.trait.hvac.FanControlTrait": "fan_control",
    "nest.trait.hvac.DisplaySettingsTrait": "display_settings",
    "nest.trait.hvac.LeafTrait": "leaf",
    "nest.trait.hvac.TimeToTemperatureTrait": "time_to_temperature",
    "nest.trait.hvac.BackplateInfoTrait": "backplate_version_info",
    # Sensors
    "nest.trait.sensor.TemperatureTrait": "current_temperature",
    "nest.trait.sensor.HumidityTrait": "current_humidity",
    "nest.trait.sensor.SmokeTrait": "smoke",
    "nest.trait.sensor.CarbonMonoxideTrait": "carbon_monoxide",
    "nest.trait.sensor.BatteryVoltageTrait": "battery_voltage",
    "nest.trait.sensor.PassiveInfraredTrait": "passive_infrared",
    "nest.trait.sensor.AmbientLightTrait": "ambient_light",
    # Safety (Protect)
    "nest.trait.safety.SafetyAlarmSmokeTrait": "safety_alarm_smoke",
    "nest.trait.safety.SafetyAlarmCOTrait": "safety_alarm_co",
    "nest.trait.safety.SafetyAlarmRemoteSmokeTrait": "safety_alarm_remote_smoke",
    "nest.trait.safety.SafetyAlarmRemoteCOTrait": "safety_alarm_remote_co",
    "nest.trait.product.protect.SafetySummaryTrait": "safety_summary",
    "nest.trait.product.protect.ProtectDeviceInfoTrait": "protect_device_info",
    "nest.trait.product.protect.LegacyProtectDeviceInfoTrait": "legacy_protect_device_info",
    # Occupancy
    "nest.trait.occupancy.StructureModeTrait": "structure_mode",
    # Device info
    "weave.trait.description.DeviceIdentityTrait": "device_identity",
    "weave.trait.heartbeat.LivenessTrait": "liveness",
    "nest.trait.firmware.FirmwareTrait": "firmware_info",
    "nest.trait.network.WifiInterfaceTrait": "wifi_interface",
}

# ---------------------------------------------------------------------------
# Minimal raw protobuf parser (no compiled .proto files needed)
# ---------------------------------------------------------------------------

def _decode_varint(buf: bytes, pos: int):
    """Read a protobuf varint from buf at pos. Returns (value, new_pos)."""
    result = 0
    shift = 0
    while pos < len(buf):
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7
    raise ValueError("Buffer ended mid-varint")

def _parse_fields(buf: bytes, pos: int = 0) -> dict[int, list]:
    """Parse protobuf wire-format bytes into {field_num: [raw_values]} dict.
    LEN fields are returned as raw bytes; varint/fixed as ints/bytes."""
    fields: dict[int, list] = {}
    end = len(buf)
    while pos < end:
        tag, pos = _decode_varint(buf, pos)
        field_num = tag >> 3
        wire_type = tag & 0x7
        if wire_type == 0:
            val, pos = _decode_varint(buf, pos)
            fields.setdefault(field_num, []).append(val)
        elif wire_type == 2:
            length, pos = _decode_varint(buf, pos)
            val = buf[pos: pos + length]
            pos += length
            fields.setdefault(field_num, []).append(val)
        elif wire_type == 1:
            if pos + 8 > end:
                break
            fields.setdefault(field_num, []).append(buf[pos: pos + 8])
            pos += 8
        elif wire_type == 5:
            if pos + 4 > end:
                break
            fields.setdefault(field_num, []).append(buf[pos: pos + 4])
            pos += 4
        elif wire_type == 3:  # SGROUP (deprecated) — skip matching group
            depth = 1
            while pos < end and depth > 0:
                try:
                    inner_tag, pos = _decode_varint(buf, pos)
                except ValueError:
                    break
                inner_wt = inner_tag & 7
                if inner_wt == 3:
                    depth += 1
                elif inner_wt == 4:
                    depth -= 1
                elif inner_wt == 0:
                    try:
                        _, pos = _decode_varint(buf, pos)
                    except ValueError:
                        break
                elif inner_wt == 1:
                    pos = min(pos + 8, end)
                elif inner_wt == 2:
                    try:
                        inner_len, pos = _decode_varint(buf, pos)
                        pos = min(pos + inner_len, end)
                    except ValueError:
                        break
                elif inner_wt == 5:
                    pos = min(pos + 4, end)
        elif wire_type == 4:  # EGROUP — end of group, stop here
            break
        else:  # wire types 6, 7 — truly invalid
            break
    return fields

def _str(fields: dict, num: int, default: str = "") -> str:
    vals = fields.get(num, [])
    if vals:
        try:
            return vals[0].decode("utf-8")
        except Exception:
            pass
    return default

def _float(data: bytes) -> Optional[float]:
    if len(data) >= 4:
        return struct.unpack("<f", data[:4])[0]
    return None

def _double(data: bytes) -> Optional[float]:
    if len(data) >= 8:
        return struct.unpack("<d", data[:8])[0]
    return None

def _int(fields: dict, num: int, default: int = 0) -> int:
    vals = fields.get(num, [])
    return vals[0] if vals else default

def _sub(fields: dict, num: int) -> Optional[bytes]:
    """Get first LEN field as bytes for sub-message parsing."""
    vals = fields.get(num, [])
    return vals[0] if vals and isinstance(vals[0], bytes) else None

def _float_indirect(data: bytes) -> Optional[float]:
    """Parse a Float_Indirect { float value = 1; } message."""
    if not data:
        return None
    f = _parse_fields(data)
    raw = f.get(1, [])
    if raw and isinstance(raw[0], bytes):
        return _float(raw[0])
    return None

def _int32_indirect(data: bytes) -> Optional[int]:
    """Parse an Int32_Indirect { int32 value = 1; } message."""
    if not data:
        return None
    f = _parse_fields(data)
    return _int(f, 1)

# ---------------------------------------------------------------------------
# Trait decoders — convert raw bytes -> Python dict matching JS field names
# ---------------------------------------------------------------------------

def _decode_target_temperature_settings(data: bytes) -> dict:
    f = _parse_fields(data)
    settings_raw = _sub(f, 1)
    active_raw = _sub(f, 2)
    result = {}
    if settings_raw:
        s = _parse_fields(settings_raw)
        hvac_mode_val = _int(s, 1)
        # HeatCoolMode: 1=HEAT, 2=COOL, 3=RANGE
        mode_map = {0: "off", 1: "heat", 2: "cool", 3: "range"}
        result["hvacMode"] = mode_map.get(hvac_mode_val, "off")
        heat_raw = _sub(s, 4)
        cool_raw = _sub(s, 5)
        if heat_raw:
            result["targetTemperatureHeat"] = {"value": _float_indirect(heat_raw)}
        if cool_raw:
            result["targetTemperatureCool"] = {"value": _float_indirect(cool_raw)}
    if active_raw:
        result["active"] = {"value": _int32_indirect(active_raw)}
    return {"settings": result, "active": result.get("active", {"value": 1})}

def _decode_hvac_control(data: bytes) -> dict:
    f = _parse_fields(data)
    settings_raw = _sub(f, 1)
    result = {"settings": {}}
    if settings_raw:
        s = _parse_fields(settings_raw)
        result["settings"]["isCooling"] = bool(_int(s, 1))
        result["settings"]["isHeating"] = bool(_int(s, 4))
    return result

def _decode_hvac_equipment_capabilities(data: bytes) -> dict:
    f = _parse_fields(data)
    return {
        "canCool": bool(_int(f, 1)),
        "canHeat": bool(_int(f, 4)),
    }

def _decode_eco_mode_state(data: bytes) -> dict:
    f = _parse_fields(data)
    eco_val = _int(f, 1)
    # EcoModeState: 1=OFF, 2=MANUAL_ECO, 3=AUTO_ECO
    eco_map = {0: "OFF", 1: "OFF", 2: "MANUAL_ECO", 3: "AUTO_ECO"}
    return {"ecoEnabled": eco_map.get(eco_val, "OFF")}

def _decode_eco_mode_settings(data: bytes) -> dict:
    f = _parse_fields(data)
    result = {"autoEcoEnabled": bool(_int(f, 1))}
    low_raw = _sub(f, 2)
    high_raw = _sub(f, 3)
    if low_raw:
        lf = _parse_fields(low_raw)
        temp_raw = _sub(lf, 1)
        result["low"] = {
            "temperature": {"value": _float_indirect(temp_raw)},
            "enabled": bool(_int(lf, 2)),
        }
    if high_raw:
        hf = _parse_fields(high_raw)
        temp_raw = _sub(hf, 1)
        result["high"] = {
            "temperature": {"value": _float_indirect(temp_raw)},
            "enabled": bool(_int(hf, 2)),
        }
    return result

def _decode_display_settings(data: bytes) -> dict:
    f = _parse_fields(data)
    units_val = _int(f, 2)
    # TemperatureUnit: 1=DEGREES_C, 2=DEGREES_F
    return {"units": "DEGREES_F" if units_val == 2 else "DEGREES_C"}

def _decode_fan_control(data: bytes) -> dict:
    f = _parse_fields(data)
    speed_val = _int(f, 1)
    # FanSpeedSetting: 1=STAGE1, 2=STAGE2, 3=STAGE3, 4=OFF, 5=AUTO
    speed_map = {0: "FAN_SPEED_SETTING_UNSPECIFIED", 1: "FAN_SPEED_SETTING_STAGE1",
                 2: "FAN_SPEED_SETTING_STAGE2", 3: "FAN_SPEED_SETTING_STAGE3",
                 4: "FAN_SPEED_SETTING_OFF", 5: "FAN_SPEED_SETTING_AUTO"}
    return {"currentSpeed": speed_map.get(speed_val, "FAN_SPEED_SETTING_OFF")}

def _decode_fan_control_settings(data: bytes) -> dict:
    f = _parse_fields(data)
    timeout_raw = _sub(f, 8)
    return {
        "fanTimerTimeout": {"value": _int32_indirect(timeout_raw)} if timeout_raw else None,
    }

def _decode_current_temperature(data: bytes) -> dict:
    f = _parse_fields(data)
    temp_msg = _sub(f, 1)
    if temp_msg:
        tf = _parse_fields(temp_msg)
        val_raw = _sub(tf, 1)
        if val_raw:
            inner = _parse_fields(val_raw)
            float_raw = inner.get(1, [])
            if float_raw and isinstance(float_raw[0], bytes):
                return {"temperature": {"value": {"value": _float(float_raw[0])}}}
    return {}

def _decode_backplate_temperature(data: bytes) -> dict:
    return _decode_current_temperature(data)

def _decode_current_humidity(data: bytes) -> dict:
    f = _parse_fields(data)
    hum_msg = _sub(f, 1)
    if hum_msg:
        hf = _parse_fields(hum_msg)
        val_raw = _sub(hf, 1)
        if val_raw:
            inner = _parse_fields(val_raw)
            float_raw = inner.get(1, [])
            if float_raw and isinstance(float_raw[0], bytes):
                return {"humidity": {"value": {"value": _float(float_raw[0])}}}
    return {}

def _decode_device_identity(data: bytes) -> dict:
    f = _parse_fields(data)
    # field 1 = fwVersion plain string ("6.3-5")
    # field 2 = manufacturer plain string ("Nest") — we hardcode this, so ignore
    # field 3 = serialNumber String_Indirect {field 1 = serial_str}
    # field 4 = modelName String_Indirect {field 1 = "Nest Learning Thermostat Display..."}
    fw = _str(f, 1)
    serial_raw = _sub(f, 3)
    serial = _str(_parse_fields(serial_raw), 1) if serial_raw else ""
    model_raw = _sub(f, 4)
    model = _str(_parse_fields(model_raw), 1) if model_raw else ""
    return {
        "modelName": {"value": model},
        "serialNumber": serial,
        "fwVersion": fw,
    }

def _decode_liveness(data: bytes) -> dict:
    f = _parse_fields(data)
    status_val = _int(f, 1)
    # LivenessStatus: 1=ONLINE, 2=OFFLINE
    return {"status": "LIVENESS_DEVICE_STATUS_ONLINE" if status_val == 1 else "LIVENESS_DEVICE_STATUS_OFFLINE"}

def _decode_peer_devices(data: bytes) -> dict:
    """Decode PeerDevicesTrait — contains device list with IDs and types.
    
    PeerDevicesTrait { repeated PeerDevice devices = 1; }
    PeerDevice { PeerDeviceInfo data = 2; }
    PeerDeviceInfo { String_Indirect device_id = 1; String_Indirect device_type = 2; string fw_version = 5; }
    String_Indirect { string value = 1; }
    """
    f = _parse_fields(data)
    devices = []
    for dev_raw in f.get(1, []):
        if not isinstance(dev_raw, bytes):
            continue
        df = _parse_fields(dev_raw)
        # PeerDevice.data = field 2 (PeerDeviceInfo)
        info_raw = _sub(df, 2)
        if not info_raw:
            continue
        info = _parse_fields(info_raw)
        # PeerDeviceInfo.device_id = field 1 (String_Indirect)
        dev_id_indirect = _sub(info, 1)
        # PeerDeviceInfo.device_type = field 2 (String_Indirect)
        dev_type_indirect = _sub(info, 2)
        # PeerDeviceInfo.fw_version = field 5 (plain string)
        fw = _str(info, 5)

        dev_id = _str(_parse_fields(dev_id_indirect), 1) if dev_id_indirect else ""
        dev_type = _str(_parse_fields(dev_type_indirect), 1) if dev_type_indirect else ""

        devices.append({
            "deviceId": {"value": dev_id},
            "deviceType": {"value": dev_type},
            "fwVersion": fw,
        })
        pass
    return {"devices": devices}

def _decode_device_located_settings(data: bytes) -> dict:
    f = _parse_fields(data)
    # Field 11 = direct UUID string (e.g. "00000000-0000-0000-0000-00010000000c")
    # Field 2  = nested msg { field 1 = "ANNOTATION_000000000000000C" } (fallback)
    where_id = _str(f, 11)
    if not where_id:
        ann_raw = _sub(f, 2)
        if ann_raw:
            ann_id = _str(_parse_fields(ann_raw), 1)
            if ann_id.startswith("ANNOTATION_"):
                where_id = _annotation_id_to_uuid(ann_id)
    return {"whereId": {"value": where_id}}

def _decode_structure_info(data: bytes) -> dict:
    f = _parse_fields(data)
    legacy_raw = _sub(f, 1)
    legacy_id = _str(_parse_fields(legacy_raw), 1) if legacy_raw else ""
    return {"legacyId": legacy_id}

def _annotation_id_to_uuid(ann_id: str) -> str:
    """Convert 'ANNOTATION_000000000000000C' to '00000000-0000-0000-0000-00010000000c'."""
    hex_part = ann_id.replace("ANNOTATION_", "").lower()
    return f"00000000-0000-0000-0000-0001{hex_part[-8:]}"

def _decode_annotation_entry(a_raw: bytes):
    """Decode one annotation entry: returns (where_uuid, label_string)."""
    af = _parse_fields(a_raw)
    # Field 1 = nested msg { field 1 = "ANNOTATION_xxx" }
    where_id_raw = _sub(af, 1)
    ann_id = _str(_parse_fields(where_id_raw), 1) if where_id_raw else ""
    where_uuid = _annotation_id_to_uuid(ann_id) if ann_id.startswith("ANNOTATION_") else ann_id
    # Field 2 = nested msg { field 1 = "Living Room" }
    label_raw = _sub(af, 2)
    label = _str(_parse_fields(label_raw), 1) if label_raw else ""
    return where_uuid, label

def _decode_located_annotations(data: bytes) -> dict:
    """Decode LocatedAnnotationsTrait for where name annotations."""
    f = _parse_fields(data)
    annotations = []
    for a_raw in f.get(1, []):
        if isinstance(a_raw, bytes):
            wid, label = _decode_annotation_entry(a_raw)
            annotations.append({"whereId": wid, "label": label})
    custom_annotations = []
    for a_raw in f.get(2, []):
        if isinstance(a_raw, bytes):
            wid, label = _decode_annotation_entry(a_raw)
            custom_annotations.append({"whereId": wid, "label": label})
    return {"annotations": annotations, "customAnnotations": custom_annotations}

def _decode_structure_mode(data: bytes) -> dict:
    f = _parse_fields(data)
    mode_val = _int(f, 1)
    mode_map = {0: "STRUCTURE_MODE_HOME", 1: "STRUCTURE_MODE_AWAY",
                2: "STRUCTURE_MODE_SLEEP", 3: "STRUCTURE_MODE_VACATION"}
    return {"structureMode": mode_map.get(mode_val, "STRUCTURE_MODE_HOME")}

def _decode_user_info(data: bytes) -> dict:
    f = _parse_fields(data)
    legacy_raw = _sub(f, 1)
    legacy_id = _str(_parse_fields(legacy_raw), 1) if legacy_raw else ""
    return {"legacyId": legacy_id}

def _decode_battery(data: bytes) -> dict:
    f = _parse_fields(data)
    voltage_raw = _sub(f, 2)
    voltage = None
    if voltage_raw:
        vf = _parse_fields(voltage_raw)
        float_raw = vf.get(1, [])
        if float_raw and isinstance(float_raw[0], bytes):
            voltage = _float(float_raw[0])
    indicator_val = _int(f, 1)
    indicator_map = {0: "REPLACE_INDICATOR_UNSPECIFIED", 1: "REPLACE_INDICATOR_LOW",
                     2: "REPLACE_INDICATOR_CRITICAL"}
    return {
        "replacementIndicator": indicator_map.get(indicator_val, "REPLACE_INDICATOR_UNSPECIFIED"),
        "assessedVoltage": voltage,
    }

def _decode_bolt_lock(data: bytes) -> dict:
    f = _parse_fields(data)
    locked_val = _int(f, 1)
    actuator_val = _int(f, 2)
    locked_map = {0: "BOLT_LOCKED_STATE_UNLOCKED", 1: "BOLT_LOCKED_STATE_LOCKED"}
    actuator_map = {0: "BOLT_ACTUATOR_STATE_OK", 1: "BOLT_ACTUATOR_STATE_LOCKING",
                    2: "BOLT_ACTUATOR_STATE_UNLOCKING"}
    return {
        "lockedState": locked_map.get(locked_val, "BOLT_LOCKED_STATE_UNLOCKED"),
        "actuatorState": actuator_map.get(actuator_val, "BOLT_ACTUATOR_STATE_OK"),
    }

def _decode_remote_comfort_sensing_settings(data: bytes) -> dict:
    f = _parse_fields(data)
    sensors = []
    for s_raw in f.get(4, []):
        if isinstance(s_raw, bytes):
            sf = _parse_fields(s_raw)
            dev_id_raw = _sub(sf, 1)
            if dev_id_raw:
                dev_id = _str(_parse_fields(dev_id_raw), 1)
                sensors.append({"deviceId": {"resourceId": dev_id}})
    return {"associatedRcsSensors": sensors}

def _decode_battery_voltage(data: bytes) -> dict:
    """BatteryVoltageTrait: field 1 = batteryVoltage (float_indirect or sfixed32)."""
    f = _parse_fields(data)
    v = _float_indirect(f.get(1, [b""])[0]) if f.get(1) and isinstance(f[1][0], bytes) else None
    return {"battery_voltage": v}

def _decode_smoke(data: bytes) -> dict:
    """SmokeTrait: field 1 = smoke level varint."""
    f = _parse_fields(data)
    return {"smoke_level": f.get(1, [None])[0]}

def _decode_carbon_monoxide(data: bytes) -> dict:
    """CarbonMonoxideTrait: field 1 = co level varint."""
    f = _parse_fields(data)
    return {"co_level": f.get(1, [None])[0]}

def _decode_safety_alarm_smoke(data: bytes) -> dict:
    """SafetyAlarmSmokeTrait."""
    f = _parse_fields(data)
    return {"alarm_state": f.get(1, [None])[0]}

def _decode_safety_alarm_co(data: bytes) -> dict:
    """SafetyAlarmCOTrait."""
    f = _parse_fields(data)
    return {"alarm_state": f.get(1, [None])[0]}

def _decode_safety_summary(data: bytes) -> dict:
    """SafetySummaryTrait: smoke and CO alarm summary."""
    f = _parse_fields(data)
    return {
        "smoke_status": f.get(1, [None])[0],
        "co_status": f.get(2, [None])[0],
        "heat_status": f.get(3, [None])[0],
    }

def _decode_protect_device_info(data: bytes) -> dict:
    """ProtectDeviceInfoTrait: smoke/CO alarm device state."""
    f = _parse_fields(data)
    return {
        "smoke_alarm_state": f.get(1, [None])[0],
        "co_alarm_state": f.get(2, [None])[0],
        "battery_health_state": f.get(3, [None])[0],
    }

def _decode_leaf(data: bytes) -> dict:
    """LeafTrait: leaf displayed."""
    f = _parse_fields(data)
    return {"leaf": bool(f.get(1, [0])[0])}

def _decode_passive_infrared(data: bytes) -> dict:
    """PassiveInfraredTrait."""
    f = _parse_fields(data)
    return {"presence": f.get(1, [None])[0]}

def _decode_ambient_light(data: bytes) -> dict:
    """AmbientLightTrait."""
    f = _parse_fields(data)
    return {"ambient_light": f.get(1, [None])[0]}

def _decode_wifi_interface(data: bytes) -> dict:
    """WifiInterfaceTrait."""
    f = _parse_fields(data)
    return {
        "ssid": _str(f, 1),
        "rssi": f.get(5, [None])[0],
    }

def _decode_firmware_info(data: bytes) -> dict:
    """FirmwareTrait."""
    f = _parse_fields(data)
    return {"firmware_version": _str(f, 1)}

def _decode_eco_mode(data: bytes) -> dict:
    """EcoModeTrait: current eco mode state."""
    f = _parse_fields(data)
    return {"eco_mode": f.get(1, [None])[0]}

def _decode_structure_mode_trait(data: bytes) -> dict:
    """StructureModeTrait: home/away state."""
    f = _parse_fields(data)
    mode_map = {1: "home", 2: "away", 3: "auto-away"}
    mode_val = f.get(1, [None])[0]
    return {"structure_mode": mode_map.get(mode_val, mode_val)}

TRAIT_DECODERS = {
    "target_temperature_settings": _decode_target_temperature_settings,
    "hvac_control": _decode_hvac_control,
    "hvac_equipment_capabilities": _decode_hvac_equipment_capabilities,
    "eco_mode_state": _decode_eco_mode_state,
    "eco_mode_settings": _decode_eco_mode_settings,
    "display_settings": _decode_display_settings,
    "fan_control": _decode_fan_control,
    "fan_control_settings": _decode_fan_control_settings,
    "current_temperature": _decode_current_temperature,
    "backplate_temperature": _decode_backplate_temperature,
    "current_humidity": _decode_current_humidity,
    "device_identity": _decode_device_identity,
    "liveness": _decode_liveness,
    "peer_devices": _decode_peer_devices,
    "device_located_settings": _decode_device_located_settings,
    "structure_info": _decode_structure_info,
    "located_annotations": _decode_located_annotations,
    "structure_mode": _decode_structure_mode,
    "user_info": _decode_user_info,
    "battery": _decode_battery,
    "battery_power_source": _decode_battery,
    "bolt_lock": _decode_bolt_lock,
    "remote_comfort_sensing_settings": _decode_remote_comfort_sensing_settings,
    "battery_voltage": _decode_battery_voltage,
    "smoke": _decode_smoke,
    "carbon_monoxide": _decode_carbon_monoxide,
    "safety_alarm_smoke": _decode_safety_alarm_smoke,
    "safety_alarm_co": _decode_safety_alarm_co,
    "safety_summary": _decode_safety_summary,
    "protect_device_info": _decode_protect_device_info,
    "leaf": _decode_leaf,
    "passive_infrared": _decode_passive_infrared,
    "ambient_light": _decode_ambient_light,
    "wifi_interface": _decode_wifi_interface,
    "firmware_info": _decode_firmware_info,
    "eco_mode": _decode_eco_mode,
    "structure_mode": _decode_structure_mode_trait,
}

# ---------------------------------------------------------------------------
# Convert hex UUID (from protobuf) to legacy MAC-style ID
# ---------------------------------------------------------------------------

def _to_legacy_id(hex_id: str) -> str:
    """Convert protobuf hex device ID to legacy uppercase MAC-style ID.
    e.g. '18b4300000cda601' -> '18B4300000CDA601'"""
    clean = hex_id.replace("-", "").replace("_", "")
    return clean.upper()

# ---------------------------------------------------------------------------
# HTTP chunked transfer encoding decoder
# ---------------------------------------------------------------------------

def _decode_chunked(buf: bytes) -> tuple[bytes, bytes]:
    """Decode HTTP chunked transfer encoding from buf.
    Returns (remaining_undecoded_buf, decoded_data).
    Handles partial chunks — leftover bytes are returned for next call."""
    decoded = b""
    pos = 0
    while pos < len(buf):
        # Find the end of the chunk size line
        crlf = buf.find(b"\r\n", pos)
        if crlf == -1:
            # Incomplete chunk size line — keep entire remainder
            return buf[pos:], decoded

        size_str = buf[pos:crlf].strip()
        pos = crlf + 2  # skip \r\n after size

        # Skip chunk extensions (semicolon separated) and empty lines
        if not size_str or size_str.startswith(b";"):
            continue

        try:
            chunk_size = int(size_str.split(b";")[0].strip(), 16)
        except ValueError:
            # Not a valid chunk size — might be HTTP headers leaking in, skip line
            continue

        if chunk_size == 0:
            # Final chunk — consume trailing \r\n if present
            return b"", decoded

        # Check if we have the full chunk data + trailing \r\n
        end = pos + chunk_size
        if end + 2 > len(buf):
            # Incomplete chunk data — return everything from current chunk start
            return buf[pos - len(size_str) - 2:], decoded

        decoded += buf[pos:end]
        pos = end + 2  # skip trailing \r\n after chunk data

    return b"", decoded

# ---------------------------------------------------------------------------
# Nest varint frame parser
# After de-chunking, body uses: byte[0]=type, varint(length) at [1:], protobuf bytes
# ---------------------------------------------------------------------------

def _parse_nest_frames(buf: bytes) -> tuple[bytes, list[bytes]]:
    """Parse Nest-framed protobuf messages from de-chunked body bytes.
    Returns (remaining_buf, list_of_protobuf_message_bytes)."""
    messages = []
    pos = 0
    while pos < len(buf):
        if pos >= len(buf):
            break
        # byte[0] = frame type byte (skip it, we just want the data)
        if pos + 1 >= len(buf):
            break  # need at least type byte + start of varint
        pos += 1  # skip type byte

        # Read varint length starting at pos
        try:
            length, new_pos = _decode_varint(buf, pos)
        except ValueError:
            # Incomplete varint — keep from frame start
            return buf[pos - 1:], messages

        end = new_pos + length
        if end > len(buf):
            # Incomplete message body — keep from frame start
            return buf[pos - 1:], messages

        message_bytes = buf[new_pos:end]
        messages.append(message_bytes)
        pos = end

    return b"", messages

# ---------------------------------------------------------------------------
# StreamBody parser -> trait list
# Returns list of (trait_name, device_id, decoded_value_dict)
# ---------------------------------------------------------------------------

def _parse_trait_observe_response(data: bytes) -> list[tuple[str, str, dict]]:
    """Parse one TraitObserveResponse message.

    TraitObserveResponse {
        TraitRequest traitRequest = 1;
        TraitStateNotification acceptedState = 2;
        TraitInfo traitInfo = 3;
    }
    TraitRequest { string resourceId = 1; string traitLabel = 2; }
    TraitStateNotification { google.protobuf.Any state = 1; uint64 monotonicVersion = 3; }
    google.protobuf.Any { string type_url = 1; bytes value = 2; }
    """
    try:
        tr = _parse_fields(data)
        req_raw = _sub(tr, 1)
        if not req_raw:
            return []

        req = _parse_fields(req_raw)
        resource_id = _str(req, 1)
        trait_label = _str(req, 2)

        # Field 3 = TraitStateNotification (confirmed from wire analysis)
        state_notif_raw = _sub(tr, 3)
        if not state_notif_raw:
            return []

        sn = _parse_fields(state_notif_raw)
        any_raw = _sub(sn, 1)
        if not any_raw:
            return []

        af = _parse_fields(any_raw)
        type_url = _str(af, 1)
        value_bytes = _sub(af, 2)

        trait_suffix = type_url.split("/")[-1] if "/" in type_url else type_url
        # Prefer trait_label if it has a decoder (handles same type url for multiple traits)
        # e.g. TemperatureTrait used for current_temperature AND backplate_temperature
        if trait_label and trait_label in TRAIT_DECODERS:
            trait_name = trait_label
        else:
            trait_name = TRAIT_TYPE_MAP.get(trait_suffix, trait_label)
        if not trait_name:
            return []

        decoder = TRAIT_DECODERS.get(trait_name)
        if not decoder or not value_bytes:
            return []

        decoded = decoder(value_bytes)
        legacy_id = _decode_resource_id(resource_id)
        return [(trait_name, legacy_id, decoded)]

    except Exception:
        return []

def _decode_resource_id(raw_id: str) -> str:
    """Convert resource ID like 'DEVICE_18B4300000CDA601' to legacy hex ID '18B4300000CDA601'.
    Also handles plain UUIDs (structures) — returns as-is."""
    if "_" in raw_id:
        return raw_id.split("_", 1)[-1].upper()
    return raw_id.upper()

def _parse_stream_body(data: bytes) -> list[tuple[str, str, dict]]:
    """Parse a ResourceObserveResponse protobuf message and return decoded traits.

    The observe stream sends one ResourceObserveResponse per Nest frame:

    message ResourceObserveResponse {
        ResourceRequest resourceRequest = 1;
        ResourceInfo resourceInfo = 2;
        repeated TraitObserveResponse traitResponses = 3;
    }
    message TraitObserveResponse {
        TraitRequest traitRequest = 1;
        TraitStateNotification acceptedState = 2;
        TraitInfo traitInfo = 3;
    }
    message TraitRequest {
        string resourceId = 1;   // e.g. "DEVICE_18B4300000CDA601"
        string traitLabel = 2;   // e.g. "target_temperature_settings"
    }
    message TraitStateNotification {
        google.protobuf.Any state = 1;
    }
    """
    results = []
    try:
        f = _parse_fields(data)

        # ResourceObserveResponse.traitResponses = field 3
        for trait_resp_raw in f.get(3, []):
            if not isinstance(trait_resp_raw, bytes):
                continue
            tr = _parse_fields(trait_resp_raw)

            # TraitObserveResponse.traitRequest = field 1
            req_raw = _sub(tr, 1)
            # TraitObserveResponse.acceptedState = field 2
            state_notif_raw = _sub(tr, 2)

            if not req_raw or not state_notif_raw:
                continue

            req = _parse_fields(req_raw)
            resource_id = _str(req, 1)   # TraitRequest.resourceId
            trait_label = _str(req, 2)   # TraitRequest.traitLabel

            # TraitStateNotification.state = field 1 (google.Any)
            sn = _parse_fields(state_notif_raw)
            any_raw = _sub(sn, 1)
            if not any_raw:
                continue

            af = _parse_fields(any_raw)
            type_url = _str(af, 1)        # google.Any.type_url
            value_bytes = _sub(af, 2)     # google.Any.value

            # Map type_url suffix to trait name
            trait_suffix = type_url.split("/")[-1] if "/" in type_url else type_url
            trait_name = TRAIT_TYPE_MAP.get(trait_suffix, trait_label)
            if not trait_name:
                continue

            decoder = TRAIT_DECODERS.get(trait_name)
            if not decoder or not value_bytes:
                continue

            try:
                decoded = decoder(value_bytes)
                legacy_id = _decode_resource_id(resource_id)
                results.append((trait_name, legacy_id, decoded))
            except Exception:
                pass

    except Exception:
        pass

    return results

# ---------------------------------------------------------------------------
# HTTP/2 observe stream client
# ---------------------------------------------------------------------------

# Load the binary observe payload at module import time (synchronous, before event loop)
_OBSERVE_PAYLOAD: bytes = b""
try:
    _payload_path = os.path.join(os.path.dirname(__file__), "protobuf", "ObserveTraits.protobuf")
    with open(_payload_path, "rb") as _f:
        _OBSERVE_PAYLOAD = _f.read()
    pass
except Exception as _e:
    pass

class ProtobufObserver:
    """Streams thermostat data from Nest's protobuf gRPC observe endpoint."""

    def __init__(self, token: str, userid: str):
        self.token = token
        self.userid = userid
        self._observe_data = _OBSERVE_PAYLOAD
        self._running = False

    async def observe(self, on_traits: Callable[[list], None]) -> None:
        """Connect to observe stream and call on_traits with each decoded trait batch."""
        self._running = True
        while self._running:
            try:
                await self._connect_and_stream(on_traits)
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(30)

    def stop(self):
        self._running = False

    async def _connect_and_stream(self, on_traits: Callable[[list], None]) -> None:
        """Make one HTTP/2 connection and stream data until disconnected."""
        # ssl.create_default_context() does blocking I/O — run in executor
        loop = asyncio.get_event_loop()
        ssl_ctx = await loop.run_in_executor(None, ssl.create_default_context)

        reader, writer = await asyncio.open_connection(
            OBSERVE_HOST, OBSERVE_PORT, ssl=ssl_ctx
        )

        try:
            # Send HTTP/1.1 -> HTTP/2 upgrade is complex.
            # Instead use a raw HTTP/1.1 chunked request — the Nest server
            # accepts HTTP/1.1 for this endpoint with chunked transfer.
            request_id = str(uuid.uuid4())
            payload = self._observe_data

            http_request = (
                f"POST {OBSERVE_PATH} HTTP/1.1\r\n"
                f"Host: {OBSERVE_HOST}\r\n"
                f"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36\r\n"
                f"Content-Type: application/x-protobuf\r\n"
                f"X-Accept-Content-Transfer-Encoding: binary\r\n"
                f"X-Accept-Response-Streaming: true\r\n"
                f"Authorization: Basic {self.token}\r\n"
                f"request-id: {request_id}\r\n"
                f"referer: https://home.nest.com/\r\n"
                f"origin: https://home.nest.com\r\n"
                f"x-nl-webapp-version: NlAppSDKVersion/8.15.0 NlSchemaVersion/2.1.20-87-gce5742894\r\n"
                f"Content-Length: {len(payload)}\r\n"
                f"Connection: keep-alive\r\n"
                f"\r\n"
            ).encode()

            writer.write(http_request)
            writer.write(payload)
            await writer.drain()

            # Read HTTP response headers
            header_buf = b""
            while b"\r\n\r\n" not in header_buf:
                chunk = await asyncio.wait_for(reader.read(4096), timeout=30)
                if not chunk:
                    raise ConnectionError("Server closed connection during headers")
                header_buf += chunk

            header_end = header_buf.index(b"\r\n\r\n") + 4
            headers_raw = header_buf[:header_end].decode("utf-8", errors="replace")
            remainder = header_buf[header_end:]

            first_line = headers_raw.split("\r\n")[0]
            if "200" not in first_line:
                return

            # The response is HTTP/1.1 chunked transfer encoding.
            # Each chunk: HEX_SIZE\r\nDATA\r\n ... ending with 0\r\n\r\n
            # After de-chunking, the body uses Nest's custom varint framing:
            #   byte[0] = frame type, varint(length) starting at byte[1], then protobuf bytes
            raw_buf = remainder      # may contain start of first chunk
            decoded_buf = b""        # de-chunked body bytes (full ResourceObserveResponse)
            parse_pos = 0            # position in decoded_buf up to which we've processed
            while self._running:
                try:
                    chunk = await asyncio.wait_for(reader.read(65536), timeout=120)
                except asyncio.TimeoutError:
                    break
                if not chunk:
                    break

                raw_buf += chunk

                # De-chunk HTTP chunked transfer encoding
                raw_buf, new_data = _decode_chunked(raw_buf)
                if not new_data:
                    continue
                decoded_buf += new_data

                # The de-chunked body is:
                #   ObserveResponse { repeated ResourceObserveResponse responses = 1; }
                # Each field-1 in decoded_buf is a complete ResourceObserveResponse.
                # ResourceObserveResponse { ResourceRequest[1], ResourceInfo[2],
                #                          repeated TraitObserveResponse[3] }
                # Parse incrementally: advance parse_pos as complete field-1 entries arrive.
                new_traits = []
                while parse_pos < len(decoded_buf):
                    save_pos = parse_pos
                    try:
                        tag, next_pos = _decode_varint(decoded_buf, parse_pos)
                    except ValueError:
                        break
                    wire_type = tag & 7
                    field_num = tag >> 3

                    if wire_type == 2:
                        try:
                            length, data_pos = _decode_varint(decoded_buf, next_pos)
                        except ValueError:
                            break
                        end_pos = data_pos + length
                        if end_pos > len(decoded_buf):
                            break  # wait for more data
                        field_bytes = decoded_buf[data_pos:end_pos]
                        parse_pos = end_pos

                        if field_num == 1:
                            # ResourceObserveResponse — scan its field-3 entries
                            ror = _parse_fields(field_bytes)
                            for tor_bytes in ror.get(3, []):
                                if isinstance(tor_bytes, bytes):
                                    new_traits.extend(_parse_trait_observe_response(tor_bytes))
                        # other fields (2=ResourceInfo etc.) — skip

                    elif wire_type == 0:
                        try:
                            _, parse_pos = _decode_varint(decoded_buf, next_pos)
                        except ValueError:
                            parse_pos = save_pos; break
                    elif wire_type == 1:
                        parse_pos = next_pos + 8
                    elif wire_type == 5:
                        parse_pos = next_pos + 4
                    else:
                        parse_pos = save_pos; break

                if new_traits:
                    try:
                        on_traits(new_traits)
                    except Exception:
                        pass

        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

# ---------------------------------------------------------------------------
# Protobuf encoding helpers (for writes via BatchUpdateState)
# ---------------------------------------------------------------------------

def _enc_varint(v: int) -> bytes:
    out = []
    while v > 0x7F:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v & 0x7F)
    return bytes(out)

def _enc_len(field: int, data: bytes) -> bytes:
    return _enc_varint((field << 3) | 2) + _enc_varint(len(data)) + data

def _enc_varint_field(field: int, v: int) -> bytes:
    return _enc_varint((field << 3) | 0) + _enc_varint(v)

def _enc_str(field: int, s: str) -> bytes:
    return _enc_len(field, s.encode("utf-8"))

def _enc_float32(field: int, f: float) -> bytes:
    import struct
    return _enc_varint((field << 3) | 5) + struct.pack("<f", f)

def _enc_double(field: int, f: float) -> bytes:
    import struct
    return _enc_varint((field << 3) | 1) + struct.pack("<d", f)

def _grpc_web_frame(body: bytes) -> bytes:
    """Wrap protobuf bytes in a gRPC-Web data frame: 0x00 + 4-byte big-endian length."""
    import struct
    return b"\x00" + struct.pack(">I", len(body)) + body

def encode_batch_update(resource_id: str, trait_label: str,
                        type_url: str, trait_bytes: bytes) -> bytes:
    """Encode a single-operation BatchUpdateState request as gRPC-Web framed bytes."""
    # TraitRequest { resource_id=1, trait_label=2 }
    trait_req = _enc_str(1, resource_id) + _enc_str(2, trait_label)
    # google.Any { type_url=1, value=2 }
    any_msg = _enc_str(1, type_url) + _enc_len(2, trait_bytes)
    # TraitOperation { trait_request=1, trait_state=2 }
    trait_op = _enc_len(1, trait_req) + _enc_len(2, any_msg)
    # TraitBatchApiRequest { operations=1 }
    batch = _enc_len(1, trait_op)
    return _grpc_web_frame(batch)

def encode_set_hvac_mode(resource_id: str, mode: str,
                         heat_c: float | None = None,
                         cool_c: float | None = None) -> bytes:
    """Encode TargetTemperatureSettingsTrait with hvac_mode and optional setpoints."""
    # HeatCoolMode: 1=HEAT, 2=COOL, 3=RANGE (heat_cool), 4=OFF
    mode_map = {"heat": 1, "cool": 2, "range": 3, "off": 4, "heat_cool": 3}
    mode_val = mode_map.get(mode.lower(), 4)
    # TargetTemperatureSettings { hvac_mode=1, heat_temperature=4, cool_temperature=5 }
    settings = _enc_varint_field(1, mode_val)
    if heat_c is not None:
        settings += _enc_len(4, _enc_float32(1, heat_c))
    if cool_c is not None:
        settings += _enc_len(5, _enc_float32(1, cool_c))
    trait_bytes = _enc_len(1, settings)
    return encode_batch_update(
        resource_id, "target_temperature_settings",
        "type.nestlabs.com/nest.trait.hvac.TargetTemperatureSettingsTrait",
        trait_bytes,
    )

def encode_set_eco_mode(resource_id: str, eco_on: bool) -> bytes:
    """Encode EcoModeStateTrait."""
    # EcoModeState: 1=OFF, 2=MANUAL_ECO
    eco_val = 2 if eco_on else 1
    trait_bytes = _enc_varint_field(1, eco_val)
    return encode_batch_update(
        resource_id, "eco_mode_state",
        "type.nestlabs.com/nest.trait.hvac.EcoModeStateTrait",
        trait_bytes,
    )

def encode_set_temperature(resource_id: str,
                           heat_c: float | None = None,
                           cool_c: float | None = None) -> bytes:
    """Encode TargetTemperatureSettingsTrait with heat/cool setpoints."""
    # TargetTemperatureSettings { heat_temperature=4: Float, cool_temperature=5: Float }
    # Float = message { value=1: float32 }
    settings = b""
    if heat_c is not None:
        float_msg = _enc_float32(1, heat_c)
        settings += _enc_len(4, float_msg)
    if cool_c is not None:
        float_msg = _enc_float32(1, cool_c)
        settings += _enc_len(5, float_msg)
    trait_bytes = _enc_len(1, settings)
    return encode_batch_update(
        resource_id, "target_temperature_settings",
        "type.nestlabs.com/nest.trait.hvac.TargetTemperatureSettingsTrait",
        trait_bytes,
    )

def encode_set_fan(resource_id: str, fan_on: bool, duration_minutes: int = 15) -> bytes:
    """Encode FanControlTrait to turn fan on/off with a timer duration."""
    import time as _time
    # FanControlTrait:
    #   field 1: current_speed (varint) — 1=STAGE1 (on), 4=OFF, 5=AUTO
    #   field 2: fan_timer_speed (varint) — same values
    #   field 3: fan_timer_timeout (Timestamp) — { field 1: seconds int64 }
    speed = 1 if fan_on else 4
    timeout_secs = int(_time.time()) + (duration_minutes * 60) if fan_on else 0
    # Encode timeout as a Timestamp message: field 1 = seconds (int64/varint)
    timeout_msg = _enc_varint_field(1, timeout_secs) if timeout_secs > 0 else b""
    trait_bytes = (
        _enc_varint_field(1, speed)
        + _enc_varint_field(2, speed)
        + (_enc_len(3, timeout_msg) if timeout_msg else _enc_len(3, b""))
    )
    return encode_batch_update(
        resource_id, "fan_control",
        "type.nestlabs.com/nest.trait.hvac.FanControlTrait",
        trait_bytes,
    )

def encode_set_temperature_units(resource_id: str, fahrenheit: bool) -> bytes:
    """Encode DisplaySettingsTrait to change temperature display units."""
    # TemperatureUnit: 1=DEGREES_C, 2=DEGREES_F
    unit_val = 2 if fahrenheit else 1
    trait_bytes = _enc_varint_field(2, unit_val)
    return encode_batch_update(
        resource_id, "display_settings",
        "type.nestlabs.com/nest.trait.display.DisplaySettingsTrait",
        trait_bytes,
    )
