"""Switch entities for Nest Direct — Eco mode and Fan."""
from __future__ import annotations

from typing import Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, DATA_NEST_CONNECTION

async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    entry_data = hass.data[DOMAIN][entry.entry_id]
    conn = entry_data[DATA_NEST_CONNECTION]
    initial_data = entry_data["data"]

    eco_entities: list[NestEcoSwitch] = []
    fan_entities: list[NestFanSwitch] = []

    if initial_data:
        for device_id, device in initial_data["devices"]["thermostats"].items():
            if device.get("has_eco_mode"):
                eco_entities.append(NestEcoSwitch(conn, entry.entry_id, device_id, device))
            if device.get("has_fan"):
                fan_entities.append(NestFanSwitch(conn, entry.entry_id, device_id, device))

    all_entities = eco_entities + fan_entities
    async_add_entities(all_entities)

    @callback
    async def _on_update(data: dict) -> None:
        existing_eco = {e.device_id for e in eco_entities}
        existing_fan = {e.device_id for e in fan_entities}
        new_entities = []
        for device_id, device in data["devices"]["thermostats"].items():
            # Update existing
            for e in eco_entities:
                if e.device_id == device_id:
                    e.update_device(device)
            for e in fan_entities:
                if e.device_id == device_id:
                    e.update_device(device)
            # Add new
            if device.get("has_eco_mode") and device_id not in existing_eco:
                existing_eco.add(device_id)
                sw = NestEcoSwitch(conn, entry.entry_id, device_id, device)
                eco_entities.append(sw)
                new_entities.append(sw)
            if device.get("has_fan") and device_id not in existing_fan:
                existing_fan.add(device_id)
                sw = NestFanSwitch(conn, entry.entry_id, device_id, device)
                fan_entities.append(sw)
                new_entities.append(sw)
        if new_entities:
            async_add_entities(new_entities)

    entry_data["listeners"].append(_on_update)

class _NestThermostatSwitch(SwitchEntity):
    """Base for thermostat switches."""

    _attr_should_poll = False

    def __init__(self, conn, entry_id: str, device_id: str, device: dict) -> None:
        self._conn = conn
        self._entry_id = entry_id
        self._device_id = device_id
        self._device = device

    @property
    def device_id(self) -> str:
        return self._device_id

    def update_device(self, device: dict) -> None:
        self._device = device
        self.async_write_ha_state()

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._device_id)},
            name=self._device.get("name", "Nest Thermostat"),
            manufacturer="Nest",
            model=self._device.get("model_name", "Nest Learning Thermostat"),
            sw_version=self._device.get("software_version"),
            serial_number=self._device.get("serial_number"),
        )

    @property
    def available(self) -> bool:
        return self._device.get("is_online", True)

class NestEcoSwitch(_NestThermostatSwitch):
    """Eco mode switch for a Nest thermostat."""

    _attr_icon = "mdi:leaf"

    def __init__(self, conn, entry_id: str, device_id: str, device: dict) -> None:
        super().__init__(conn, entry_id, device_id, device)
        self._attr_unique_id = f"nest_eco_{device_id}"
        self._attr_name = f"{device.get('name', 'Nest Thermostat')} Eco Mode"

    @property
    def is_on(self) -> bool:
        eco = self._device.get("eco", {})
        return isinstance(eco, dict) and eco.get("mode") in ("manual-eco", "auto-eco")

    async def async_turn_on(self, **kwargs: Any) -> None:
        # Save current raw setpoints before eco overrides them in the observe stream
        raw = self._conn.proto_devices.get(self._device_id, {})
        if raw.get("target_temperature_low"):
            raw["_pre_eco_heat_c"] = raw["target_temperature_low"]
        if raw.get("target_temperature_high"):
            raw["_pre_eco_cool_c"] = raw["target_temperature_high"]
        self._device["eco"] = {"mode": "manual-eco"}
        self._device["hvac_mode"] = "eco"
        self.async_write_ha_state()
        await self._conn.update_property(f"shared.{self._device_id}", "hvac_mode", "eco")

    async def async_turn_off(self, **kwargs: Any) -> None:
        prev = self._device.get("previous_hvac_mode", "range")
        self._device["eco"] = {"mode": "schedule"}
        self._device["hvac_mode"] = prev
        self.async_write_ha_state()
        # Restore the setpoints saved before eco was enabled.
        # Fall back to current proto_devices values if not saved.
        raw = self._conn.proto_devices.get(self._device_id, {})
        heat_c = raw.get("_pre_eco_heat_c") or raw.get("target_temperature_low")
        cool_c = raw.get("_pre_eco_cool_c") or raw.get("target_temperature_high")
        # Clear saved values
        raw.pop("_pre_eco_heat_c", None)
        raw.pop("_pre_eco_cool_c", None)
        await self._conn.update_property(
            f"shared.{self._device_id}", "hvac_mode", prev,
            extra={"heat_c": heat_c, "cool_c": cool_c},
        )

class NestFanSwitch(_NestThermostatSwitch):
    """Fan run switch for a Nest thermostat."""

    _attr_icon = "mdi:fan"

    def __init__(self, conn, entry_id: str, device_id: str, device: dict) -> None:
        super().__init__(conn, entry_id, device_id, device)
        self._attr_unique_id = f"nest_fan_{device_id}"
        self._attr_name = f"{device.get('name', 'Nest Thermostat')} Fan"

    @property
    def is_on(self) -> bool:
        return bool(self._device.get("fan_timer_active") or self._device.get("hvac_fan_state"))

    async def async_turn_on(self, **kwargs: Any) -> None:
        self._device["fan_timer_active"] = True
        self.async_write_ha_state()
        await self._conn.update_property(f"device.{self._device_id}", "fan_timer_active", True)

    async def async_turn_off(self, **kwargs: Any) -> None:
        self._device["fan_timer_active"] = False
        self.async_write_ha_state()
        await self._conn.update_property(f"device.{self._device_id}", "fan_timer_active", False)
