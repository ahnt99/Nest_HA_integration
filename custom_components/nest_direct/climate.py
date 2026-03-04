"""Climate platform for Nest Direct - Thermostat support."""

from typing import Any, Optional

from homeassistant.components.climate import (
    ClimateEntity,
    ClimateEntityFeature,
    HVACAction,
    HVACMode,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import ATTR_TEMPERATURE, UnitOfTemperature
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DATA_NEST_CONNECTION, DOMAIN
from .nest_connection import NestConnection

HVAC_MODE_MAP = {
    "heat": HVACMode.HEAT,
    "cool": HVACMode.COOL,
    "range": HVACMode.HEAT_COOL,
    "eco": HVACMode.OFF,
    "off": HVACMode.OFF,
}

HVAC_ACTION_MAP = {
    "heating": HVACAction.HEATING,
    "cooling": HVACAction.COOLING,
    "off": HVACAction.IDLE,
}


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Nest thermostats."""
    entry_data = hass.data[DOMAIN][entry.entry_id]
    conn: NestConnection = entry_data[DATA_NEST_CONNECTION]
    initial_data = entry_data["data"]

    entities = []
    if initial_data:
        for device_id, device in initial_data["devices"]["thermostats"].items():
            entities.append(NestThermostat(hass, conn, entry.entry_id, device_id, device))

    async_add_entities(entities)

    @callback
    async def _on_update(data: dict):
        existing_ids = {e.device_id for e in entities}
        for device_id, device in data["devices"]["thermostats"].items():
            if device_id in existing_ids:
                for entity in entities:
                    if entity.device_id == device_id:
                        entity.update_device(device)
                        entity.async_write_ha_state()
            else:
                new_entity = NestThermostat(hass, conn, entry.entry_id, device_id, device)
                entities.append(new_entity)
                async_add_entities([new_entity])

    entry_data["listeners"].append(_on_update)


class NestThermostat(ClimateEntity):
    """Representation of a Nest Thermostat."""

    _attr_should_poll = False
    _attr_has_entity_name = True

    def __init__(
        self,
        hass: HomeAssistant,
        conn: NestConnection,
        entry_id: str,
        device_id: str,
        device: dict,
    ):
        self.hass = hass
        self._conn = conn
        self._entry_id = entry_id
        self.device_id = device_id
        self._device = device
        self._attr_unique_id = f"nest_climate_{device_id}"
        self._attr_name = None  # Use device name as entity name (avoids double naming)

    def update_device(self, device: dict):
        self._device = device

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self.device_id)},
            name=self._device.get("name", "Nest Thermostat"),
            manufacturer="Nest",
            model=self._device.get("model_name", "Nest Learning Thermostat"),
            sw_version=self._device.get("software_version"),
            serial_number=self._device.get("serial_number"),
        )

    @property
    def temperature_unit(self) -> str:
        return (
            UnitOfTemperature.FAHRENHEIT
            if self._device.get("temperature_scale") == "F"
            else UnitOfTemperature.CELSIUS
        )

    @property
    def precision(self) -> float:
        return 1.0

    @property
    def suggested_display_precision(self) -> int:
        return 0

    @property
    def available(self) -> bool:
        return self._device.get("is_online", True)

    @property
    def current_temperature(self) -> Optional[float]:
        return self._device.get("current_temperature")

    @property
    def current_humidity(self) -> Optional[int]:
        hum = self._device.get("current_humidity")
        return round(hum) if hum is not None else None

    @property
    def target_temperature(self) -> Optional[float]:
        mode = self._device.get("hvac_mode", "off")
        if mode in ("range", "off", "eco"):
            return None
        return self._device.get("target_temperature")

    @property
    def target_temperature_low(self) -> Optional[float]:
        mode = self._device.get("hvac_mode", "off")
        if mode == "eco":
            return self._device.get("away_temperature_low")
        return self._device.get("target_temperature_low")

    @property
    def target_temperature_high(self) -> Optional[float]:
        mode = self._device.get("hvac_mode", "off")
        if mode == "eco":
            return self._device.get("away_temperature_high")
        return self._device.get("target_temperature_high")

    @property
    def hvac_mode(self) -> HVACMode:
        mode = self._device.get("hvac_mode", "off")
        return HVAC_MODE_MAP.get(mode, HVACMode.OFF)

    @property
    def hvac_modes(self) -> list[HVACMode]:
        modes = [HVACMode.OFF]
        if self._device.get("can_heat"):
            modes.append(HVACMode.HEAT)
        if self._device.get("can_cool"):
            modes.append(HVACMode.COOL)
        if self._device.get("can_heat") and self._device.get("can_cool"):
            modes.append(HVACMode.HEAT_COOL)
        return modes

    @property
    def hvac_action(self) -> Optional[HVACAction]:
        state = self._device.get("hvac_state", "off")
        return HVAC_ACTION_MAP.get(state, HVACAction.IDLE)

    @property
    def supported_features(self) -> ClimateEntityFeature:
        features = ClimateEntityFeature(0)
        mode = self._device.get("hvac_mode", "off")
        if mode in ("off", "eco"):
            return features
        can_heat = self._device.get("can_heat", False)
        can_cool = self._device.get("can_cool", False)
        if can_heat and can_cool and mode == "range":
            features |= ClimateEntityFeature.TARGET_TEMPERATURE_RANGE
        elif can_heat or can_cool:
            features |= ClimateEntityFeature.TARGET_TEMPERATURE
        return features

    @property
    def min_temp(self) -> float:
        if self._device.get("temperature_scale") == "F":
            return 50.0
        return 9.0

    @property
    def max_temp(self) -> float:
        if self._device.get("temperature_scale") == "F":
            return 90.0
        return 32.0

    async def async_set_hvac_mode(self, hvac_mode: HVACMode) -> None:
        nest_mode_map = {
            HVACMode.OFF: "off",
            HVACMode.HEAT: "heat",
            HVACMode.COOL: "cool",
            HVACMode.HEAT_COOL: "range",
            HVACMode.AUTO: "eco",
        }
        nest_mode = nest_mode_map.get(hvac_mode, "off")
        self._device["hvac_mode"] = nest_mode
        # Optimistically align target_temperature to the new mode
        heat = self._device.get("target_temperature_low")
        cool = self._device.get("target_temperature_high")
        if nest_mode == "heat" and heat is not None:
            self._device["target_temperature"] = heat
        elif nest_mode == "cool" and cool is not None:
            self._device["target_temperature"] = cool
        if nest_mode == "eco":
            await self._conn.update_property(f"shared.{self.device_id}", "hvac_mode", "eco")
        else:
            await self._conn.update_property(f"shared.{self.device_id}", "hvac_mode", nest_mode)
        self.async_write_ha_state()

    async def async_set_temperature(self, **kwargs: Any) -> None:
        mode = self._device.get("hvac_mode", "off")
        if mode == "range":
            high = kwargs.get("target_temp_high")
            low = kwargs.get("target_temp_low")
            if high is not None:
                self._device["target_temperature_high"] = high
            if low is not None:
                self._device["target_temperature_low"] = low
            # Send a single write with both setpoints to avoid a race condition
            # where two separate writes overwrite each other on the Nest server.
            # Use the updated device values (already F if scale is F).
            await self._conn.update_property(
                f"shared.{self.device_id}", "target_temperature_range",
                {
                    "high": self._device.get("target_temperature_high"),
                    "low": self._device.get("target_temperature_low"),
                },
                mode,
            )
        else:
            temp = kwargs.get(ATTR_TEMPERATURE)
            if temp is not None:
                self._device["target_temperature"] = temp
                if mode == "heat":
                    self._device["target_temperature_low"] = temp
                elif mode == "cool":
                    self._device["target_temperature_high"] = temp
                await self._conn.update_property(
                    f"shared.{self.device_id}", "target_temperature", temp, mode
                )
        self.async_write_ha_state()
