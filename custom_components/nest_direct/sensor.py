"""Sensor platform for Nest Direct - Temperature sensors, humidity, and home/away."""

from typing import Optional

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import PERCENTAGE, UnitOfTemperature
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DATA_NEST_CONNECTION, DOMAIN
from .nest_connection import NestConnection

async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Nest sensors."""
    entry_data = hass.data[DOMAIN][entry.entry_id]
    initial_data = entry_data["data"]

    entities: list = []

    def _build_entities(data: dict):
        new_entities: list = []
        existing_ids = {e.unique_id for e in entities}

        # Temperature sensors (Nest Temperature Sensor puck)
        for device_id, device in data["devices"]["temp_sensors"].items():
            uid = f"nest_temp_sensor_{device_id}"
            if uid not in existing_ids:
                new_entities.append(NestTemperatureSensor(device_id, device))

        # Thermostat temperature and humidity sensors
        for device_id, device in data["devices"]["thermostats"].items():
            temp_uid = f"nest_current_temperature_{device_id}"
            if temp_uid not in existing_ids and device.get("current_temperature") is not None:
                new_entities.append(NestThermostatTemperatureSensor(device_id, device))
            hum_uid = f"nest_current_humidity_{device_id}"
            if hum_uid not in existing_ids and device.get("current_humidity") is not None:
                new_entities.append(NestHumiditySensor(device_id, device))

        return new_entities

    if initial_data:
        entities.extend(_build_entities(initial_data))
        async_add_entities(entities)

    @callback
    async def _on_update(data: dict):
        new_entities = _build_entities(data)
        if new_entities:
            entities.extend(new_entities)
            async_add_entities(new_entities)

        # Update existing
        for entity in entities:
            if hasattr(entity, "update_device"):
                device_id = entity.device_id
                # Find in appropriate dict
                for group in ["temp_sensors", "thermostats"]:
                    if device_id in data["devices"][group]:
                        entity.update_device(data["devices"][group][device_id])
                        entity.async_write_ha_state()
                        break
                else:
                    # Check all groups
                    for group, devs in data["devices"].items():
                        if device_id in devs:
                            entity.update_device(devs[device_id])
                            entity.async_write_ha_state()
                            break

    entry_data["listeners"].append(_on_update)

class NestTemperatureSensor(SensorEntity):
    """Represents a Nest Temperature Sensor puck."""

    _attr_should_poll = False
    _attr_device_class = SensorDeviceClass.TEMPERATURE
    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(self, device_id: str, device: dict):
        self.device_id = device_id
        self._device = device
        self._attr_unique_id = f"nest_temp_sensor_{device_id}"
        self._attr_name = device.get("name", "Nest Temperature Sensor")
        scale = device.get("temperature_scale", "C")
        self._attr_native_unit_of_measurement = (
            UnitOfTemperature.FAHRENHEIT if scale == "F" else UnitOfTemperature.CELSIUS
        )

    def update_device(self, device: dict):
        self._device = device

    @property
    def native_value(self) -> Optional[float]:
        return self._device.get("current_temperature")

    @property
    def extra_state_attributes(self) -> dict:
        voltage = self._device.get("battery_voltage", 0)
        return {
            "battery_voltage": voltage,
            "battery_low": voltage < 2.66 if voltage else None,
            "serial_number": self._device.get("serial_number"),
            "thermostat_device_id": self._device.get("thermostat_device_id"),
        }

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, self.device_id)},
            "name": self._device.get("name", "Nest Temperature Sensor"),
            "manufacturer": "Nest",
            "model": "Nest Temperature Sensor",
            "sw_version": self._device.get("software_version"),
            "serial_number": self._device.get("serial_number"),
        }

class NestThermostatTemperatureSensor(SensorEntity):
    """Thermostat current room temperature sensor."""

    _attr_should_poll = False
    _attr_has_entity_name = True
    _attr_name = "Current Temperature"
    _attr_device_class = SensorDeviceClass.TEMPERATURE
    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(self, device_id: str, device: dict):
        self.device_id = device_id
        self._device = device
        self._attr_unique_id = f"nest_current_temperature_{device_id}"
        self._update_unit()

    def _update_unit(self):
        scale = self._device.get("temperature_scale", "C")
        self._attr_native_unit_of_measurement = (
            UnitOfTemperature.FAHRENHEIT if scale == "F" else UnitOfTemperature.CELSIUS
        )

    def update_device(self, device: dict):
        self._device = device
        self._update_unit()

    @property
    def native_value(self) -> Optional[float]:
        temp = self._device.get("current_temperature")
        return round(temp, 1) if temp is not None else None

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

class NestHumiditySensor(SensorEntity):
    """Represents humidity reported by a Nest Thermostat."""

    _attr_should_poll = False
    _attr_has_entity_name = True
    _attr_name = "Current Humidity"
    _attr_device_class = SensorDeviceClass.HUMIDITY
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement = PERCENTAGE

    def __init__(self, device_id: str, device: dict):
        self.device_id = device_id
        self._device = device
        self._attr_unique_id = f"nest_current_humidity_{device_id}"

    def update_device(self, device: dict):
        self._device = device

    @property
    def native_value(self) -> Optional[int]:
        hum = self._device.get("current_humidity")
        return round(hum) if hum is not None else None

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
