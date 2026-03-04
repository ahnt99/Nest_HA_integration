"""Binary sensor platform for Nest Direct - Smoke/CO alarms, Home/Away, online status."""

from typing import Optional

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DATA_NEST_CONNECTION, DOMAIN
from .nest_connection import NestConnection

async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Nest binary sensors."""
    entry_data = hass.data[DOMAIN][entry.entry_id]
    initial_data = entry_data["data"]

    entities: list = []

    def _build_entities(data: dict):
        new_entities: list = []
        existing_ids = {e.unique_id for e in entities}

        # Nest Protect smoke sensors
        for device_id, device in data["devices"]["smoke_co_alarms"].items():
            uid_smoke = f"nest_smoke_{device_id}"
            uid_co = f"nest_co_{device_id}"
            uid_battery = f"nest_protect_battery_{device_id}"
            uid_online = f"nest_protect_online_{device_id}"

            if uid_smoke not in existing_ids:
                new_entities.append(NestSmokeSensor(device_id, device))
            if uid_co not in existing_ids:
                new_entities.append(NestCOSensor(device_id, device))
            if uid_battery not in existing_ids:
                new_entities.append(NestProtectBatterySensor(device_id, device))
            if uid_online not in existing_ids:
                new_entities.append(NestProtectOnlineSensor(device_id, device))

        # Home/Away sensors (per structure)
        for structure_id, sensor in data["devices"]["home_away_sensors"].items():
            uid = f"nest_home_away_{structure_id}"
            if uid not in existing_ids:
                new_entities.append(NestHomeAwaySensor(structure_id, sensor))

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

        for entity in entities:
            device_id = getattr(entity, "device_id", None)
            if device_id is None:
                continue

            updated_device = None
            if hasattr(entity, "_is_protect"):
                updated_device = data["devices"]["smoke_co_alarms"].get(device_id)
            elif hasattr(entity, "_is_home_away"):
                updated_device = data["devices"]["home_away_sensors"].get(device_id)

            if updated_device:
                entity.update_device(updated_device)
                entity.async_write_ha_state()

    entry_data["listeners"].append(_on_update)

# ---- Nest Protect ----

class NestSmokeSensor(BinarySensorEntity):
    """Nest Protect smoke detector."""

    _attr_should_poll = False
    _attr_device_class = BinarySensorDeviceClass.SMOKE
    _is_protect = True

    def __init__(self, device_id: str, device: dict):
        self.device_id = device_id
        self._device = device
        self._attr_unique_id = f"nest_smoke_{device_id}"
        self._attr_name = f"{device.get('name', 'Nest Protect')} Smoke"

    def update_device(self, device: dict):
        self._device = device

    @property
    def is_on(self) -> bool:
        return self._device.get("smoke_alarm_state", "ok") != "ok"

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, self.device_id)},
            "name": self._device.get("name", "Nest Protect"),
            "manufacturer": "Google",
            "model": self._device.get("model", "Nest Protect"),
        }

class NestCOSensor(BinarySensorEntity):
    """Nest Protect CO detector."""

    _attr_should_poll = False
    _attr_device_class = BinarySensorDeviceClass.CO
    _is_protect = True

    def __init__(self, device_id: str, device: dict):
        self.device_id = device_id
        self._device = device
        self._attr_unique_id = f"nest_co_{device_id}"
        self._attr_name = f"{device.get('name', 'Nest Protect')} Carbon Monoxide"

    def update_device(self, device: dict):
        self._device = device

    @property
    def is_on(self) -> bool:
        return self._device.get("co_alarm_state", "ok") != "ok"

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, self.device_id)},
            "name": self._device.get("name", "Nest Protect"),
            "manufacturer": "Google",
            "model": self._device.get("model", "Nest Protect"),
        }

class NestProtectBatterySensor(BinarySensorEntity):
    """Nest Protect battery status."""

    _attr_should_poll = False
    _attr_device_class = BinarySensorDeviceClass.BATTERY
    _is_protect = True

    def __init__(self, device_id: str, device: dict):
        self.device_id = device_id
        self._device = device
        self._attr_unique_id = f"nest_protect_battery_{device_id}"
        self._attr_name = f"{device.get('name', 'Nest Protect')} Battery"

    def update_device(self, device: dict):
        self._device = device

    @property
    def is_on(self) -> bool:
        """True means battery is LOW."""
        return self._device.get("battery_health", "ok") != "ok"

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, self.device_id)},
            "name": self._device.get("name", "Nest Protect"),
            "manufacturer": "Google",
            "model": self._device.get("model", "Nest Protect"),
        }

class NestProtectOnlineSensor(BinarySensorEntity):
    """Nest Protect connectivity status."""

    _attr_should_poll = False
    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY
    _is_protect = True

    def __init__(self, device_id: str, device: dict):
        self.device_id = device_id
        self._device = device
        self._attr_unique_id = f"nest_protect_online_{device_id}"
        self._attr_name = f"{device.get('name', 'Nest Protect')} Online"

    def update_device(self, device: dict):
        self._device = device

    @property
    def is_on(self) -> bool:
        return bool(self._device.get("is_online", False))

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, self.device_id)},
            "name": self._device.get("name", "Nest Protect"),
            "manufacturer": "Google",
            "model": self._device.get("model", "Nest Protect"),
        }

# ---- Home/Away ----

class NestHomeAwaySensor(BinarySensorEntity):
    """Nest Home/Away presence sensor (per structure)."""

    _attr_should_poll = False
    _attr_device_class = BinarySensorDeviceClass.PRESENCE
    _is_home_away = True

    def __init__(self, structure_id: str, device: dict):
        self.device_id = structure_id
        self._device = device
        self._attr_unique_id = f"nest_home_away_{structure_id}"
        self._attr_name = device.get("name", "Home Occupied")

    def update_device(self, device: dict):
        self._device = device

    @property
    def is_on(self) -> bool:
        """True = home (not away)."""
        return not self._device.get("away", False)

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, f"structure_{self.device_id}")},
            "name": self._device.get("name", "Nest Home"),
            "manufacturer": "Google",
            "model": "Nest Structure",
        }
