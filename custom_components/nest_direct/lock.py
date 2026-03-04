"""Lock platform for Nest Direct - Nest x Yale Lock support."""

from typing import Any, Optional

from homeassistant.components.lock import LockEntity, LockEntityFeature
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
    """Set up Nest x Yale locks."""
    entry_data = hass.data[DOMAIN][entry.entry_id]
    conn: NestConnection = entry_data[DATA_NEST_CONNECTION]
    initial_data = entry_data["data"]

    entities: list = []

    if initial_data:
        for device_id, device in initial_data["devices"]["locks"].items():
            entities.append(NestYaleLock(conn, device_id, device))
        async_add_entities(entities)

    @callback
    async def _on_update(data: dict):
        existing_ids = {e.device_id for e in entities}
        for device_id, device in data["devices"]["locks"].items():
            if device_id in existing_ids:
                for entity in entities:
                    if entity.device_id == device_id:
                        entity.update_device(device)
                        entity.async_write_ha_state()
            else:
                new_entity = NestYaleLock(conn, device_id, device)
                entities.append(new_entity)
                async_add_entities([new_entity])

    entry_data["listeners"].append(_on_update)

class NestYaleLock(LockEntity):
    """Representation of a Nest x Yale Lock."""

    _attr_should_poll = False

    def __init__(self, conn: NestConnection, device_id: str, device: dict):
        self._conn = conn
        self.device_id = device_id
        self._device = device
        self._attr_unique_id = f"nest_lock_{device_id}"
        self._attr_name = device.get("name", "Nest x Yale Lock")

    def update_device(self, device: dict):
        self._device = device

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, self.device_id)},
            "name": self._device.get("name", "Nest x Yale Lock"),
            "manufacturer": "Yale / Google",
            "model": "Nest x Yale Lock",
            "sw_version": self._device.get("software_version"),
        }

    @property
    def is_locked(self) -> Optional[bool]:
        if self._device.get("bolt_moving"):
            return None
        return self._device.get("bolt_locked", False)

    @property
    def is_locking(self) -> bool:
        return bool(
            self._device.get("bolt_moving") and self._device.get("bolt_moving_to")
        )

    @property
    def is_unlocking(self) -> bool:
        return bool(
            self._device.get("bolt_moving") and not self._device.get("bolt_moving_to")
        )

    @property
    def extra_state_attributes(self) -> dict:
        voltage = self._device.get("battery_voltage")
        battery_low = None
        if self._device.get("battery_status"):
            battery_low = self._device["battery_status"] not in (
                "BATTERY_REPLACEMENT_INDICATOR_NOT_AT_ALL",
            )
        return {
            "battery_voltage": voltage,
            "battery_low": battery_low,
            "bolt_moving": self._device.get("bolt_moving", False),
        }

    async def async_lock(self, **kwargs: Any) -> None:
        """Lock the door."""
        # Nest x Yale uses protobuf commands - we issue via the connection's protobuf endpoint.
        # Since protobuf send-command requires full protobuf encoding (which needs the .proto files),
        # we delegate to nest_connection which handles the REST fallback path.
        # The actual lock command mirrors what homebridge-nest does:
        #   BoltLockChangeRequest: state = BOLT_STATE_EXTENDED
        self._device["bolt_moving"] = True
        self._device["bolt_moving_to"] = True
        await self._conn.update_property(f"yale.{self.device_id}", "bolt_lock_target", True)
        self.async_write_ha_state()

    async def async_unlock(self, **kwargs: Any) -> None:
        """Unlock the door."""
        self._device["bolt_moving"] = True
        self._device["bolt_moving_to"] = False
        await self._conn.update_property(f"yale.{self.device_id}", "bolt_lock_target", False)
        self.async_write_ha_state()
