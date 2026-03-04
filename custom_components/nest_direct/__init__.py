"""Nest Direct Home Assistant Integration."""

import asyncio

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady

from .const import (
    CONF_ACCESS_TOKEN,
    DATA_NEST_CONNECTION,
    DOMAIN,
    PLATFORMS,
)
from .nest_connection import NestConnection


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Nest Direct from a config entry."""
    config = {"access_token": entry.data[CONF_ACCESS_TOKEN]}

    conn = NestConnection(config)

    if not await conn.auth():
        raise ConfigEntryNotReady("Failed to authenticate with Nest — check your access token.")

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = {
        DATA_NEST_CONNECTION: conn,
        "data": None,
        "listeners": [],
    }

    # Start the subscribe loop and wait for first data
    first_data_event = asyncio.Event()

    def _on_data_update(data: dict):
        hass.data[DOMAIN][entry.entry_id]["data"] = data
        if not first_data_event.is_set():
            first_data_event.set()
        for listener in hass.data[DOMAIN][entry.entry_id]["listeners"]:
            hass.async_create_task(listener(data))

    subscribe_task = asyncio.ensure_future(conn.subscribe_loop(_on_data_update))
    hass.data[DOMAIN][entry.entry_id]["subscribe_task"] = subscribe_task

    # Wait up to 60 seconds — protobuf observe stream needs time to connect and deliver data
    try:
        await asyncio.wait_for(first_data_event.wait(), timeout=60)
    except asyncio.TimeoutError:
        subscribe_task.cancel()
        await conn.close()
        raise ConfigEntryNotReady(
            "Timed out waiting for Nest data. "
            "The protobuf observe stream may be blocked — check your network/firewall."
        )

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)

    if unload_ok:
        entry_data = hass.data[DOMAIN].pop(entry.entry_id)
        task = entry_data.get("subscribe_task")
        if task:
            task.cancel()
        conn = entry_data[DATA_NEST_CONNECTION]
        await conn.close()

    return unload_ok
