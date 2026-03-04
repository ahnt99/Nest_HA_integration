"""Config flow for Nest Direct integration."""

from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers.selector import (
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .const import CONF_ACCESS_TOKEN, DOMAIN
from .nest_connection import NestConnection

STEP_USER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_ACCESS_TOKEN): TextSelector(
            TextSelectorConfig(type=TextSelectorType.PASSWORD)
        ),
    }
)


class NestDirectConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Nest Direct."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step."""
        if self._async_current_entries():
            return self.async_abort(reason="already_configured")

        if user_input is None:
            return self.async_show_form(
                step_id="user",
                data_schema=STEP_USER_SCHEMA,
            )

        errors: dict[str, str] = {}
        token = user_input.get(CONF_ACCESS_TOKEN, "").strip()

        config = {"access_token": token}
        conn = NestConnection(config)
        try:
            success = await conn.auth()
            if success:
                return self.async_create_entry(
                    title="Nest Direct",
                    data={CONF_ACCESS_TOKEN: token},
                )
            else:
                errors["base"] = "invalid_auth"
        except Exception:  # noqa: BLE001
            errors["base"] = "unknown"
        finally:
            await conn.close()

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_SCHEMA,
            errors=errors,
        )
