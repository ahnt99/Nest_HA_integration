# Nest Direct

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-orange.svg)](https://github.com/hacs/integration)
![Version](https://img.shields.io/badge/version-1.0.0-blue)
![HA Compatibility](https://img.shields.io/badge/HA-2023.3%2B-green)

A Home Assistant custom integration providing **direct Nest API access** — no Works with Nest (deprecated), no SDM API, no Google Cloud project required. Uses the same undocumented API as the official Nest app.

> Converted and extended from the [homebridge-nest](https://github.com/chrisjshull/homebridge-nest) plugin by Adrian Cable.

---

## Supported Devices

| Device | Entities |
|---|---|
| **Nest Learning Thermostat** (all generations) | Climate, Current Temperature sensor, Current Humidity sensor, Eco Mode switch, Fan switch |
| **Nest Thermostat E** | Climate, Current Temperature sensor, Current Humidity sensor, Eco Mode switch |
| **Nest Temperature Sensor** | Temperature sensor, Battery |
| **Nest Protect** (smoke/CO alarm) | Smoke alarm, CO alarm, Battery, Online status |
| **Nest x Yale Lock** | Lock/Unlock, Battery |
| **Home/Away** (per structure) | Occupancy binary sensor |

---

## Prerequisites

You need a **Nest account** linked to your Nest devices. No developer account or API key is required.

### Getting Your Access Token

1. Log in at [home.nest.com](https://home.nest.com) in your browser
2. In the **same browser**, navigate to [home.nest.com/session](https://home.nest.com/session)
3. You will see a JSON response — find `"access_token"` and copy its value, a long string starting with **`b`**

```json
{"access_token":"b.your_token_here","email":"...","expires_in":"..."}
```

> ⚠️ **Do not log out** of home.nest.com after copying the token — logging out invalidates it immediately. Simply close the browser tab.

---

## Installation

### Via HACS (Recommended)

1. Open HACS in your Home Assistant instance
2. Go to **Integrations** → click the three-dot menu → **Custom Repositories**
3. Add this repository URL and select **Integration** as the category
4. Click **Add**
5. Search for **Nest Direct** and click **Download**
6. Restart Home Assistant

### Manual

1. Download the latest release zip from the [Releases](../../releases) page
2. Extract the `nest_direct` folder into your HA `config/custom_components/` directory:

```
config/
└── custom_components/
    └── nest_direct/
        ├── __init__.py
        ├── climate.py
        ├── sensor.py
        └── ...
```

3. Restart Home Assistant

---

## Setup

1. Go to **Settings → Devices & Services → Add Integration**
2. Search for **Nest Direct**
3. Enter your access token when prompted
4. Home Assistant will discover all your Nest devices automatically

---

## Entities

### Thermostat

Each thermostat creates the following entities:

| Entity | Type | Description |
|---|---|---|
| `climate.<name>_thermostat` | Climate | HVAC control — heat, cool, heat/cool, off |
| `sensor.<name>_thermostat_current_temperature` | Sensor | Current room temperature |
| `sensor.<name>_thermostat_current_humidity` | Sensor | Current humidity % |
| `switch.<name>_thermostat_eco_mode` | Switch | Toggle eco mode on/off |
| `switch.<name>_thermostat_fan` | Switch | Run fan manually |

The climate entity supports:
- **HVAC modes:** Off, Heat, Cool, Heat/Cool
- **Eco mode:** Controlled via the dedicated Eco Mode switch
- **Current humidity:** Displayed in the climate card
- **Temperature unit:** Follows the thermostat's configured unit (°F or °C)

### Nest Protect

| Entity | Type | Description |
|---|---|---|
| `binary_sensor.<name>_smoke` | Binary Sensor | Smoke alarm (`on` = emergency) |
| `binary_sensor.<name>_co` | Binary Sensor | CO alarm (`on` = emergency) |
| `binary_sensor.<name>_battery` | Binary Sensor | Battery low (`on` = low) |
| `binary_sensor.<name>_online` | Binary Sensor | Device online status |

### Nest Temperature Sensor

| Entity | Type | Description |
|---|---|---|
| `sensor.<name>_temperature` | Sensor | Current temperature |

### Home/Away

| Entity | Type | Description |
|---|---|---|
| `binary_sensor.home_occupied` | Binary Sensor | `on` = home, `off` = away |

---

## Notes

- This integration uses the same undocumented Nest API as the official Nest app. It is not affiliated with or endorsed by Google or Nest Labs.
- The integration maintains a persistent protobuf gRPC streaming connection for real-time updates alongside a 30-second REST polling interval.
- Newer thermostats (Learning Thermostat 3rd gen+) use a gRPC/protobuf API and do not appear in the standard Nest REST API — this integration handles both transparently.
- The access token expires periodically. If devices stop updating, re-authenticate by removing and re-adding the integration.

---

## Troubleshooting

| Problem | Solution |
|---|---|
| Authentication fails | Ensure you copied the first `access_token` from home.nest.com/session and have not logged out |
| Devices not appearing | Check HA logs; ensure your Nest account has devices linked |
| Thermostat shows unavailable | Device may be offline — check the physical device and the Nest app |
| Temperature changes not applying | Confirm the thermostat is not in Eco mode (eco overrides manual setpoints) |
| Integration fails to load after update | Delete the entire `custom_components/nest_direct/` folder and re-extract the new version fresh |

---

## License

This project is open source. See [LICENSE](LICENSE) for details.
