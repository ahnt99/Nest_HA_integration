"""Constants for Nest Direct integration."""

DOMAIN = "nest_direct"

# Configuration keys
CONF_ACCESS_TOKEN = "access_token"
CONF_FAN_DURATION_MINUTES = "fan_duration_minutes"
CONF_HOT_WATER_DURATION_MINUTES = "hot_water_duration_minutes"

# Platform names
PLATFORM_CLIMATE = "climate"
PLATFORM_SENSOR = "sensor"
PLATFORM_BINARY_SENSOR = "binary_sensor"
PLATFORM_LOCK = "lock"

PLATFORM_SWITCH = "switch"
PLATFORMS = [PLATFORM_CLIMATE, PLATFORM_SENSOR, PLATFORM_BINARY_SENSOR, PLATFORM_LOCK, PLATFORM_SWITCH]

# API constants
NEST_API_HOSTNAME = "home.nest.com"
URL_NEST_AUTH = "https://home.nest.com/session"
URL_PROTOBUF = "https://grpc-web.production.nest.com"
ENDPOINT_OBSERVE = "/nestlabs.gateway.v2.GatewayService/Observe"
ENDPOINT_UPDATE = "/nestlabs.gateway.v1.TraitBatchApi/BatchUpdateState"
ENDPOINT_SENDCOMMAND = "/nestlabs.gateway.v1.ResourceApi/SendCommand"
ENDPOINT_PUT = "/v5/put"

DEFAULT_FAN_DURATION_MINUTES = 15
DEFAULT_HOT_WATER_DURATION_MINUTES = 30

# Timing
API_TIMEOUT_SECONDS = 40
API_OBSERVE_TIMEOUT_SECONDS = 130
API_RETRY_DELAY_SECONDS = 10
API_AUTH_FAIL_RETRY_DELAY_SECONDS = 15

API_PUSH_DEBOUNCE_SECONDS = 2
API_MODE_CHANGE_DELAY_SECONDS = 7

# Data keys
DATA_NEST_CONNECTION = "nest_connection"
DATA_DEVICES = "devices"
DATA_STRUCTURES = "structures"

USER_AGENT_STRING = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36"
)

OBSERVE_HOST = "grpc-web.production.nest.com"

