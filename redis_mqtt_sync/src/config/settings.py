"""
Configuration settings for the Redis MQTT Synchronization service.

This module provides centralized configuration management by loading
settings from environment variables and/or configuration files.
"""

import logging
import os
from typing import Any, Dict, List, Optional, Set

# Default configuration values
DEFAULT_CONFIG = {
    # General settings
    "LOG_LEVEL": "INFO",
    "LOG_FORMAT": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    # Redis settings
    "REDIS_HOST": "localhost",
    "REDIS_PORT": 6379,
    "REDIS_PASSWORD": None,
    "REDIS_USERNAME": None,
    "REDIS_SSL": False,
    "REDIS_POOL_SIZE": 10,
    "REDIS_MAX_RETRIES": 5,
    "REDIS_RETRY_INTERVAL": 5,
    "REDIS_RATE_LIMIT": 1000,
    "REDIS_DBS_TO_SEARCH": [0],
    # MQTT settings
    "MQTT_BROKER_HOST": "localhost",
    "MQTT_BROKER_PORT": 1883,
    "MQTT_CLIENT_ID": "redis-mqtt-sync",
    "MQTT_USERNAME": None,
    "MQTT_PASSWORD": None,
    "MQTT_USE_TLS": False,
    "MQTT_QOS": 1,
    "MQTT_RETAIN": False,
    "MQTT_BASE_TOPIC": "redis",
    "MQTT_FILTER_FIELD": None,
    "MQTT_FILTER_VALUES": [],
    # Stream settings
    "REDIS_STREAMS": [],
    "STREAM_AUTO_DISCOVERY": True,
    "STREAM_GROUP": "mqtt-group",
    "STREAM_CONSUMER": "mqtt-sync",
    "STREAM_BATCH_SIZE": 100,
    "STREAM_READ_TIMEOUT_MS": 1000,
}


class Settings:
    """Manages configuration settings for the Redis to MQTT Synchronization Service."""

    def __init__(self) -> None:
        """Initialize with default settings."""
        self._settings = DEFAULT_CONFIG.copy()
        self._configured = False

    def _load_from_env(self) -> None:
        """Load configuration from environment variables."""
        # Process boolean values
        boolean_keys = [
            "REDIS_SSL",
            "MQTT_USE_TLS",
            "MQTT_RETAIN",
            "STREAM_AUTO_DISCOVERY",
        ]

        # Process integer values
        integer_keys = [
            "REDIS_PORT",
            "REDIS_POOL_SIZE",
            "REDIS_MAX_RETRIES",
            "REDIS_RETRY_INTERVAL",
            "REDIS_RATE_LIMIT",
            "MQTT_BROKER_PORT",
            "MQTT_QOS",
            "STREAM_BATCH_SIZE",
            "STREAM_READ_TIMEOUT_MS",
        ]

        # Process list values
        list_keys = ["REDIS_DBS_TO_SEARCH", "REDIS_STREAMS", "MQTT_FILTER_VALUES"]

        # Load all settings from environment variables
        for key in self._settings.keys():
            env_value = os.environ.get(key)
            if env_value is not None:
                # Convert value based on expected type
                if key in boolean_keys:
                    self._settings[key] = env_value.lower() in ["true", "yes", "1", "y"]
                elif key in integer_keys:
                    self._settings[key] = int(env_value)
                elif key in list_keys:
                    # Lists are comma-separated values
                    if env_value:
                        if key == "REDIS_DBS_TO_SEARCH":
                            self._settings[key] = [
                                int(db) for db in env_value.split(",") if db.strip()
                            ]
                        else:
                            self._settings[key] = [
                                item.strip()
                                for item in env_value.split(",")
                                if item.strip()
                            ]
                else:
                    # String values
                    self._settings[key] = env_value

    def _load_from_file(self, config_path: str) -> None:
        """Load configuration from a file (env.conf format)."""
        try:
            if not os.path.exists(config_path):
                logging.warning(f"Configuration file not found: {config_path}")
                return

            with open(config_path, "r") as config_file:
                for line in config_file:
                    line = line.strip()
                    # Skip comments and empty lines
                    if not line or line.startswith("#"):
                        continue

                    # Parse key-value pair
                    if "=" in line:
                        key, value = line.split("=", 1)
                        key = key.strip()
                        value = value.strip()

                        # Skip if not in our settings
                        if key not in self._settings:
                            continue

                        # Process based on expected type
                        if isinstance(self._settings[key], bool):
                            self._settings[key] = value.lower() in [
                                "true",
                                "yes",
                                "1",
                                "y",
                            ]
                        elif isinstance(self._settings[key], int):
                            self._settings[key] = int(value)
                        elif isinstance(self._settings[key], list):
                            if key == "REDIS_DBS_TO_SEARCH":
                                self._settings[key] = [
                                    int(db) for db in value.split(",") if db.strip()
                                ]
                            else:
                                self._settings[key] = [
                                    item.strip()
                                    for item in value.split(",")
                                    if item.strip()
                                ]
                        else:
                            # Handle special case for None values
                            if value.lower() in ["none", "null", ""]:
                                self._settings[key] = None
                            else:
                                self._settings[key] = value
        except Exception as e:
            logging.error(f"Error loading configuration from file {config_path}: {e}")

    def configure(self, config_path: Optional[str] = None) -> None:
        """Configure settings from file and environment variables."""
        if self._configured:
            logging.warning("Settings already configured, skipping reconfiguration")
            return

        # Default config file path
        if config_path is None:
            config_path = os.environ.get("CONFIG_FILE", "/app/env.conf")

        # Load settings in order of precedence
        self._load_from_file(config_path)  # First from config file
        self._load_from_env()  # Then from environment (overrides file settings)

        self._configured = True

    def __getattr__(self, name: str) -> Any:
        """Access settings as attributes."""
        if name in self._settings:
            return self._settings[name]
        raise AttributeError(f"Setting '{name}' not found")


# Singleton instance
settings = Settings()
