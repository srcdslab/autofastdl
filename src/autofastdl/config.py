"""
Loading, validation and normalisation of the autofastdl configuration.

Configuration is resolved in three steps:

1. the JSON file (``--config``, ``AUTOFASTDL_CONFIG``, or ``config.json``),
2. environment overrides, so credentials can come from a secret store rather
   than a file baked into an image,
3. validation and normalisation, so a bad key fails at startup with a message
   naming it instead of surfacing as a KeyError from a worker thread later on.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List

DEFAULT_CONFIG_PATH = "config.json"

SUPPORTED_PROTOCOLS = ("ftp", "ftps")

REQUIRED_KEYS = (
    "extensions",
    "sources",
    "ftp_host",
    "ftp_path",
    "ftp_user",
    "ftp_password",
)

DEFAULTS: Dict[str, Any] = {
    "threads": 8,
    "debug": False,
    "docker": False,
    "ftp_protocol": "ftp",
    "ignore_names": [],
    "ignore_folders": [],
    # Tuning knobs for the event pipeline; see Reconciler.
    "reconcile_debounce_seconds": 30,
    "created_grace_seconds": 5,
    "queue_high_water": 10000,
}

# Environment overrides. Credentials are included so that deployments can use
# Docker/Kubernetes secrets or CI variables without templating a JSON file.
ENV_OVERRIDES = {
    "AUTOFASTDL_FTP_PROTOCOL": "ftp_protocol",
    "AUTOFASTDL_FTP_HOST": "ftp_host",
    "AUTOFASTDL_FTP_PATH": "ftp_path",
    "AUTOFASTDL_FTP_USER": "ftp_user",
    "AUTOFASTDL_FTP_PASSWORD": "ftp_password",
}

ENV_BOOL_OVERRIDES = {
    "AUTOFASTDL_DEBUG": "debug",
    "AUTOFASTDL_DOCKER": "docker",
}


class ConfigError(Exception):
    """Raised when the configuration is missing or invalid."""


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


def config_path(argv: List[str]) -> str:
    """
    Resolve the config path from --config/-c, then AUTOFASTDL_CONFIG, then the
    historical default of ./config.json.
    """
    for index, arg in enumerate(argv):
        if arg in ("--config", "-c"):
            if index + 1 >= len(argv):
                raise ConfigError(f"{arg} requires a path argument")
            return argv[index + 1]
        if arg.startswith("--config="):
            return arg.split("=", 1)[1]

    return os.environ.get("AUTOFASTDL_CONFIG", DEFAULT_CONFIG_PATH)


def normalise_extensions(extensions: Any) -> tuple:
    """
    Return extensions as a dot-prefixed, lowercase tuple.

    Without the dot, ``str.endswith`` matches any filename merely ending in
    those letters -- 'mymapbsp' and 'foo.notbsp' both counted as maps. Both
    spellings are accepted in the config file for backward compatibility.
    """
    if not isinstance(extensions, (list, tuple)):
        raise ConfigError("extensions must be a list")
    if not extensions:
        raise ConfigError("extensions must not be empty")

    normalised = []
    for extension in extensions:
        if not isinstance(extension, str) or not extension.strip("."):
            raise ConfigError(f"Invalid extension {extension!r}")
        value = extension.strip().lower()
        normalised.append(value if value.startswith(".") else "." + value)
    return tuple(normalised)


def split_path(pathname: str) -> List[str]:
    return [part for part in re.split(r"[\\/]+", pathname) if part]


def apply_env_overrides(config: Dict[str, Any]) -> None:
    for variable, key in ENV_OVERRIDES.items():
        value = os.environ.get(variable)
        if value is not None:
            config[key] = value

    for variable, key in ENV_BOOL_OVERRIDES.items():
        value = os.environ.get(variable)
        if value is not None:
            config[key] = as_bool(value)


def validate(config: Dict[str, Any]) -> None:
    missing = [key for key in REQUIRED_KEYS if key not in config]
    if missing:
        raise ConfigError(
            "Missing required configuration key(s): " + ", ".join(sorted(missing))
        )

    if config["ftp_protocol"] not in SUPPORTED_PROTOCOLS:
        raise ConfigError(
            "Unsupported ftp_protocol {0!r}, expected one of: {1}".format(
                config["ftp_protocol"], ", ".join(SUPPORTED_PROTOCOLS)
            )
        )

    if not isinstance(config["sources"], list) or not config["sources"]:
        raise ConfigError("sources must be a non-empty list of directories")

    for source in config["sources"]:
        if not isinstance(source, str):
            raise ConfigError(f"Invalid source {source!r}, expected a path")

    for key in ("ignore_names", "ignore_folders"):
        if not isinstance(config[key], list):
            raise ConfigError(f"{key} must be a list")

    for key in (
        "threads",
        "reconcile_debounce_seconds",
        "created_grace_seconds",
        "queue_high_water",
    ):
        try:
            config[key] = int(config[key])
        except (TypeError, ValueError):
            raise ConfigError(f"{key} must be an integer, got {config[key]!r}")
        if config[key] < 1:
            raise ConfigError(f"{key} must be >= 1, got {config[key]}")

    autoremove = config.get("autoremove")
    if autoremove is not None:
        if not isinstance(autoremove, dict):
            raise ConfigError("autoremove must be an object")

        priority = autoremove.get("priority")
        if priority is not None and priority not in ("local", "remote"):
            raise ConfigError(
                f"autoremove.priority must be 'local' or 'remote', got {priority!r}"
            )

        for scope in ("local", "remote"):
            section = autoremove.get(scope)
            if section is None:
                continue
            if not isinstance(section, dict):
                raise ConfigError(f"autoremove.{scope} must be an object")
            for unit in ("days", "minutes", "seconds"):
                if unit in section and not isinstance(section[unit], int):
                    raise ConfigError(
                        f"autoremove.{scope}.{unit} must be an integer, "
                        f"got {section[unit]!r}"
                    )


def load(path: str) -> Dict[str, Any]:
    """
    Read, validate and normalise the configuration at ``path``.

    Raises ConfigError with an actionable message on any problem.
    """
    try:
        with open(path, "r") as jsonfile:
            config: Dict[str, Any] = json.load(jsonfile)
    except FileNotFoundError:
        raise ConfigError(
            f"Configuration file not found: {path} "
            "(set --config or AUTOFASTDL_CONFIG to change the location)"
        )
    except json.JSONDecodeError as e:
        raise ConfigError(f"Configuration file {path} is not valid JSON: {e}")

    if not isinstance(config, dict):
        raise ConfigError(f"Configuration file {path} must contain a JSON object")

    for key, value in DEFAULTS.items():
        config.setdefault(key, list(value) if isinstance(value, list) else value)

    apply_env_overrides(config)
    validate(config)

    config["extensions"] = normalise_extensions(config["extensions"])
    config["debug"] = as_bool(config["debug"])
    config["docker"] = as_bool(config["docker"])

    return config


def missing_sources(config: Dict[str, Any]) -> List[str]:
    """Sources that do not currently exist on disk."""
    return [source for source in config["sources"] if not os.path.isdir(source)]
