"""Configuration loading, validation and normalisation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pytest

from autofastdl import config as configuration

VALID: Dict[str, Any] = {
    "extensions": ["bsp", "nav"],
    "sources": ["/srv/css/maps"],
    "ftp_host": "127.0.0.1",
    "ftp_path": "/fastdl/css",
    "ftp_user": "user",
    "ftp_password": "pass",
}


def write(tmp_path: Path, **overrides: Any) -> str:
    data = dict(VALID)
    data.update(overrides)
    target = tmp_path / "config.json"
    target.write_text(json.dumps(data))
    return str(target)


class TestConfigPath:
    def test_flag_with_separate_value(self) -> None:
        assert configuration.config_path(["--config", "/etc/a.json"]) == "/etc/a.json"

    def test_short_flag(self) -> None:
        assert configuration.config_path(["-c", "/etc/a.json"]) == "/etc/a.json"

    def test_inline_value(self) -> None:
        assert configuration.config_path(["--config=/etc/b.json"]) == "/etc/b.json"

    def test_flag_without_a_value_is_rejected(self) -> None:
        with pytest.raises(configuration.ConfigError):
            configuration.config_path(["--config"])

    def test_environment_variable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AUTOFASTDL_CONFIG", "/etc/env.json")
        assert configuration.config_path([]) == "/etc/env.json"

    def test_flag_beats_the_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AUTOFASTDL_CONFIG", "/etc/env.json")
        assert configuration.config_path(["--config", "/etc/a.json"]) == "/etc/a.json"

    def test_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("AUTOFASTDL_CONFIG", raising=False)
        assert configuration.config_path([]) == "config.json"


class TestLoadErrors:
    def test_missing_file_names_the_override(self, tmp_path: Path) -> None:
        with pytest.raises(configuration.ConfigError, match="not found"):
            configuration.load(str(tmp_path / "nope.json"))

    def test_invalid_json(self, tmp_path: Path) -> None:
        target = tmp_path / "config.json"
        target.write_text("{nope")
        with pytest.raises(configuration.ConfigError, match="not valid JSON"):
            configuration.load(str(target))

    @pytest.mark.parametrize("key", sorted(VALID))
    def test_each_required_key_is_reported_by_name(
        self, tmp_path: Path, key: str
    ) -> None:
        data = {k: v for k, v in VALID.items() if k != key}
        target = tmp_path / "config.json"
        target.write_text(json.dumps(data))
        with pytest.raises(configuration.ConfigError, match=key):
            configuration.load(str(target))

    @pytest.mark.parametrize(
        "overrides, message",
        [
            ({"ftp_protocol": "sftp"}, "Unsupported ftp_protocol"),
            ({"sources": []}, "non-empty list"),
            ({"sources": "not-a-list"}, "non-empty list"),
            ({"threads": "lots"}, "must be an integer"),
            ({"threads": 0}, "must be >= 1"),
            ({"extensions": []}, "must not be empty"),
            ({"ignore_names": "no"}, "must be a list"),
            ({"autoremove": {"priority": "elsewhere"}}, "priority"),
            ({"autoremove": {"local": {"days": "7"}}}, "must be an integer"),
            ({"autoremove": "yes"}, "must be an object"),
        ],
    )
    def test_invalid_values_are_rejected(
        self, tmp_path: Path, overrides: dict, message: str
    ) -> None:
        with pytest.raises(configuration.ConfigError, match=message):
            configuration.load(write(tmp_path, **overrides))


class TestNormalisation:
    def test_extensions_gain_a_leading_dot(self, tmp_path: Path) -> None:
        config = configuration.load(write(tmp_path, extensions=["bsp", "nav"]))
        assert config["extensions"] == (".bsp", ".nav")

    def test_dotted_extensions_are_accepted_unchanged(self, tmp_path: Path) -> None:
        config = configuration.load(write(tmp_path, extensions=[".bsp"]))
        assert config["extensions"] == (".bsp",)

    def test_extensions_are_lowercased(self, tmp_path: Path) -> None:
        config = configuration.load(write(tmp_path, extensions=["BSP", ".NAV"]))
        assert config["extensions"] == (".bsp", ".nav")

    def test_defaults_are_applied(self, tmp_path: Path) -> None:
        config = configuration.load(write(tmp_path))
        assert config["threads"] == 8
        assert config["ftp_protocol"] == "ftp"
        assert config["ignore_names"] == []
        assert config["debug"] is False

    def test_ftps_is_supported(self, tmp_path: Path) -> None:
        config = configuration.load(write(tmp_path, ftp_protocol="ftps"))
        assert config["ftp_protocol"] == "ftps"


class TestEnvironmentOverrides:
    def test_credentials_come_from_the_environment(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AUTOFASTDL_FTP_USER", "from-secret")
        monkeypatch.setenv("AUTOFASTDL_FTP_PASSWORD", "s3cret")
        config = configuration.load(write(tmp_path))
        assert config["ftp_user"] == "from-secret"
        assert config["ftp_password"] == "s3cret"

    def test_environment_beats_the_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AUTOFASTDL_FTP_HOST", "ftp.example.org")
        config = configuration.load(write(tmp_path, ftp_host="127.0.0.1"))
        assert config["ftp_host"] == "ftp.example.org"

    @pytest.mark.parametrize(
        "value, expected",
        [("true", True), ("1", True), ("yes", True), ("false", False), ("0", False)],
    )
    def test_boolean_overrides(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        value: str,
        expected: bool,
    ) -> None:
        monkeypatch.setenv("AUTOFASTDL_DEBUG", value)
        assert configuration.load(write(tmp_path))["debug"] is expected


class TestSplitPath:
    @pytest.mark.parametrize(
        "path, expected",
        [
            ("/srv/css/maps", ["srv", "css", "maps"]),
            ("srv\\css\\maps", ["srv", "css", "maps"]),
            ("//srv//css//", ["srv", "css"]),
        ],
    )
    def test_components(self, path: str, expected: list) -> None:
        assert configuration.split_path(path) == expected


class TestShippedExamples:
    @pytest.mark.parametrize(
        "name",
        [
            "config.example.fastdl.json",
            "config.example.demos.json",
            "config.example.torchlight.json",
        ],
    )
    def test_example_configs_remain_valid(self, name: str) -> None:
        # These are what users copy; they must keep loading unchanged.
        config = configuration.load(str(Path(__file__).parent.parent / name))
        assert all(ext.startswith(".") for ext in config["extensions"])
