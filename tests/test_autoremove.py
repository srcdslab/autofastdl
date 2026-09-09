"""Retention logic: the days/minutes/seconds window and its config guards."""

from __future__ import annotations

import datetime
from typing import Any, Dict

import pytest
from conftest import FakeFTP

from autofastdl import autofastdl as app


def utc_ago(**delta: int) -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(**delta)


class TestIsOutdated:
    def test_file_older_than_the_window_is_outdated(
        self, config: Dict[str, Any]
    ) -> None:
        config["autoremove"] = {"remote": {"days": 7}}
        assert app.AutoRemove.IsOutdated(utc_ago(days=8), "remote")

    def test_file_inside_the_window_is_kept(self, config: Dict[str, Any]) -> None:
        config["autoremove"] = {"remote": {"days": 7}}
        assert not app.AutoRemove.IsOutdated(utc_ago(days=1), "remote")

    def test_units_accumulate(self, config: Dict[str, Any]) -> None:
        config["autoremove"] = {"remote": {"days": 1, "minutes": 30, "seconds": 30}}
        assert app.AutoRemove.IsOutdated(utc_ago(days=1, minutes=31), "remote")
        assert not app.AutoRemove.IsOutdated(utc_ago(days=1, minutes=29), "remote")

    def test_no_retention_configured_never_expires(
        self, config: Dict[str, Any]
    ) -> None:
        # A zero window must not mean "everything is outdated".
        config["autoremove"] = {"remote": {"remove": True}}
        assert not app.AutoRemove.IsOutdated(utc_ago(days=3650), "remote")

    def test_absent_autoremove_block_never_expires(
        self, config: Dict[str, Any]
    ) -> None:
        config.pop("autoremove", None)
        assert not app.AutoRemove.IsOutdated(utc_ago(days=3650), "remote")

    def test_naive_timestamps_are_treated_as_utc(self, config: Dict[str, Any]) -> None:
        # MDTM returns UTC as a naive datetime; comparing it against local
        # now() shifted the window by the host's UTC offset.
        config["autoremove"] = {"remote": {"days": 7}}
        naive = utc_ago(days=8).replace(tzinfo=None)
        assert app.AutoRemove.IsOutdated(naive, "remote")

    def test_local_and_remote_windows_are_independent(
        self, config: Dict[str, Any]
    ) -> None:
        config["autoremove"] = {"local": {"days": 1}, "remote": {"days": 30}}
        stamp = utc_ago(days=7)
        assert app.AutoRemove.IsOutdated(stamp, "local")
        assert not app.AutoRemove.IsOutdated(stamp, "remote")


class TestTimestamps:
    def test_ftp_timestamp_is_timezone_aware_utc(self, config: Dict[str, Any]) -> None:
        ftp = FakeFTP()
        ftp.mdtm["/fastdl/css/maps/a.bsp.bz2"] = "20240101120000"
        stamp = app.AutoRemove.GetTimestampFTP(ftp, "/fastdl/css/maps/a.bsp.bz2")
        assert stamp.tzinfo is not None
        assert stamp.utcoffset() == datetime.timedelta(0)

    def test_local_timestamp_is_timezone_aware_utc(self, tmp_path: Any) -> None:
        target = tmp_path / "a.bsp"
        target.write_bytes(b"x")
        stamp = app.AutoRemove.GetTimestampFile(str(target))
        assert stamp.tzinfo is not None
        assert stamp.utcoffset() == datetime.timedelta(0)


class TestConfigGuards:
    @pytest.mark.parametrize(
        "block, method, expected",
        [
            ({"remote": {"remove": True}}, "remote", True),
            ({"remote": {"remove": False}}, "remote", False),
            ({"remote": {}}, "remote", False),
            ({}, "remote", False),
        ],
    )
    def test_is_file_removed(
        self, config: Dict[str, Any], block: dict, method: str, expected: bool
    ) -> None:
        config["autoremove"] = block
        assert app.AutoRemove.IsFileRemoved(method) is expected

    @pytest.mark.parametrize(
        "block, expected",
        [
            ({"remote": {"autoclean": True}}, True),
            ({"remote": {"startup_clean": True}}, False),
            ({}, False),
        ],
    )
    def test_is_auto_cleaned(
        self, config: Dict[str, Any], block: dict, expected: bool
    ) -> None:
        config["autoremove"] = block
        assert app.AutoRemove.IsAutoCleaned("remote") is expected

    def test_after_upload_guard(self, config: Dict[str, Any]) -> None:
        config["autoremove"] = {"after_upload": True}
        assert app.AutoRemove.IsFileRemovedAfterUpload()
        config["autoremove"] = {}
        assert not app.AutoRemove.IsFileRemovedAfterUpload()


class TestCheckDirFTPGating:
    """
    CheckDirFTP was gated on startup_clean no matter why it ran, so
    `autoclean: true` alone silently did nothing.
    """

    @pytest.mark.parametrize(
        "block, trigger, should_run",
        [
            ({"remote": {"autoclean": True}}, "autoclean", True),
            ({"remote": {"startup_clean": True}}, "autoclean", False),
            ({"remote": {"startup_clean": True}}, "startup_clean", True),
            ({"remote": {"autoclean": True}}, "startup_clean", False),
        ],
    )
    def test_trigger_selects_the_matching_flag(
        self,
        config: Dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
        block: dict,
        trigger: str,
        should_run: bool,
    ) -> None:
        config["autoremove"] = block
        walked = []
        monkeypatch.setattr(
            app.FTPHelper,
            "walk",
            staticmethod(lambda ftp, path: walked.append(path) or iter([])),
        )
        app.AutoRemove.CheckDirFTP(FakeFTP(), "/fastdl/css", False, trigger)
        assert bool(walked) is should_run
