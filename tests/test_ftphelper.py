"""FTP helpers: listing cache, connection recovery, and the upload path."""

from __future__ import annotations

import ftplib
import queue
import threading
from typing import Any, Dict

import pytest
from conftest import FakeFTP, dir_line, file_line

from autofastdl import autofastdl as app


@pytest.fixture(autouse=True)
def reset_cache() -> None:
    # Clear the attributes rather than rebinding _cache: replacing the object
    # would substitute the production storage type and hide a regression from
    # threading.local() back to shared state.
    for attribute in ("ftp", "path", "resp"):
        try:
            delattr(app.FTPHelper._cache, attribute)
        except AttributeError:
            pass


class TestFileExists:
    def test_detects_a_file_in_the_listing(self, config: Dict[str, Any]) -> None:
        ftp = FakeFTP({"/fastdl/css/maps": [file_line("a.bsp.bz2")]})
        assert app.FTPHelper.file_exists(ftp, "/fastdl/css/maps/a.bsp.bz2")
        assert not app.FTPHelper.file_exists(ftp, "/fastdl/css/maps/b.bsp.bz2")

    def test_directories_are_not_files(self, config: Dict[str, Any]) -> None:
        ftp = FakeFTP({"/fastdl/css": [dir_line("maps")]})
        assert not app.FTPHelper.file_exists(ftp, "/fastdl/css/maps")

    def test_cache_is_per_thread(self, config: Dict[str, Any]) -> None:
        """
        The cache used to live on the shared function object while each worker
        held its own connection, so one thread could answer from a listing
        another had fetched for a different directory.
        """
        listing = {
            "/fastdl/a": [file_line("only-in-a.bz2")],
            "/fastdl/b": [file_line("only-in-b.bz2")],
        }

        # Populate this thread's cache.
        assert app.FTPHelper.file_exists(FakeFTP(listing), "/fastdl/a/only-in-a.bz2")
        assert app.FTPHelper._cache.path == "/fastdl/a"

        seen: Dict[str, Any] = {}

        def other_worker() -> None:
            # A second worker must start from an empty cache rather than
            # inheriting -- or clobbering -- this one's.
            seen["before"] = getattr(app.FTPHelper._cache, "path", None)
            seen["result"] = app.FTPHelper.file_exists(
                FakeFTP(listing), "/fastdl/b/only-in-b.bz2"
            )
            seen["after"] = app.FTPHelper._cache.path

        thread = threading.Thread(target=other_worker)
        thread.start()
        thread.join()

        assert seen["before"] is None
        assert seen["result"] is True
        assert seen["after"] == "/fastdl/b"
        # The other worker must not have disturbed this thread's cache.
        assert app.FTPHelper._cache.path == "/fastdl/a"

    def test_failed_listing_does_not_poison_the_cache(
        self, config: Dict[str, Any]
    ) -> None:
        class Failing(FakeFTP):
            def dir(self, path: str, callback: Any) -> None:
                raise ftplib.error_temp("421 timeout")

        ftp = Failing()
        assert not app.FTPHelper.file_exists(ftp, "/fastdl/css/maps/a.bsp.bz2")
        # A later good listing must be consulted rather than a stale empty one.
        good = FakeFTP({"/fastdl/css/maps": [file_line("a.bsp.bz2")]})
        assert app.FTPHelper.file_exists(good, "/fastdl/css/maps/a.bsp.bz2")


class TestEnsureConnection:
    def test_returns_a_live_connection_unchanged(self, config: Dict[str, Any]) -> None:
        ftp = FakeFTP()
        assert app.FTPHelper.EnsureConnection(ftp) is ftp

    def test_reconnects_after_a_transient_failure(
        self, config: Dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(app, "sleep", lambda seconds: None)
        replacement = FakeFTP()
        monkeypatch.setattr(
            app.FTPHelper, "GetConnection", staticmethod(lambda: replacement)
        )
        dead = FakeFTP()
        dead.noop_failures = 1
        assert app.FTPHelper.EnsureConnection(dead) is replacement

    def test_raises_once_the_retry_budget_is_spent(
        self, config: Dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        `while retry < retry_max` exits with retry == retry_max, so the
        following `if retry > retry_max` bailout could never fire and the job
        ran against a dead connection.
        """
        monkeypatch.setattr(app, "sleep", lambda seconds: None)
        monkeypatch.setattr(
            app.FTPHelper,
            "GetConnection",
            staticmethod(lambda: (_ for _ in ()).throw(OSError("down"))),
        )
        dead = FakeFTP()
        dead.noop_failures = 999
        with pytest.raises(ConnectionError):
            app.FTPHelper.EnsureConnection(dead)


class TestWorker:
    def test_unrecoverable_failure_signals_the_process(
        self, config: Dict[str, Any], jobs: queue.Queue, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        sys.exit() in a worker thread only unwinds that thread, so the pool
        silently shrank and the queue stopped draining.
        """
        monkeypatch.setattr(app, "fatal_error", threading.Event())
        monkeypatch.setattr(
            app.FTPHelper,
            "GetConnection",
            staticmethod(lambda: (_ for _ in ()).throw(OSError("refused"))),
        )
        app.FTPHelper.Worker()
        assert app.fatal_error.is_set()


class TestRmtree:
    def test_removes_files_then_directories_depth_first(
        self, config: Dict[str, Any]
    ) -> None:
        ftp = FakeFTP(
            {
                "/fastdl/css/maps": [file_line("a.bsp.bz2"), dir_line("sub")],
                "/fastdl/css/maps/sub": [file_line("b.bsp.bz2")],
            }
        )
        app.FTPHelper.rmtree(ftp, "/fastdl/css/maps")
        assert ftp.deleted == [
            "/fastdl/css/maps/a.bsp.bz2",
            "/fastdl/css/maps/sub/b.bsp.bz2",
        ]
        # Children before parents, or RMD fails on a non-empty directory.
        assert ftp.removed_dirs == ["/fastdl/css/maps/sub", "/fastdl/css/maps"]


class TestCompress:
    def test_uploads_to_a_temporary_name_then_renames(
        self, config: Dict[str, Any], tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        STOR wrote straight to the final name, so an interrupted transfer left
        a truncated .bz2 that clients would download and fail to decompress.
        """
        monkeypatch.setattr(app, "commonprefix_ftp", "/fastdl", raising=False)
        source = tmp_path / "a.bsp"
        source.write_bytes(b"map data")
        ftp = FakeFTP({"/fastdl/css/maps": []})

        app.AsyncFunc.Compress(
            ftp, (str(source), "/fastdl/css/maps/a.bsp.bz2", str(tmp_path))
        )

        assert len(ftp.stored) == 1
        assert ftp.stored[0].startswith("/fastdl/css/maps/a.bsp.bz2.part-")
        assert ftp.renamed == [(ftp.stored[0], "/fastdl/css/maps/a.bsp.bz2")]

    def test_failed_upload_keeps_the_previous_remote_file(
        self, config: Dict[str, Any], tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        The remote file was deleted *before* compressing the replacement, so a
        failure lost a working asset permanently.
        """
        monkeypatch.setattr(app, "commonprefix_ftp", "/fastdl", raising=False)
        source = tmp_path / "a.bsp"
        source.write_bytes(b"map data")

        class FailingUpload(FakeFTP):
            def storbinary(self, command: str, handle: Any) -> None:
                raise ftplib.error_temp("426 transfer aborted")

        ftp = FailingUpload({"/fastdl/css/maps": [file_line("a.bsp.bz2")]})
        app.AsyncFunc.Compress(
            ftp, (str(source), "/fastdl/css/maps/a.bsp.bz2", str(tmp_path))
        )

        # The live archive must survive, and only the partial is cleaned up.
        assert "/fastdl/css/maps/a.bsp.bz2" not in ftp.deleted
        assert ftp.renamed == []
        assert all(".part-" in path for path in ftp.deleted)

    def test_temporary_directory_is_cleaned_up(
        self, config: Dict[str, Any], tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(app, "commonprefix_ftp", "/fastdl", raising=False)
        source = tmp_path / "a.bsp"
        source.write_bytes(b"map data")
        staging = tmp_path / "staging"
        staging.mkdir()
        monkeypatch.setenv("TMPDIR", str(staging))

        ftp = FakeFTP({"/fastdl/css/maps": []})
        app.AsyncFunc.Compress(
            ftp, (str(source), "/fastdl/css/maps/a.bsp.bz2", str(tmp_path))
        )
        assert list(staging.iterdir()) == []


class TestDelete:
    def test_file_uses_dele(self, config: Dict[str, Any]) -> None:
        ftp = FakeFTP()
        app.AsyncFunc.Delete(ftp, ("/fastdl/css/maps/a.bsp.bz2", False))
        assert ftp.deleted == ["/fastdl/css/maps/a.bsp.bz2"]
        assert ftp.removed_dirs == []

    def test_directory_uses_rmd(self, config: Dict[str, Any]) -> None:
        # ftp.delete() fails with 550 on a directory, and the error was
        # swallowed, so remote trees were orphaned forever.
        ftp = FakeFTP({"/fastdl/css/maps": []})
        app.AsyncFunc.Delete(ftp, ("/fastdl/css/maps", True))
        assert ftp.removed_dirs == ["/fastdl/css/maps"]
