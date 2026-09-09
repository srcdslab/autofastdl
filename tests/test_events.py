"""
Filesystem event routing.

Each test here corresponds to a defect fixed during the audit; the "before"
behaviour is noted so a regression is recognisable.
"""

from __future__ import annotations

import queue
import threading
from typing import Any, Dict

import pytest
from conftest import drain, event, job_names

from autofastdl import autofastdl as app

SOURCE = "/srv/css/maps"
DEST = "/fastdl/css"


@pytest.fixture
def handler(
    config: Dict[str, Any], jobs: queue.Queue, monkeypatch: pytest.MonkeyPatch
) -> app.EventHandler:
    """An EventHandler with reconciliation stubbed out to a recorded call."""
    scheduled = []

    class StubReconciler:
        def schedule(self, source: str, destination: str) -> None:
            scheduled.append((source, destination))

    monkeypatch.setattr(app, "reconciler", StubReconciler(), raising=False)
    handler = app.EventHandler(SOURCE, DEST)
    handler.scheduled = scheduled  # type: ignore[attr-defined]
    return handler


class TestOnMoved:
    """
    The guard returned early whenever the *source* had a tracked extension,
    so no rename of a tracked file was mirrored, and the tracked -> untracked
    branch below it was unreachable.
    """

    def test_rename_between_tracked_extensions_is_mirrored(
        self, handler: app.EventHandler, jobs: queue.Queue
    ) -> None:
        handler.on_moved(event(f"{SOURCE}/a.bsp", f"{SOURCE}/b.bsp"))
        assert job_names(jobs) == ["Move"]

    def test_rename_into_tracked_extension_uploads(
        self, handler: app.EventHandler, jobs: queue.Queue
    ) -> None:
        handler.on_moved(event(f"{SOURCE}/a.tmp", f"{SOURCE}/b.bsp"))
        assert job_names(jobs) == ["Compress"]

    def test_rename_out_of_tracked_extension_deletes_remote(
        self, handler: app.EventHandler, jobs: queue.Queue
    ) -> None:
        handler.on_moved(event(f"{SOURCE}/a.bsp", f"{SOURCE}/b.tmp"))
        names = drain(jobs)
        assert [n for n, _ in names] == ["Delete"]
        # The *source* archive is what has to go, not the destination.
        assert names[0][1][0].endswith("a.bsp.bz2")

    def test_untracked_rename_is_ignored(
        self, handler: app.EventHandler, jobs: queue.Queue
    ) -> None:
        handler.on_moved(event(f"{SOURCE}/a.tmp", f"{SOURCE}/b.tmp"))
        assert job_names(jobs) == []

    def test_rename_to_ignored_name_is_skipped(
        self, handler: app.EventHandler, jobs: queue.Queue, config: Dict[str, Any]
    ) -> None:
        config["ignore_names"] = ["de_dust2.bsp"]
        handler.on_moved(event(f"{SOURCE}/a.bsp", f"{SOURCE}/de_dust2.bsp"))
        assert job_names(jobs) == []

    def test_directory_rename_is_mirrored(
        self, handler: app.EventHandler, jobs: queue.Queue
    ) -> None:
        handler.on_moved(event(f"{SOURCE}/old", f"{SOURCE}/new", is_directory=True))
        assert job_names(jobs) == ["Move"]


class TestCreateCloseDeduplication:
    """
    An ordinary write emits create *and* close, and both handlers enqueued a
    Compress, so every new asset was uploaded twice.
    """

    def test_create_then_close_uploads_once(
        self, handler: app.EventHandler, jobs: queue.Queue
    ) -> None:
        path = f"{SOURCE}/new.bsp"
        handler.on_created(event(path))
        handler.on_closed(event(path))
        assert job_names(jobs) == ["Compress"]

    def test_create_alone_is_deferred_not_dropped(
        self, handler: app.EventHandler, jobs: queue.Queue, config: Dict[str, Any]
    ) -> None:
        # Files moved in from outside the watch emit no close event; they are
        # the reason on_created exists at all.
        config["created_grace_seconds"] = 1
        handler.on_created(event(f"{SOURCE}/moved-in.bsp"))
        assert job_names(jobs) == []

        fired = threading.Event()
        deadline = 5.0
        while not fired.wait(0.05) and deadline > 0:
            if not jobs.empty():
                break
            deadline -= 0.05
        assert job_names(jobs) == ["Compress"]

    def test_close_cancels_the_pending_creation(
        self, handler: app.EventHandler
    ) -> None:
        path = f"{SOURCE}/new.bsp"
        handler.on_created(event(path))
        assert handler._pending_creations
        handler.on_closed(event(path))
        assert not handler._pending_creations

    def test_delete_cancels_the_pending_creation(
        self, handler: app.EventHandler
    ) -> None:
        path = f"{SOURCE}/new.bsp"
        handler.on_created(event(path))
        handler.on_deleted(event(path))
        assert not handler._pending_creations


class TestOnDeleted:
    def test_directory_deletion_is_queued(
        self, handler: app.EventHandler, jobs: queue.Queue
    ) -> None:
        # Was gated on os.path.exists() applied to a *remote* path, so it
        # essentially never fired.
        handler.on_deleted(event(f"{SOURCE}/sub", is_directory=True))
        jobs_seen = drain(jobs)
        assert [n for n, _ in jobs_seen] == ["Delete"]
        assert jobs_seen[0][1][1] is True  # is_directory flag

    def test_file_deletion_removes_the_archive(
        self, handler: app.EventHandler, jobs: queue.Queue
    ) -> None:
        handler.on_deleted(event(f"{SOURCE}/a.bsp"))
        jobs_seen = drain(jobs)
        assert jobs_seen[0][1][0].endswith("a.bsp.bz2")
        assert jobs_seen[0][1][1] is False

    def test_file_removed_after_upload_keeps_the_remote_copy(
        self,
        handler: app.EventHandler,
        jobs: queue.Queue,
        config: Dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        config["autoremove"] = {"after_upload": True}
        path = f"{SOURCE}/a.bsp"
        app.AutoRemove.files_removed_after_upload = [path]
        monkeypatch.setattr(
            app.AutoRemove, "WasFileRemovedAfterUpload", staticmethod(lambda p: True)
        )
        handler.on_deleted(event(path))
        assert job_names(jobs) == []


class TestBackpressure:
    def test_events_are_dropped_above_the_high_water_mark(
        self, handler: app.EventHandler, jobs: queue.Queue, config: Dict[str, Any]
    ) -> None:
        config["queue_high_water"] = 3
        for index in range(10):
            handler.on_closed(event(f"{SOURCE}/m{index}.bsp"))
        # Dropping is safe: reconciliation re-derives the work from disk.
        assert len(job_names(jobs)) == 3
        assert handler.scheduled  # type: ignore[attr-defined]


class TestTracking:
    def test_extension_match_requires_the_dot(
        self, handler: app.EventHandler, config: Dict[str, Any]
    ) -> None:
        # Bare suffixes made str.endswith match any name ending in the letters.
        assert handler.IsTracked(f"{SOURCE}/real.bsp")
        assert not handler.IsTracked(f"{SOURCE}/mymapbsp")
        assert not handler.IsTracked(f"{SOURCE}/foo.notbsp")

    def test_ignored_folders_match_components(
        self, handler: app.EventHandler, config: Dict[str, Any]
    ) -> None:
        config["ignore_folders"] = ["workshop"]
        assert not handler.IsTracked(f"{SOURCE}/workshop/a.bsp")
        # Previously excluded too, because the match was an unanchored substring.
        assert handler.IsTracked(f"{SOURCE}/workshop_backup/a.bsp")
        assert handler.IsTracked(f"{SOURCE}/de_workshop/a.bsp")
