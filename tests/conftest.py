"""
Shared fixtures.

The application module keeps its state in module-level globals (``config``,
``jobs``, ``reconciler``), so tests install them explicitly rather than going
through ``main()``. Nothing here touches the network or a real FTP server.
"""

from __future__ import annotations

import queue
import types
from typing import Any, Dict, List, Optional, Tuple

import pytest

from autofastdl import autofastdl as app


def make_config(**overrides: Any) -> Dict[str, Any]:
    config: Dict[str, Any] = {
        "threads": 1,
        "debug": False,
        "docker": False,
        "extensions": (".bsp", ".nav"),
        "ignore_names": [],
        "ignore_folders": [],
        "sources": ["/srv/css/maps"],
        "ftp_protocol": "ftp",
        "ftp_host": "127.0.0.1",
        "ftp_path": "/fastdl/css",
        "ftp_user": "user",
        "ftp_password": "pass",
        "reconcile_debounce_seconds": 30,
        "created_grace_seconds": 5,
        "queue_high_water": 10000,
    }
    config.update(overrides)
    return config


@pytest.fixture
def config(monkeypatch: pytest.MonkeyPatch) -> Dict[str, Any]:
    """Install a default application config for the duration of a test."""
    values = make_config()
    monkeypatch.setattr(app, "config", values, raising=False)
    # Set by main() in production; needed by anything that logs a remote path.
    monkeypatch.setattr(app, "commonprefix_ftp", "/fastdl", raising=False)
    return values


@pytest.fixture
def jobs(monkeypatch: pytest.MonkeyPatch) -> queue.Queue:
    """Install a fresh job queue and return it."""
    q: queue.Queue = queue.Queue()
    monkeypatch.setattr(app, "jobs", q, raising=False)
    return q


def drain(q: queue.Queue) -> List[Tuple[str, tuple]]:
    """Return queued jobs as (function name, arguments) pairs."""
    out = []
    while not q.empty():
        job = q.get()
        q.task_done()
        out.append((job[0].__name__, job[1:]))
    return out


def job_names(q: queue.Queue) -> List[str]:
    return [name for name, _ in drain(q)]


def event(
    src: str, dest: Optional[str] = None, is_directory: bool = False
) -> types.SimpleNamespace:
    """A stand-in for a watchdog filesystem event."""
    return types.SimpleNamespace(
        src_path=src, dest_path=dest, is_directory=is_directory
    )


class FakeFTP:
    """
    Minimal ftplib.FTP stand-in.

    ``listing`` maps a directory to the raw LIST lines the server would return,
    in the same format the real code parses.
    """

    def __init__(self, listing: Optional[Dict[str, List[str]]] = None) -> None:
        self.listing = listing or {}
        self.deleted: List[str] = []
        self.removed_dirs: List[str] = []
        self.made_dirs: List[str] = []
        self.renamed: List[Tuple[str, str]] = []
        self.stored: List[str] = []
        self.cwd_calls: List[str] = []
        self.noop_failures = 0
        self.mdtm: Dict[str, str] = {}

    # -- connection ------------------------------------------------------
    def voidcmd(self, command: str) -> str:
        if self.noop_failures > 0:
            self.noop_failures -= 1
            raise OSError("connection closed")
        return "200 OK"

    def sendcmd(self, command: str) -> str:
        if command.startswith("MDTM "):
            return "213 " + self.mdtm[command[5:]]
        raise AssertionError(f"unexpected command {command!r}")

    def quit(self) -> None:
        pass

    # -- listing ---------------------------------------------------------
    def dir(self, path: str, callback: Any) -> None:
        for line in self.listing.get(path, []):
            callback(line)

    def cwd(self, path: str) -> None:
        self.cwd_calls.append(path)
        if path != ".." and path not in self.listing:
            raise OSError(f"550 no such directory: {path}")

    def retrlines(self, command: str, callback: Any) -> None:
        path = self.cwd_calls[-1]
        for line in self.listing.get(path, []):
            callback(line)

    # -- mutation --------------------------------------------------------
    def delete(self, path: str) -> None:
        self.deleted.append(path)

    def rmd(self, path: str) -> None:
        self.removed_dirs.append(path)

    def mkd(self, path: str) -> None:
        self.made_dirs.append(path)

    def rename(self, source: str, dest: str) -> None:
        self.renamed.append((source, dest))

    def storbinary(self, command: str, handle: Any) -> None:
        handle.read()
        self.stored.append(command.split(" ", 1)[1])


def file_line(name: str) -> str:
    return f"-rw-r--r-- 1 ftp ftp 1024 Jan 01 00:00 {name}"


def dir_line(name: str) -> str:
    return f"drwxr-xr-x 2 ftp ftp 4096 Jan 01 00:00 {name}"
