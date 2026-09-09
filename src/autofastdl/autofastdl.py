from __future__ import annotations

import bz2
import datetime
import ftplib
import logging
import os
import pathlib
import queue
import random
import shutil
import string
import sys
import tempfile
import threading
import traceback
from time import monotonic, sleep
from types import TracebackType
from typing import Any, Dict, Generator, List, Optional, Tuple, Type

from dateutil import parser
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.api import BaseObserver

from autofastdl import config as configuration
from autofastdl.config import split_path

logger = logging.getLogger(__name__)

config: Dict[str, Any]
commonprefix_ftp: str
jobs: queue.Queue

# Set by any worker that hits an unrecoverable condition. The main loop watches
# this and terminates the process, because sys.exit() in a worker thread only
# unwinds that thread and would silently shrink the pool.
fatal_error = threading.Event()

EXIT_FAILURE = 84


def random_string(length: int) -> str:
    return "".join(random.choice(string.ascii_letters) for m in range(length))


def log_prefix(source_directory: str) -> str:
    """
    Prefix used to render paths in logs relative to the watched source root,
    matching how destination paths are derived (``<source>/..``).
    """
    return os.path.dirname(os.path.abspath(source_directory))


# Seconds of filesystem quiet before a full reconciliation pass is queued.
DEFAULT_RECONCILE_DEBOUNCE = 30
# Grace period before a created-but-never-closed file is treated as complete.
DEFAULT_CREATED_GRACE = 5
# Depth beyond which new event-driven work is dropped; the reconciler will
# pick it up on its next pass.
DEFAULT_QUEUE_HIGH_WATER = 10000


def path_is_ignored(pathname: str) -> bool:
    """
    True when any component of the path is an ignored folder.

    Matching used to be an unanchored substring test, so an "workshop" rule
    also excluded "workshop_backup" and any path merely containing the word.
    """
    ignore_folders = config["ignore_folders"]
    if not ignore_folders:
        return False
    parts = set(split_path(pathname))
    return any(folder in parts for folder in ignore_folders)


def config_int(name: str, default: int) -> int:
    value = config.get(name, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        logger.warning(f"Invalid {name}={value!r}, falling back to {default}")
        return default


def queue_has_headroom() -> bool:
    """
    Backpressure for the watchdog thread.

    The queue is intentionally unbounded: ``CheckAllFiles`` runs *inside* a
    worker and enqueues further jobs, so a hard ``maxsize`` would deadlock the
    pool as soon as every worker blocked on put(). Instead the producing side
    stops adding work past a high-water mark. Dropping an event is safe because
    reconciliation re-derives the same work from the filesystem afterwards.
    """
    high_water = config_int("queue_high_water", DEFAULT_QUEUE_HIGH_WATER)
    if jobs.qsize() < high_water:
        return True
    logger.warning(
        f"Job queue above high-water mark ({high_water}), deferring event to "
        "the next reconciliation pass"
    )
    return False


class Reconciler:
    """
    Coalesces full-tree reconciliation passes.

    Previously every filesystem event enqueued a ``CheckAllFiles`` job, each of
    which walks the whole source tree and enqueues further work -- so copying
    N files triggered N full walks. Requests are now debounced: a burst of
    events results in a single pass once the filesystem goes quiet.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pending: Dict[Tuple[str, str], float] = {}
        self._wakeup = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def schedule(self, source: str, destination: str) -> None:
        with self._lock:
            self._pending[(source, destination)] = monotonic()
        self._wakeup.set()

    def _due(self, debounce: float) -> List[Tuple[str, str]]:
        now = monotonic()
        with self._lock:
            due = [key for key, last in self._pending.items() if now - last >= debounce]
            for key in due:
                del self._pending[key]
            return due

    def _run(self) -> None:
        while not fatal_error.is_set():
            debounce = config_int(
                "reconcile_debounce_seconds", DEFAULT_RECONCILE_DEBOUNCE
            )
            self._wakeup.wait(timeout=1)
            self._wakeup.clear()

            for source, destination in self._due(debounce):
                # Preserve the ordering intent of fd66aba: let queued work
                # settle before walking the tree again, so reconciliation sees
                # the results of the uploads it triggered.
                jobs.join()
                logger.debug(f"Reconciliation pass queued for {source}")
                jobs.put((AsyncFunc.CheckAllFiles, source, destination))


reconciler: Reconciler


class FTPHelper:
    """
    This class is contain corresponding functions for traversing the FTP
    servers using BFS algorithm.
    """

    @staticmethod
    def GetConnection() -> ftplib.FTP:
        protocol = config["ftp_protocol"]

        if protocol == "ftps":
            # Explicit TLS: AUTH TLS on the control channel, then PROT P so the
            # data channel is encrypted too. Without prot_p() the credentials
            # would be protected but the file transfers would not.
            ftps = ftplib.FTP_TLS(config["ftp_host"])
            ftps.login(config["ftp_user"], config["ftp_password"])
            ftps.prot_p()
            return ftps

        if protocol != "ftp":
            raise ValueError(
                "Unsupported ftp_protocol {0!r}, expected one of: ftp, ftps".format(
                    protocol
                )
            )

        ftp = ftplib.FTP(config["ftp_host"])
        ftp.login(config["ftp_user"], config["ftp_password"])
        return ftp

    @staticmethod
    def listdir(ftp: ftplib.FTP, _path: str) -> Tuple[List[str], List[str]]:
        """
        return files and directory names within a path (directory)
        """

        file_list, dirs, nondirs = [], [], []
        try:
            ftp.cwd(_path)
        except Exception as exp:
            logger.warning(f"Cannot list remote directory {_path}: {exp}")
            return [], []
        else:
            ftp.retrlines("LIST", lambda x: file_list.append(x.split()))
            for info in file_list:
                ls_type, name = info[0], info[-1]
                if ls_type.startswith("d"):
                    dirs.append(name)
                else:
                    nondirs.append(name)
            return dirs, nondirs

    @staticmethod
    def walk(
        ftp: ftplib.FTP, path: str = "/"
    ) -> Generator[Tuple[str, List[str], List[str]], None, None]:
        """
        Walk through FTP server's directory tree, based on a BFS algorithm.
        """
        dirs, nondirs = FTPHelper.listdir(ftp, path)
        yield path, dirs, nondirs
        for name in dirs:
            path = os.path.join(path, name)
            yield from FTPHelper.walk(ftp, path)
            ftp.cwd("..")
            path = os.path.dirname(path)

    # Per-thread listing cache. Every worker owns its own FTP connection, so a
    # cache shared across threads would let one worker answer from a listing
    # another worker fetched for a different directory -- and this result decides
    # whether a file is uploaded or deleted.
    _cache = threading.local()

    # This is called a lot during startup.
    @staticmethod
    def file_exists(ftp: ftplib.FTP, path: str) -> bool:
        Exists = False
        cache = FTPHelper._cache
        try:
            # Cache should only be valid for one ftp connection
            if getattr(cache, "ftp", None) is not ftp:
                cache.ftp = ftp
                cache.path = None
                cache.resp = []

            directory = os.path.dirname(path)
            if cache.path != directory:
                resp: List[str] = []
                ftp.dir(directory, resp.append)
                cache.path = directory
                cache.resp = resp

            for line in cache.resp:
                if line[0] == "-":
                    line = line.split(maxsplit=8)[8]
                    if line == os.path.basename(path):
                        Exists = True
                        break
        except ftplib.all_errors:
            # Do not leave a half-populated listing behind for the next call.
            cache.path = None
            cache.resp = []
            return Exists

        return Exists

    @staticmethod
    def makedirs(ftp: ftplib.FTP, directory: str) -> None:
        """
        Create a remote directory tree, ignoring components that already exist.
        """
        create_tree = os.path.relpath(directory, commonprefix_ftp).split("/")
        create_tree.reverse()
        # First one will always be /cstrike or whatever...
        create_dir = os.path.abspath(os.path.join(commonprefix_ftp, create_tree.pop()))
        while create_tree:
            create_dir = os.path.abspath(os.path.join(create_dir, create_tree.pop()))
            try:
                ftp.mkd(create_dir)
            except ftplib.error_perm as e:
                # ignore "directory already exists"
                if not e.args[0].startswith("550"):
                    raise

    @staticmethod
    def rmtree(ftp: ftplib.FTP, path: str) -> None:
        """
        Recursively remove a remote directory. ``DELE`` only works on files, so
        the tree has to be emptied depth-first before ``RMD`` will succeed.
        """
        dirs, nondirs = FTPHelper.listdir(ftp, path)
        for name in nondirs:
            try:
                ftp.delete(path + "/" + name)
            except ftplib.all_errors as e:
                logger.warning(f"Could not delete remote file {path}/{name}: {e}")
        for name in dirs:
            FTPHelper.rmtree(ftp, path + "/" + name)
        try:
            ftp.rmd(path)
        except ftplib.all_errors as e:
            logger.warning(f"Could not remove remote directory {path}: {e}")

    @staticmethod
    def dir_exists(ftp: ftplib.FTP, path: str) -> bool:
        Exists = False
        try:
            resp: List[str] = []
            ftp.dir(os.path.abspath(os.path.join(path, "..")), resp.append)
            for line in resp:
                if line[0] == "d":
                    line = line.split(maxsplit=8)[8]
                    if line == os.path.basename(path):
                        Exists = True
                        break
        except ftplib.all_errors:
            return Exists

        return Exists

    @staticmethod
    def EnsureConnection(ftp: ftplib.FTP) -> ftplib.FTP:
        """
        Return a live connection, reconnecting if the current one is dead.

        Raises ConnectionError once the retry budget is exhausted.
        """
        sleep_seconds = 10
        retry_max = 10

        for retry in range(1, retry_max + 1):
            try:
                ftp.voidcmd("NOOP")
                return ftp
            except Exception as e:
                logger.warning("Failed sending a NOOP command ({0})".format(e))
                logger.info(
                    f"Trying to reconnect in {sleep_seconds} seconds [{retry}/{retry_max}]",
                )
                sleep(sleep_seconds)
                try:
                    ftp = FTPHelper.GetConnection()
                except Exception as reconnect_error:
                    logger.warning(f"Reconnection attempt failed: {reconnect_error}")

        raise ConnectionError(
            f"Could not reconnect to the FTP server after {retry_max} attempts"
        )

    @staticmethod
    def Worker() -> None:
        try:
            ftp = FTPHelper.GetConnection()
        except Exception as e:
            logger.error(f"Worker could not establish an FTP connection: {e}")
            fatal_error.set()
            return

        try:
            while not fatal_error.is_set():
                try:
                    job = jobs.get(timeout=1)
                except queue.Empty:
                    continue

                try:
                    ftp = FTPHelper.EnsureConnection(ftp)

                    logger.debug("Job: {0}({1})".format(job[0].__name__, job[1]))

                    job[0](ftp, job[1:])

                except ConnectionError as e:
                    # Unrecoverable: stop the whole process rather than quietly
                    # losing a worker and stalling the queue.
                    logger.error(f"{e}, exiting")
                    fatal_error.set()
                except Exception as e:
                    logger.error("worker error {0}".format(e))
                    logger.error(traceback.format_exc())
                finally:
                    jobs.task_done()
        finally:
            try:
                ftp.quit()
            except Exception:
                pass


class AutoRemove:
    configName = "autoremove"
    configLocal = "local"
    configFTP = "remote"
    configPriority = "priority"

    files_removed_after_upload_lock = threading.Lock()
    files_removed_after_upload: List[str] = []

    @staticmethod
    def HandleFileUploaded(filepath: str, commonprefix: str) -> None:
        if AutoRemove.IsFileRemovedAfterUpload():
            with AutoRemove.files_removed_after_upload_lock:
                AutoRemove.files_removed_after_upload += [os.path.abspath(filepath)]
                logger.info(
                    f"Local file {os.path.relpath(filepath, commonprefix)} deleted"
                )
                os.remove(filepath)

    @staticmethod
    def WasFileRemovedAfterUpload(filepath: str) -> bool:
        if AutoRemove.IsFileRemovedAfterUpload():
            with AutoRemove.files_removed_after_upload_lock:
                for filepath_removed in AutoRemove.files_removed_after_upload:
                    if filepath_removed == filepath:
                        AutoRemove.files_removed_after_upload.remove(filepath_removed)
                        return True
        return False

    @staticmethod
    def CheckAllFiles(ftp: ftplib.FTP, sourcedir: str, destdir: str) -> None:
        if not AutoRemove.IsAutoCleaned(
            AutoRemove.configFTP
        ) and not AutoRemove.IsAutoCleaned(AutoRemove.configLocal):
            return

        for dirpath, _dirnames, filenames in os.walk(sourcedir):
            if not path_is_ignored(dirpath):

                if AutoRemove.IsAutoCleaned(AutoRemove.configFTP):
                    destdirectory = os.path.join(
                        destdir, os.path.relpath(dirpath, os.path.join(sourcedir, ".."))
                    )
                    jobs.put((AsyncFunc.CheckDirFTP, destdirectory, "autoclean"))

                if AutoRemove.IsAutoCleaned(AutoRemove.configLocal):
                    logger.info(f"Local auto cleanup in {sourcedir} started")
                    filenames.sort()
                    for filename in [
                        f
                        for f in filenames
                        if f.endswith(config["extensions"])
                        and f not in config["ignore_names"]
                    ]:
                        sourcefile = os.path.join(dirpath, filename)
                        destfile = os.path.join(
                            destdir,
                            os.path.relpath(dirpath, os.path.join(sourcedir, "..")),
                            filename + ".bz2",
                        )
                        commonprefix = log_prefix(sourcedir)

                        if FTPHelper.file_exists(ftp, destfile):
                            jobs.put(
                                (
                                    AsyncFunc.CheckFile,
                                    sourcefile,
                                    destfile,
                                    commonprefix,
                                    commonprefix_ftp,
                                )
                            )
                        else:
                            jobs.put(
                                (
                                    AsyncFunc.CheckFileLocal,
                                    sourcefile,
                                    commonprefix,
                                    None,
                                )
                            )
                    logger.info(f"Local auto cleanup in {sourcedir} done")

    @staticmethod
    def CheckDirFTP(
        ftp: ftplib.FTP,
        sourcedir: str,
        threaded: bool = False,
        trigger: str = "startup_clean",
    ) -> None:
        # A remote pass runs at startup under `startup_clean`, and during
        # reconciliation under `autoclean`. Gating both on `startup_clean` made
        # `autoclean: true` alone silently do nothing.
        if trigger == "autoclean":
            enabled = AutoRemove.IsAutoCleaned(AutoRemove.configFTP)
        else:
            enabled = AutoRemove.IsStartupClean(AutoRemove.configFTP)

        if not enabled:
            return

        logger.info(f"Remote cleanup in {sourcedir} started")

        ftp_extensions: List[str] = []
        for ext in config["extensions"]:
            ftp_extensions.append(f"{ext}.bz2")

        ftp_ignore_names: List[str] = []
        for ign_name in config["ignore_names"]:
            ftp_ignore_names.append(f"{ign_name}.bz2")

        for dirpath, _dirnames, filenames in FTPHelper.walk(ftp, sourcedir):
            if not path_is_ignored(dirpath):
                filenames.sort()
                for filename in [
                    f
                    for f in filenames
                    if f.endswith(tuple(ftp_extensions)) and f not in ftp_ignore_names
                ]:
                    if threaded:
                        jobs.put(
                            (
                                AsyncFunc.CheckFileFTP,
                                dirpath + "/" + filename,
                                dirpath,
                                None,
                            )
                        )
                    else:
                        AutoRemove.CheckFileFTP(ftp, dirpath + "/" + filename, dirpath)

        logger.info(f"Cleanup in {sourcedir} done")

    @staticmethod
    def CheckFile(
        ftp: ftplib.FTP,
        sourcefile: str,
        destfile: str,
        sourcecommonprefix: str,
        destcommonprefix: str,
    ) -> None:
        if AutoRemove.configName in config:
            filedatetime = None
            if (
                AutoRemove.configLocal in config[AutoRemove.configName]
                and AutoRemove.configPriority in config[AutoRemove.configName]
            ):
                if (
                    AutoRemove.configLocal
                    == config[AutoRemove.configName][AutoRemove.configPriority]
                ):
                    filedatetime = AutoRemove.GetTimestampFile(sourcefile)
                elif (
                    AutoRemove.configFTP
                    == config[AutoRemove.configName][AutoRemove.configPriority]
                ):
                    filedatetime = AutoRemove.GetTimestampFTP(ftp, destfile)

            AutoRemove.CheckFileFTP(ftp, destfile, destcommonprefix, filedatetime)
            AutoRemove.CheckFileLocal(sourcefile, sourcecommonprefix, filedatetime)

    @staticmethod
    def GetTimestampFTP(ftp: ftplib.FTP, myfile: str) -> datetime.datetime:
        # MDTM is specified to return UTC, but yields a naive datetime. Tag it so
        # it is never compared against a local-time value.
        timestamp = ftp.sendcmd("MDTM " + myfile)[4:].strip()
        parsed = parser.parse(timestamp)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed

    @staticmethod
    def GetTimestampFile(myfile: str) -> datetime.datetime:
        fname = pathlib.Path(myfile)
        return datetime.datetime.fromtimestamp(
            fname.stat().st_mtime, tz=datetime.timezone.utc
        )

    @staticmethod
    def CheckFileFTP(
        ftp: ftplib.FTP,
        myfile: str,
        commonprefix: str,
        filedatetime: Optional[datetime.datetime] = None,
    ) -> bool:
        method = AutoRemove.configFTP

        if not isinstance(filedatetime, datetime.datetime):
            filedatetime = AutoRemove.GetTimestampFTP(ftp, myfile)

        if AutoRemove.IsOutdated(filedatetime, method):
            logger.info(
                f"{method.capitalize()} file {os.path.basename(myfile)} outdated ({str(filedatetime)})",
            )
            if AutoRemove.IsFileRemoved(method):
                ftp.delete(myfile)
                logger.info(
                    f"{method.capitalize()} file {os.path.basename(myfile)} deleted"
                )
                return False
            else:
                logger.info(
                    f"{method.capitalize()} file {os.path.basename(myfile)} kept"
                )
        return True

    @staticmethod
    def CheckFileLocal(
        myfile: str, commonprefix: str, filedatetime: Optional[datetime.datetime] = None
    ) -> bool:
        method = AutoRemove.configLocal

        if not isinstance(filedatetime, datetime.datetime):
            filedatetime = AutoRemove.GetTimestampFile(myfile)

        if AutoRemove.IsOutdated(filedatetime, method):
            logger.info(
                f"{method.capitalize()} file {os.path.basename(myfile)} outdated ({str(filedatetime)})",
            )
            if AutoRemove.IsFileRemoved(method):
                os.remove(myfile)
                logger.info(
                    f"{method.capitalize()} file {os.path.relpath(myfile, commonprefix)} deleted"
                )
                return False
            else:
                logger.info(
                    f"{method.capitalize()} file {os.path.relpath(myfile, commonprefix)} kept"
                )
        return True

    @staticmethod
    def IsOutdated(mydatetime: datetime.datetime, method: str) -> bool:
        if AutoRemove.configName in config and method in config[AutoRemove.configName]:
            checkTimeDelta = datetime.timedelta(minutes=0)
            if "days" in config[AutoRemove.configName][method]:
                checkTimeDelta += datetime.timedelta(
                    days=config[AutoRemove.configName][method]["days"]
                )
            if "minutes" in config[AutoRemove.configName][method]:
                checkTimeDelta += datetime.timedelta(
                    minutes=config[AutoRemove.configName][method]["minutes"]
                )
            if "seconds" in config[AutoRemove.configName][method]:
                checkTimeDelta += datetime.timedelta(
                    seconds=config[AutoRemove.configName][method]["seconds"]
                )

            currenttime = datetime.datetime.now(datetime.timezone.utc)

            if mydatetime.tzinfo is None:
                mydatetime = mydatetime.replace(tzinfo=datetime.timezone.utc)

            if (
                checkTimeDelta != datetime.timedelta(minutes=0)
                and mydatetime < currenttime - checkTimeDelta
            ):
                return True
        return False

    @staticmethod
    def IsFileRemoved(method: str) -> bool:
        return (
            AutoRemove.configName in config
            and method in config[AutoRemove.configName]
            and "remove" in config[AutoRemove.configName][method]
            and config[AutoRemove.configName][method]["remove"]
        )

    @staticmethod
    def IsFileRemovedAfterUpload() -> bool:
        return (
            AutoRemove.configName in config
            and "after_upload" in config[AutoRemove.configName]
            and config[AutoRemove.configName]["after_upload"]
        )

    @staticmethod
    def IsStartupClean(method: str) -> bool:
        return (
            AutoRemove.configName in config
            and method in config[AutoRemove.configName]
            and "startup_clean" in config[AutoRemove.configName][method]
            and config[AutoRemove.configName][method]["startup_clean"]
        )

    @staticmethod
    def IsAutoCleaned(method: str) -> bool:
        return (
            AutoRemove.configName in config
            and method in config[AutoRemove.configName]
            and "autoclean" in config[AutoRemove.configName][method]
            and config[AutoRemove.configName][method]["autoclean"]
        )


class AsyncFunc:
    @staticmethod
    def CheckFile(ftp: ftplib.FTP, item: Tuple[str, str, str, str]) -> None:
        sourcefile, destfile, sourcecommonprefix, destcommonprefix = item

        AutoRemove.CheckFile(
            ftp, sourcefile, destfile, sourcecommonprefix, destcommonprefix
        )

    @staticmethod
    def CheckFileFTP(ftp: ftplib.FTP, item: Tuple[str, str, datetime.datetime]) -> None:
        myfile, commonprefix, filedatetime = item

        AutoRemove.CheckFileFTP(ftp, myfile, commonprefix, filedatetime)

    @staticmethod
    def CheckFileLocal(
        ftp: ftplib.FTP, item: Tuple[str, str, datetime.datetime]
    ) -> None:
        myfile, commonprefix, filedatetime = item

        AutoRemove.CheckFileLocal(myfile, commonprefix, filedatetime)

    @staticmethod
    def CheckDirFTP(ftp: ftplib.FTP, item: Tuple[str, str]) -> None:
        sourcedir = item[0]
        trigger = item[1] if len(item) > 1 else "startup_clean"

        AutoRemove.CheckDirFTP(ftp, sourcedir, True, trigger)

    @staticmethod
    def CheckFileAdd(ftp: ftplib.FTP, item: Tuple[str, str, str]) -> None:
        sourcefile, commonprefix, destfile = item
        if AutoRemove.CheckFileLocal(sourcefile, commonprefix):
            logger.info(
                f"Local file {os.path.relpath(sourcefile, commonprefix)} added to queue"
            )
            jobs.put((AsyncFunc.Compress, sourcefile, destfile, commonprefix))

    @staticmethod
    def CheckAllFiles(ftp: ftplib.FTP, item: Tuple[str, str]) -> None:
        sourcedir, destdir = item

        AutoRemove.CheckAllFiles(ftp, sourcedir, destdir)

    @staticmethod
    def Compress(ftp: ftplib.FTP, item: Tuple[str, str, str]) -> None:
        sourcefile, destfile, commonprefix = item
        relative_source = os.path.relpath(sourcefile, commonprefix)

        # Check whether directory tree exists at destination, create it if necessary
        directory = os.path.dirname(destfile)
        if not FTPHelper.dir_exists(ftp, directory):
            FTPHelper.makedirs(ftp, directory)

        # Upload under a temporary name and rename into place, so that an
        # interrupted transfer can never leave a truncated archive that clients
        # would download, and so a failure keeps the previous file intact.
        uploadfile = destfile + ".part-" + random_string(8)

        with tempfile.TemporaryDirectory(prefix="fastDL_sync_") as folder:
            localtemp = os.path.join(folder, os.path.basename(destfile))

            with open(sourcefile, "rb") as infile:
                with bz2.BZ2File(localtemp, "wb", compresslevel=9) as outfile:
                    shutil.copyfileobj(infile, outfile, 64 * 1024)

            try:
                with open(localtemp, "rb") as temp:
                    ftp.storbinary("STOR {0}".format(uploadfile), temp)

                # Replace the previous archive only now that the new one is
                # fully transferred.
                if FTPHelper.file_exists(ftp, destfile):
                    ftp.delete(destfile)
                ftp.rename(uploadfile, destfile)

                logger.info(f"Local file {relative_source} uploaded to remote")
                AutoRemove.HandleFileUploaded(sourcefile, commonprefix)
            except Exception:
                logger.error(f"Unexpected error:\n{str(sys.exc_info())}")
                logger.warning(
                    f"Local file {relative_source} failed to upload to remote (Skipping)"
                )
                try:
                    ftp.delete(uploadfile)
                except ftplib.all_errors:
                    pass

    @staticmethod
    def Delete(ftp: ftplib.FTP, item: Tuple[str, bool]) -> None:
        path = item[0]
        is_directory = bool(item[1]) if len(item) > 1 else False

        try:
            if is_directory:
                FTPHelper.rmtree(ftp, path)
                logger.info(
                    f"Remote directory {os.path.relpath(path, commonprefix_ftp)} deleted"
                )
            else:
                ftp.delete(path)
                logger.info(
                    f"Remote file {os.path.relpath(path, commonprefix_ftp)} deleted"
                )
        except ftplib.error_perm:
            pass

    @staticmethod
    def Move(ftp: ftplib.FTP, item: Tuple[str, str]) -> None:
        sourcepath, destpath = item

        # Check whether directory tree exists at destination, create it if necessary
        directory = os.path.dirname(destpath)
        if not FTPHelper.dir_exists(ftp, directory):
            FTPHelper.makedirs(ftp, directory)

        ftp.rename(sourcepath, destpath)

        logger.info(
            "Remote file moved {0} -> {1}".format(
                os.path.relpath(sourcepath, commonprefix_ftp),
                os.path.relpath(destpath, commonprefix_ftp),
            ),
        )


class EventHandler(FileSystemEventHandler):
    def __init__(self, source: str, destination: str) -> None:
        super().__init__()
        self.SourceDirectory = os.path.abspath(source)
        self.DestinationDirectory = os.path.abspath(destination)
        self.LogPrefix = log_prefix(self.SourceDirectory)
        self._pending_creations: Dict[str, threading.Timer] = {}
        self._pending_lock = threading.Lock()

    def IsTracked(self, pathname: str) -> bool:
        return (
            pathname.endswith(config["extensions"])
            and os.path.basename(pathname) not in config["ignore_names"]
            and not path_is_ignored(pathname)
        )

    def RemotePath(self, pathname: str) -> str:
        return os.path.join(
            self.DestinationDirectory,
            os.path.relpath(pathname, os.path.join(self.SourceDirectory, "..")),
        )

    def Reconcile(self) -> None:
        reconciler.schedule(self.SourceDirectory, self.DestinationDirectory)

    def CancelPendingCreation(self, pathname: str) -> bool:
        """Drop a deferred creation. Returns True if one was pending."""
        with self._pending_lock:
            timer = self._pending_creations.pop(pathname, None)
        if timer is None:
            return False
        timer.cancel()
        return True

    def QueueCompress(self, pathname: str) -> None:
        if not queue_has_headroom():
            return
        jobs.put(
            (
                AsyncFunc.Compress,
                pathname,
                self.RemotePath(pathname) + ".bz2",
                self.LogPrefix,
            )
        )

    def on_closed(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        pathname = os.fsdecode(event.src_path)
        logger.debug(f"on_closed: {pathname}")
        if not self.IsTracked(pathname):
            return

        # A normal write emits create *and* close. Cancel the deferred creation
        # so the file is compressed and uploaded exactly once.
        self.CancelPendingCreation(pathname)

        self.QueueCompress(pathname)
        self.Reconcile()

    def on_created(self, event: FileSystemEvent) -> None:
        # Handle files moved into the watched directory from an untracked location.
        # Such moves do not generate a close event, so we handle them here.
        if event.is_directory:
            return
        pathname = os.fsdecode(event.src_path)
        logger.debug(f"on_created: {pathname}")
        if not self.IsTracked(pathname):
            return

        # Defer: if a close event follows (an ordinary write still in progress)
        # it cancels this and handles the upload itself. Only files that are
        # never closed -- moved in from outside the watch -- land here.
        grace = config_int("created_grace_seconds", DEFAULT_CREATED_GRACE)

        def fire() -> None:
            with self._pending_lock:
                self._pending_creations.pop(pathname, None)
            logger.debug(f"No close event for {pathname}, treating as complete")
            self.QueueCompress(pathname)
            self.Reconcile()

        timer = threading.Timer(grace, fire)
        timer.daemon = True
        with self._pending_lock:
            existing = self._pending_creations.pop(pathname, None)
            self._pending_creations[pathname] = timer
        if existing is not None:
            existing.cancel()
        timer.start()

    def on_deleted(self, event: FileSystemEvent) -> None:
        pathname = os.fsdecode(event.src_path)
        logger.debug(f"on_deleted: {pathname}")
        destpath = self.RemotePath(pathname)
        self.CancelPendingCreation(pathname)
        if event.is_directory:
            # destpath lives on the FTP server; whether it exists is decided by
            # the worker holding the connection, not by the local filesystem.
            jobs.put((AsyncFunc.Delete, destpath, True))
        else:
            if not self.IsTracked(pathname):
                return

            if not AutoRemove.WasFileRemovedAfterUpload(pathname):
                jobs.put((AsyncFunc.Delete, destpath + ".bz2", False))
            else:
                logger.info(f"Keeping remote file {destpath}.bz2")
        self.Reconcile()

    def on_moved(self, event: FileSystemEvent) -> None:
        src_path = os.fsdecode(event.src_path)
        dest_path = os.fsdecode(event.dest_path)
        logger.debug(f"on_moved: {src_path} -> {dest_path}")
        # Moved inside tracked directory, handle as rename
        self.CancelPendingCreation(src_path)
        sourcepath = self.RemotePath(src_path)
        destpath = self.RemotePath(dest_path)

        if event.is_directory:
            jobs.put((AsyncFunc.Move, sourcepath, destpath))
        else:
            source_tracked = src_path.endswith(config["extensions"])
            dest_tracked = dest_path.endswith(config["extensions"])

            # Nothing to mirror if neither side is a tracked asset, or if the
            # destination is excluded by configuration.
            if (
                not source_tracked
                and not dest_tracked
                or os.path.basename(dest_path) in config["ignore_names"]
                or path_is_ignored(dest_path)
            ):
                return

            if not source_tracked and dest_tracked:
                # Renamed invalid_ext file to valid one -> compress
                jobs.put(
                    (
                        AsyncFunc.Compress,
                        dest_path,
                        destpath + ".bz2",
                        self.LogPrefix,
                    )
                )
                return

            elif source_tracked and not dest_tracked:
                # Renamed valid_ext file to invalid one -> delete from destination
                jobs.put((AsyncFunc.Delete, sourcepath + ".bz2", False))
                return

            jobs.put((AsyncFunc.Move, sourcepath + ".bz2", destpath + ".bz2"))
        self.Reconcile()


class DirectoryHandler:
    def __init__(
        self,
        source: str,
        destination: str,
        observer: Optional[BaseObserver] = None,
    ):
        self.SourceDirectory = os.path.abspath(source)
        self.DestinationDirectory = destination

        if observer:
            self.Observer = observer
            self.NotifyHandler = EventHandler(
                source=self.SourceDirectory, destination=self.DestinationDirectory
            )
            self.NotifyWatch = observer.schedule(
                self.NotifyHandler, self.SourceDirectory, recursive=True
            )

    def __enter__(self) -> DirectoryHandler:
        return self

    def __exit__(
        self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.Observer.unschedule(self.NotifyWatch)

    def Do(self, ftp: ftplib.FTP) -> None:  # Normal mode
        for dirpath, _dirnames, filenames in os.walk(self.SourceDirectory):
            if not path_is_ignored(dirpath):

                destdirectory = os.path.join(
                    self.DestinationDirectory,
                    os.path.relpath(dirpath, os.path.join(self.SourceDirectory, "..")),
                )
                jobs.put((AsyncFunc.CheckDirFTP, destdirectory, "startup_clean"))

                filenames.sort()
                for filename in [
                    f
                    for f in filenames
                    if f.endswith(config["extensions"])
                    and f not in config["ignore_names"]
                ]:
                    self.Checkfile(ftp, dirpath, filename)

    def Checkfile(self, ftp: ftplib.FTP, dirpath: str, filename: str) -> None:
        sourcefile = os.path.join(dirpath, filename)
        destfile = os.path.join(
            self.DestinationDirectory,
            os.path.relpath(dirpath, os.path.join(self.SourceDirectory, "..")),
            filename + ".bz2",
        )
        commonprefix = log_prefix(self.SourceDirectory)

        if FTPHelper.file_exists(ftp, destfile):
            logger.debug(
                f"Local file {os.path.relpath(sourcefile, commonprefix)} exists"
            )
            jobs.put(
                (
                    AsyncFunc.CheckFile,
                    sourcefile,
                    destfile,
                    commonprefix,
                    commonprefix_ftp,
                )
            )
        else:
            jobs.put((AsyncFunc.CheckFileAdd, sourcefile, commonprefix, destfile))


def setup_logging(debug: bool, docker: bool) -> None:
    """
    Configure the root logger.

    Under `docker`, timestamps are omitted: the container runtime already
    stamps every line it collects, so emitting our own duplicates them in
    `docker logs` and in anything downstream of it.
    """
    if docker:
        logging.basicConfig(
            level=logging.DEBUG if debug else logging.INFO,
            format="%(levelname)s | %(message)s",
            stream=sys.stdout,
        )
        return

    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


def main() -> None:
    global config

    try:
        path = configuration.config_path(sys.argv[1:])
        config = configuration.load(path)
    except configuration.ConfigError as e:
        # Logging is not configured yet, and the message must not be swallowed.
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(EXIT_FAILURE)

    # Configure logging before anything that can fail, so startup errors are
    # emitted through the configured handler instead of the fallback one.
    setup_logging(config["debug"], config["docker"])

    logger.info("AutoFastDL started")
    logger.debug(f"Configuration loaded from {path}")

    for source in configuration.missing_sources(config):
        logger.warning(f"Source directory does not exist yet: {source}")

    if config["ftp_protocol"] == "ftp":
        logger.warning(
            "Using plaintext FTP: credentials and file contents are sent "
            "unencrypted. Set ftp_protocol to 'ftps' if the server supports it."
        )

    try:
        ftp = FTPHelper.GetConnection()
    except Exception as e:
        logger.error(f"Could not connect to the FTP server: {e}")
        sys.exit(EXIT_FAILURE)

    # make common prefix for better logging
    global commonprefix_ftp
    commonprefix_ftp = os.path.dirname(config["ftp_path"])

    global jobs
    jobs = queue.Queue()

    global reconciler
    reconciler = Reconciler()

    # Create initial jobs
    observer = Observer()
    DirectoryHandlers = []
    for source in config["sources"]:
        handler = DirectoryHandler(source, config["ftp_path"], observer)
        DirectoryHandlers.append(handler)
        handler.Do(ftp)

    ftp.quit()

    # Start worker threads
    for _i in range(config["threads"]):
        worker_thread = threading.Thread(target=FTPHelper.Worker)
        worker_thread.daemon = True
        worker_thread.start()

    # Coalesces reconciliation passes instead of one per filesystem event
    reconciler.start()

    # filesystem event loop
    observer.start()
    interrupted = False
    try:
        # fatal_error is set by a worker that exhausted its FTP retry budget.
        while not fatal_error.wait(timeout=1):
            pass
    except KeyboardInterrupt:
        interrupted = True

    observer.stop()
    observer.join()

    if interrupted:
        logger.info("Waiting for remaining jobs to complete...")
        jobs.join()

    if fatal_error.is_set():
        logger.error("AutoFastDL exiting after an unrecoverable error")
        sys.exit(EXIT_FAILURE)

    logger.info("AutoFastDL exiting")


if __name__ == "__main__":
    main()
