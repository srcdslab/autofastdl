from __future__ import annotations

import bz2
import datetime
import ftplib
import json
import logging
import os
import pathlib
import queue
import random
import shutil
import string
import sys
import threading
import traceback
from pyclbr import Function
from time import sleep
from types import TracebackType
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Type

import pyinotify
from dateutil import parser

logger = logging.getLogger(__name__)

config: Dict[str, Any]
commonprefix_ftp: str
jobs: queue.Queue

# inotify mask
NOTIFY_MASK = (
    pyinotify.IN_CLOSE_WRITE
    | pyinotify.IN_DELETE
    | pyinotify.IN_MOVED_TO
    | pyinotify.IN_MOVED_FROM
)


def random_string(length: int) -> str:
    return "".join(random.choice(string.ascii_letters) for m in range(length))


def static_var(varname: str, value: Optional[Any] = None) -> Callable:
    def decorate(func: Function) -> Function:
        setattr(func, varname, value)
        return func

    return decorate


class FTPHelper:
    """
    This class is contain corresponding functions for traversing the FTP
    servers using BFS algorithm.
    """

    @staticmethod
    def GetConnection() -> ftplib.FTP:
        if not config["ftp_protocol"] == "ftp":
            logger.error("FTP protocol not supported!")
            sys.exit(84)

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
            print("the current path is : ", ftp.pwd(), exp.__str__(), _path)
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

    # This is called a lot during startup.
    @static_var("cache_path")
    @static_var("cache_resp")
    @static_var("cache_ftp")
    @staticmethod
    def file_exists(ftp: ftplib.FTP, path: str) -> bool:
        Exists = False
        try:
            # Cache should only be valid for one ftp connection
            if FTPHelper.file_exists.cache_ftp != ftp:
                FTPHelper.file_exists.cache_ftp = ftp
                FTPHelper.file_exists.cache_path = None
                FTPHelper.file_exists.cache_resp = None

            if FTPHelper.file_exists.cache_path != os.path.dirname(path):
                FTPHelper.file_exists.cache_path = os.path.dirname(path)
                FTPHelper.file_exists.cache_resp = []
                ftp.dir(os.path.dirname(path), FTPHelper.file_exists.cache_resp.append)

            for line in FTPHelper.file_exists.cache_resp:
                if line[0] == "-":
                    line = line.split(maxsplit=8)[8]
                    if line == os.path.basename(path):
                        Exists = True
                        break
        except ftplib.all_errors:
            return Exists

        return Exists

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
    def Worker() -> None:
        ftp = FTPHelper.GetConnection()

        while True:
            job = jobs.get()

            try:
                retry = 0
                sleep_seconds = 10
                retry_max = 10
                while retry < retry_max:
                    try:
                        ftp.voidcmd("NOOP")
                        break
                    except Exception as e:
                        logger.warning("Failed sending a NOOP command ({0})".format(e))
                        logger.info(
                            f"Trying to reconnect in {sleep_seconds} seconds [{retry}/{retry_max}]",
                        )
                        sleep(sleep_seconds)
                        try:
                            ftp = FTPHelper.GetConnection()
                        except Exception:
                            pass

                    retry += 1

                if retry > retry_max:
                    logger.error(
                        "Failed too many times trying to reconnect to the FTP, exiting",
                    )
                    sys.exit(84)

                logger.debug("Job: {0}({1})".format(job[0].__name__, job[1]))

                job[0](ftp, job[1:])

            except Exception as e:
                logger.error("worker error {0}".format(e))
                logger.error(traceback.format_exc())

            jobs.task_done()

        ftp.quit()


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
            if not any(folder in dirpath for folder in config["ignore_folders"]):

                if AutoRemove.IsAutoCleaned(AutoRemove.configFTP):
                    destdirectory = os.path.join(
                        destdir, os.path.relpath(dirpath, os.path.join(sourcedir, ".."))
                    )
                    jobs.put((AsyncFunc.CheckDirFTP, destdirectory))

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
                        commonprefix = os.path.abspath(
                            os.path.join(
                                os.path.dirname(os.path.commonprefix(sourcefile)), ".."
                            )
                        )

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
    def CheckDirFTP(ftp: ftplib.FTP, sourcedir: str, threaded: bool = False) -> None:
        if not AutoRemove.IsStartupClean(AutoRemove.configFTP):
            return

        logger.info(f"Remote cleanup in {sourcedir} started")

        ftp_extensions: List[str] = []
        for ext in config["extensions"]:
            ftp_extensions.append(f"{ext}.bz2")

        ftp_ignore_names: List[str] = []
        for ign_name in config["ignore_names"]:
            ftp_ignore_names.append(f"{ign_name}.bz2")

        for dirpath, _dirnames, filenames in FTPHelper.walk(ftp, sourcedir):
            if not any(folder in dirpath for folder in config["ignore_folders"]):
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
        timestamp = ftp.sendcmd("MDTM " + myfile)[4:].strip()
        return parser.parse(timestamp)

    @staticmethod
    def GetTimestampFile(myfile: str) -> datetime.datetime:
        fname = pathlib.Path(myfile)
        return datetime.datetime.fromtimestamp(fname.stat().st_mtime)

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

            currenttime = datetime.datetime.now()

            if (
                checkTimeDelta != datetime.timedelta(minutes=0)
                and mydatetime < currenttime - checkTimeDelta
            ):
                return True
        return False

    @staticmethod
    def IsConfigGood() -> bool:
        return AutoRemove.configName in config

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

    @staticmethod
    def GetTimezone(method: str, name: str) -> bool:
        return (
            AutoRemove.configName in config
            and method in config[AutoRemove.configName]
            and name in config[AutoRemove.configName][method]
            and config[AutoRemove.configName][method][name]
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

        AutoRemove.CheckDirFTP(ftp, sourcedir, True)

    @staticmethod
    def CheckFileAdd(ftp: ftplib.FTP, item: Tuple[str, str, str]) -> None:
        sourcefile, commonprefix, destfile = item
        if AutoRemove.CheckFileLocal(sourcefile, commonprefix):
            logger.info(
                f"Local file {os.path.relpath(sourcefile, commonprefix)} added to queue"
            )
            jobs.put((AsyncFunc.Compress, sourcefile, destfile))

    @staticmethod
    def CheckAllFiles(ftp: ftplib.FTP, item: Tuple[str, str]) -> None:
        sourcedir, destdir = item

        AutoRemove.CheckAllFiles(ftp, sourcedir, destdir)

    @staticmethod
    def Compress(ftp: ftplib.FTP, item: Tuple[str, str]) -> None:
        sourcefile, destfile = item
        # Remove destination file if already exists
        if FTPHelper.file_exists(ftp, destfile):
            ftp.delete(destfile)

        # Check whether directory tree exists at destination, create it if necessary
        directory = os.path.dirname(destfile)
        if not FTPHelper.dir_exists(ftp, directory):
            create_tree = os.path.relpath(directory, commonprefix_ftp).split("/")
            create_tree.reverse()
            # First one will always be /cstrike or whatever...
            create_dir = os.path.abspath(
                os.path.join(commonprefix_ftp, create_tree.pop())
            )
            while create_tree:
                create_dir = os.path.abspath(
                    os.path.join(create_dir, create_tree.pop())
                )
                try:
                    ftp.mkd(create_dir)
                except ftplib.error_perm as e:
                    # ignore "directory already exists"
                    if not e.args[0].startswith("550"):
                        raise

        folder = "/tmp/fastDL_sync_" + random_string(10)

        os.mkdir(folder)

        tempfile = os.path.join(folder, os.path.basename(destfile))

        with open(sourcefile, "rb") as infile:
            with bz2.BZ2File(tempfile, "wb", compresslevel=9) as outfile:
                shutil.copyfileobj(infile, outfile, 64 * 1024)

        commonprefix = os.path.abspath(
            os.path.join(os.path.dirname(os.path.commonprefix(sourcefile)), "..")
        )

        try:
            with open(tempfile, "rb") as temp:
                ftp.storbinary("STOR {0}".format(destfile), temp)

            logger.info(
                f"Local file {os.path.relpath(sourcefile, commonprefix)} uploaded to remote"
            )
            AutoRemove.HandleFileUploaded(sourcefile, commonprefix)
        except Exception:
            logger.error(f"Unexpected error:\n{str(sys.exc_info())}")
            logger.warn(
                f"Local file {os.path.relpath(sourcefile, commonprefix)} failed to upload to remote (Skipping)"
            )

        os.remove(tempfile)

        os.rmdir(folder)

    @staticmethod
    def Delete(ftp: ftplib.FTP, item: Tuple[str, str]) -> None:
        path = item[0]

        try:
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
            ftp.mkd(directory)

        ftp.rename(sourcepath, destpath)

        logger.info(
            "Remote file moved {0} -> {1}".format(
                os.path.relpath(sourcepath, commonprefix_ftp),
                os.path.relpath(destpath, commonprefix_ftp),
            ),
        )


class EventHandler(pyinotify.ProcessEvent):
    def my_init(self, source: str, destination: str) -> None:
        self.SourceDirectory = os.path.abspath(source)
        self.DestinationDirectory = os.path.abspath(destination)

    def process_IN_CLOSE_WRITE(self, event: pyinotify.ProcessEvent) -> None:
        logger.debug(f"process_IN_CLOSE_WRITE: {str(event)}")
        if (
            not event.pathname.endswith(config["extensions"])
            or os.path.basename(event.pathname) in config["ignore_names"]
            or any(folder in event.pathname for folder in config["ignore_folders"])
        ):
            return

        destpath = os.path.join(
            self.DestinationDirectory,
            os.path.relpath(event.pathname, os.path.join(self.SourceDirectory, "..")),
        )
        jobs.put((AsyncFunc.Compress, event.pathname, destpath + ".bz2"))

    def process_IN_DELETE(self, event: pyinotify.ProcessEvent) -> None:
        logger.debug(f"process_IN_DELETE: {str(event)}")
        destpath = os.path.join(
            self.DestinationDirectory,
            os.path.relpath(event.pathname, os.path.join(self.SourceDirectory, "..")),
        )
        if event.dir:
            if os.path.exists(destpath):
                jobs.put((AsyncFunc.Delete, destpath))
        else:
            if (
                not event.pathname.endswith(config["extensions"])
                or os.path.basename(event.pathname) in config["ignore_names"]
                or any(folder in event.pathname for folder in config["ignore_folders"])
            ):
                return

            if not AutoRemove.WasFileRemovedAfterUpload(event.pathname):
                jobs.put((AsyncFunc.Delete, destpath + ".bz2"))
            else:
                logger.info(f"Keeping remote file {destpath}.bz2")

    def process_IN_MOVED_TO(self, event: pyinotify.ProcessEvent) -> None:
        logger.debug(f"process_IN_MOVED_TO: {str(event)}")
        # Moved from untracked directory, handle as new file
        if not hasattr(event, "src_pathname"):
            if (
                not event.pathname.endswith(config["extensions"])
                or os.path.basename(event.pathname) in config["ignore_names"]
                or any(folder in event.pathname for folder in config["ignore_folders"])
            ):
                return

            destpath = os.path.join(
                self.DestinationDirectory,
                os.path.relpath(
                    event.pathname, os.path.join(self.SourceDirectory, "..")
                ),
            )
            jobs.put((AsyncFunc.Compress, event.pathname, destpath + ".bz2"))
            return

        # Moved inside tracked directory, handle as rename
        sourcepath = os.path.join(
            self.DestinationDirectory,
            os.path.relpath(
                event.src_pathname, os.path.join(self.SourceDirectory, "..")
            ),
        )
        destpath = os.path.join(
            self.DestinationDirectory,
            os.path.relpath(event.pathname, os.path.join(self.SourceDirectory, "..")),
        )

        if event.dir:
            jobs.put((AsyncFunc.Move, sourcepath, destpath))
        else:
            if (
                event.src_pathname.endswith(config["extensions"])
                or os.path.basename(event.pathname) in config["ignore_names"]
                or any(folder in event.pathname for folder in config["ignore_folders"])
            ):
                return

            if not event.src_pathname.endswith(
                config["extensions"]
            ) and event.pathname.endswith(config["extensions"]):
                # Renamed invalid_ext file to valid one -> compress
                jobs.put((AsyncFunc.Compress, event.pathname, destpath + ".bz2"))
                return

            elif event.src_pathname.endswith(
                config["extensions"]
            ) and not event.pathname.endswith(config["extensions"]):
                # Renamed valid_ext file to invalid one -> delete from destination
                jobs.put((AsyncFunc.Delete, sourcepath + ".bz2"))
                return

            jobs.put((AsyncFunc.Move, sourcepath + ".bz2", destpath + ".bz2"))


class DirectoryHandler:
    def __init__(
        self,
        source: str,
        destination: str,
        watchmanager: Optional[pyinotify.WatchManager] = None,
    ):
        self.SourceDirectory = os.path.abspath(source)
        self.DestinationDirectory = destination

        if watchmanager:
            self.WatchManager = watchmanager
            self.NotifyHandler = EventHandler(
                source=self.SourceDirectory, destination=self.DestinationDirectory
            )
            self.NotifyNotifier = pyinotify.Notifier(
                self.WatchManager, self.NotifyHandler, timeout=1000
            )
            self.NotifyWatch = self.WatchManager.add_watch(
                self.SourceDirectory, NOTIFY_MASK, rec=True, auto_add=True
            )

    def __enter__(self) -> DirectoryHandler:
        return self

    def __exit__(
        self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.WatchManager.rm_watch(self.NotifyWatch, rec=True)

    def Loop(self) -> None:
        self.NotifyNotifier.process_events()
        while self.NotifyNotifier.check_events():
            self.NotifyNotifier.read_events()
            self.NotifyNotifier.process_events()
            jobs.put(
                (
                    AsyncFunc.CheckAllFiles,
                    self.SourceDirectory,
                    self.DestinationDirectory,
                )
            )

    def Do(self, ftp: ftplib.FTP) -> None:  # Normal mode
        for dirpath, _dirnames, filenames in os.walk(self.SourceDirectory):
            if not any(folder in dirpath for folder in config["ignore_folders"]):

                destdirectory = os.path.join(
                    self.DestinationDirectory,
                    os.path.relpath(dirpath, os.path.join(self.SourceDirectory, "..")),
                )
                jobs.put((AsyncFunc.CheckDirFTP, destdirectory))

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
        commonprefix = os.path.abspath(
            os.path.join(os.path.dirname(os.path.commonprefix(sourcefile)), "..")
        )

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


def main() -> None:
    global config
    with open("config.json", "r") as jsonfile:
        config = json.load(jsonfile)

    config["extensions"] = tuple(config["extensions"])

    ftp = FTPHelper.GetConnection()

    # make common prefix for better logging
    global commonprefix_ftp
    commonprefix_ftp = os.path.dirname(config["ftp_path"])

    log_level = logging.INFO
    if config["debug"]:
        log_level = logging.DEBUG

    logging.basicConfig(
        level=log_level,
        format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
        datefmt='%H:%M:%S',
    )

    logger.info("AutoFastDL started")

    global jobs
    jobs = queue.Queue()

    # Create initial jobs
    WatchManager = pyinotify.WatchManager()
    DirectoryHandlers = []
    for source in config["sources"]:
        handler = DirectoryHandler(source, config["ftp_path"], WatchManager)
        DirectoryHandlers.append(handler)
        handler.Do(ftp)

    ftp.quit()

    # Start worker threads
    for _i in range(config["threads"]):
        worker_thread = threading.Thread(target=FTPHelper.Worker)
        worker_thread.daemon = True
        worker_thread.start()

    # inotify loop
    try:
        while True:
            for handler in DirectoryHandlers:
                handler.Loop()
    except KeyboardInterrupt:
        logger.info("Waiting for remaining jobs to complete...")
        jobs.join()

    logger.info("AutoFastDL exiting")


if __name__ == "__main__":
    main()
