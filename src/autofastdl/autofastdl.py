#!/usr/local/bin/python3.8
import bz2
import datetime
import ftplib
import json
import math
import os
import pathlib
import queue
import random
import shutil
import string
import sys
import threading
from time import sleep
import traceback
from typing import Any, Dict, List, Optional, Tuple

import pyinotify
from colorama import Fore, Style
from dateutil import parser

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


def static_var(varname: str, value: Optional[Any] = None):
    def decorate(func):
        setattr(func, varname, value)
        return func

    return decorate


class Logger:
    @staticmethod
    def ListColoramaColors() -> None:
        from colorama import Fore
        from colorama import init as colorama_init

        colorama_init(autoreset=True)

        colors = dict(Fore.__dict__.items())

        for color in colors.keys():
            print(colors[color] + f"{color}")

    @staticmethod
    def PrettyPrint(filename, status, tag="autofastdl"):
        if status == "Exists":
            color = Fore.WHITE
        elif status == "Added" or status == "Started":
            color = Fore.LIGHTMAGENTA_EX
        elif status == "Done" or status == "Moved" or status == "Moved":
            color = Fore.GREEN
        elif status == "Outdated":
            color = Fore.YELLOW
        elif status.startswith("process_") or status == "Keeping":
            color = Fore.LIGHTCYAN_EX
        else:
            color = Fore.RED

        mytime_text = str(datetime.datetime.now())
        tag_text = str("[" + tag + " @ " + mytime_text + "] ").upper()
        tag_color = Fore.CYAN
        filename_color = Fore.RESET

        if "docker" in config and config["docker"]:
            text_status = "  =>  " + color + status
            if status == "":
                text_status = ""
            text = (
                tag_color
                + tag_text
                + filename_color
                + filename
                + text_status
                + Style.RESET_ALL
            )
            print(text + "\n")
        else:
            columns = int(os.popen("stty size", "r").read().split()[1])
            rows = math.ceil((len(filename) + len(status)) / columns)
            text = (
                tag_color
                + tag_text
                + filename_color
                + filename
                + "." * (columns * rows - (len(tag_text) + len(filename) + len(status)))
                + color
                + status
                + Style.RESET_ALL
            )
            text += chr(8) * (len(text) + 1)
            print(text + "\n" * rows)


class FTPHelper:
    """
    This class is contain corresponding functions for traversing the FTP
    servers using BFS algorithm.
    """

    @staticmethod
    def GetConnection() -> ftplib.FTP:
        if not config["ftp_protocol"] == "ftp":
            Logger.PrettyPrint("FTP protocol not supported!", "ERROR")
            sys.exit(1)

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
    def walk(ftp: ftplib.FTP, path: str = "/"):
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
    @staticmethod
    @static_var("cache_path")
    @static_var("cache_resp")
    @static_var("cache_ftp")
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
    def dir_exists(ftp, path):
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
                retry_max = 10
                infinite_retry = True
                while retry < retry_max:
                    try:
                        ftp.voidcmd("NOOP")
                        break
                    except Exception as e:
                        Logger.PrettyPrint(
                            "Error sending a NOOP cmd ({0})".format(e), "", "error"
                        )
                        Logger.PrettyPrint(
                            f"Trying to reconnect...[{retry}/{retry_max}", "", "info"
                        )
                        sleep(1)
                        try:
                            ftp = FTPHelper.GetConnection()
                        except Exception:
                            pass

                    if not infinite_retry:
                        retry += 1

                if config["debug"]:
                    Logger.PrettyPrint(
                        "Job: {0}({1})".format(job[0].__name__, job[1]), "", "debug"
                    )

                job[0](ftp, job[1:])

                jobs.task_done()

            except Exception as e:
                Logger.PrettyPrint("worker error {0}".format(e), "", "error")
                Logger.PrettyPrint(traceback.format_exc(), "", "error")
                # Put back job in queue if job fails
                jobs.put(job)

        ftp.quit()


class AutoRemove:
    configName = "autoremove"
    configLocal = "local"
    configFTP = "remote"
    configPriority = "priority"

    files_removed_after_upload_lock = threading.Lock()
    files_removed_after_upload: List[str] = []

    @staticmethod
    def HandleFileUploaded(filepath, commonprefix):
        if AutoRemove.IsFileRemovedAfterUpload():
            with AutoRemove.files_removed_after_upload_lock:
                AutoRemove.files_removed_after_upload += [os.path.abspath(filepath)]
                Logger.PrettyPrint(
                    os.path.relpath(filepath, commonprefix), "Deleted", "local"
                )
                os.remove(filepath)

    @staticmethod
    def WasFileRemovedAfterUpload(filepath):
        if AutoRemove.IsFileRemovedAfterUpload():
            with AutoRemove.files_removed_after_upload_lock:
                for filepath_removed in AutoRemove.files_removed_after_upload:
                    if filepath_removed == filepath:
                        AutoRemove.files_removed_after_upload.remove(filepath_removed)
                        return True
        return False

    @staticmethod
    def CheckAllFiles(ftp, sourcedir, destdir):
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
                    Logger.PrettyPrint(
                        "AutoCleanup in " + sourcedir, "Started", "local"
                    )
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
                    Logger.PrettyPrint("AutoCleanup in " + sourcedir, "Done", "local")

    @staticmethod
    def CheckDirFTP(ftp, sourcedir, threaded=False):
        method = AutoRemove.configFTP

        if not AutoRemove.IsStartupClean(method):
            return

        Logger.PrettyPrint("Cleanup in " + sourcedir, "Started", method)

        ftp_extensions: Tuple[str] = ()
        for ext in config["extensions"]:
            ftp_extensions += (ext + ".bz2",)

        ftp_ignore_names: Tuple[str] = ()
        for ign_name in config["ignore_names"]:
            ftp_ignore_names += (ign_name + ".bz2",)

        for dirpath, _dirnames, filenames in FTPHelper.walk(ftp, sourcedir):
            if not any(folder in dirpath for folder in config["ignore_folders"]):
                filenames.sort()
                for filename in [
                    f
                    for f in filenames
                    if f.endswith(ftp_extensions) and f not in ftp_ignore_names
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

        Logger.PrettyPrint("Cleanup", "Done", method)

    @staticmethod
    def CheckFile(ftp, sourcefile, destfile, sourcecommonprefix, destcommonprefix):
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
    def GetTimestampFTP(ftp, myfile):
        timestamp = ftp.sendcmd("MDTM " + myfile)[4:].strip()
        return parser.parse(timestamp)

    @staticmethod
    def GetTimestampFile(myfile):
        fname = pathlib.Path(myfile)
        return datetime.datetime.fromtimestamp(fname.stat().st_mtime)

    @staticmethod
    def CheckFileFTP(ftp, myfile, commonprefix, filedatetime=None):
        method = AutoRemove.configFTP

        if not isinstance(filedatetime, datetime.datetime):
            filedatetime = AutoRemove.GetTimestampFTP(ftp, myfile)

        if AutoRemove.IsOutdated(filedatetime, method):
            Logger.PrettyPrint(
                os.path.basename(myfile) + " (" + str(filedatetime) + ")",
                "Outdated",
                method,
            )
            if AutoRemove.IsFileRemoved(method):
                ftp.delete(myfile)
                Logger.PrettyPrint(myfile, "Deleted", method)
                return False
            else:
                Logger.PrettyPrint(myfile, "Keeping", method)
        return True

    @staticmethod
    def CheckFileLocal(myfile, commonprefix, filedatetime=None):
        method = AutoRemove.configLocal

        if not isinstance(filedatetime, datetime.datetime):
            filedatetime = AutoRemove.GetTimestampFile(myfile)

        if AutoRemove.IsOutdated(filedatetime, method):
            Logger.PrettyPrint(
                os.path.basename(myfile) + " (" + str(filedatetime) + ")",
                "Outdated",
                method,
            )
            if AutoRemove.IsFileRemoved(method):
                os.remove(myfile)
                Logger.PrettyPrint(
                    os.path.relpath(myfile, commonprefix), "Deleted", method
                )
                return False
            else:
                Logger.PrettyPrint(
                    os.path.relpath(myfile, commonprefix), "Keeping", method
                )
        return True

    @staticmethod
    def IsOutdated(mydatetime, method):
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
    def IsConfigGood(method) -> bool:
        return AutoRemove.configName in config

    @staticmethod
    def IsFileRemoved(method) -> bool:
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
    def IsStartupClean(method) -> bool:
        return (
            AutoRemove.configName in config
            and method in config[AutoRemove.configName]
            and "startup_clean" in config[AutoRemove.configName][method]
            and config[AutoRemove.configName][method]["startup_clean"]
        )

    @staticmethod
    def IsAutoCleaned(method) -> bool:
        return (
            AutoRemove.configName in config
            and method in config[AutoRemove.configName]
            and "autoclean" in config[AutoRemove.configName][method]
            and config[AutoRemove.configName][method]["autoclean"]
        )

    @staticmethod
    def GetTimezone(method, name) -> bool:
        return (
            AutoRemove.configName in config
            and method in config[AutoRemove.configName]
            and name in config[AutoRemove.configName][method]
            and config[AutoRemove.configName][method][name]
        )


class AsyncFunc:
    @staticmethod
    def CheckFile(ftp, item) -> None:
        sourcefile, destfile, sourcecommonprefix, destcommonprefix = item

        AutoRemove.CheckFile(
            ftp, sourcefile, destfile, sourcecommonprefix, destcommonprefix
        )

    @staticmethod
    def CheckFileFTP(ftp, item) -> None:
        myfile, commonprefix, filedatetime = item

        AutoRemove.CheckFileFTP(ftp, myfile, commonprefix, filedatetime)

    @staticmethod
    def CheckFileLocal(ftp, item) -> None:
        myfile, commonprefix, filedatetime = item

        AutoRemove.CheckFileLocal(myfile, commonprefix, filedatetime)

    @staticmethod
    def CheckDirFTP(ftp, item):
        sourcedir = item[0]

        AutoRemove.CheckDirFTP(ftp, sourcedir, True)

    @staticmethod
    def CheckFileAdd(ftp, item):
        sourcefile, commonprefix, destfile = item
        if AutoRemove.CheckFileLocal(sourcefile, commonprefix):
            Logger.PrettyPrint(
                os.path.relpath(sourcefile, commonprefix), "Added", "remote"
            )
            jobs.put((AsyncFunc.Compress, sourcefile, destfile))

    @staticmethod
    def CheckAllFiles(ftp, item):
        sourcedir, destdir = item

        AutoRemove.CheckAllFiles(ftp, sourcedir, destdir)

    @staticmethod
    def Compress(ftp, item):
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

            Logger.PrettyPrint(
                os.path.relpath(sourcefile, commonprefix), "Done", "remote"
            )
            AutoRemove.HandleFileUploaded(sourcefile, commonprefix)
        except Exception:
            print("Unexpected error:", sys.exc_info())
            Logger.PrettyPrint(
                os.path.relpath(sourcefile, commonprefix), "Failed (Skipping)", "remote"
            )

        os.remove(tempfile)

        os.rmdir(folder)

    @staticmethod
    def Delete(ftp, item):
        item = item[0]

        try:
            ftp.delete(item)

            Logger.PrettyPrint(
                os.path.relpath(item, commonprefix_ftp), "Deleted", "remote"
            )
        except ftplib.error_perm:
            pass

    @staticmethod
    def Move(ftp, item):
        sourcepath, destpath = item

        # Check whether directory tree exists at destination, create it if necessary
        directory = os.path.dirname(destpath)
        if not FTPHelper.dir_exists(ftp, directory):
            ftp.mkd(directory)

        ftp.rename(sourcepath, destpath)

        Logger.PrettyPrint(
            "{0} -> {1}".format(
                os.path.relpath(sourcepath, commonprefix_ftp),
                os.path.relpath(destpath, commonprefix_ftp),
            ),
            "Moved",
            "remote",
        )


class EventHandler(pyinotify.ProcessEvent):
    def my_init(self, source, destination) -> None:
        self.SourceDirectory = os.path.abspath(source)
        self.DestinationDirectory = os.path.abspath(destination)

    def process_IN_CLOSE_WRITE(self, event) -> None:
        Logger.PrettyPrint(str(event), "process_IN_CLOSE_WRITE", "info")
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

    def process_IN_DELETE(self, event) -> None:
        Logger.PrettyPrint(str(event), "process_IN_DELETE", "info")
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
                Logger.PrettyPrint(destpath + ".bz2", "Keeping", "remote")

    def process_IN_MOVED_TO(self, event) -> None:
        Logger.PrettyPrint(str(event), "process_IN_MOVED_TO", "info")
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
    def __init__(self, source, destination, watchmanager=None):
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

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback) -> None:
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

    def Do(self, ftp) -> None:  # Normal mode
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

    def Checkfile(self, ftp, dirpath, filename) -> None:
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
            Logger.PrettyPrint(
                os.path.relpath(sourcefile, commonprefix), "Exists", "local"
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

    Logger.PrettyPrint("AutoFastDL", "Started", "info")

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
        Logger.PrettyPrint("Waiting for remaining jobs to complete...", "", "info")
        jobs.join()

    Logger.PrettyPrint("AutoFastDL", "Exiting", "info")


if __name__ == "__main__":
    main()
