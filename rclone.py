import subprocess
import re
import json
import atexit
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import watchdog.events as Events
import asyncio
import hashlib
import concurrent.futures.thread
import concurrent.futures
import datetime

from pathlib import *

IGNORE_FILES: set = {".hashsum", ".hashsum_old"}
IGNORE_FOLDS: set = {".sync_ignore"}


class FileEventHandler(FileSystemEventHandler):

    def __init__(self):
        super().__init__()
        self.monitoring = False
        self.__modified_files__: set = set()

    def getModified(self):
        return self.__modified_files__

    def clearModified(self):
        self.__modified_files__.clear()

    def togglestate(self, monitor: bool = False):
        self.monitoring = monitor

    def on_modified(self, event):
        if(self.monitoring):
            # print(event, event.src_path)
            if(type(event) == Events.DirModifiedEvent):
                # print("dir modified")
                self.__modified_files__.add(Path(event.src_path))
            elif(type(event) == Events.FileModifiedEvent):
                # print("file modified")
                file = Path(event.src_path)
                if file.name in IGNORE_FILES:
                    self.__modified_files__.add(file.parent)

        return super().on_modified(event)


class DriveSync:

    # some static variables

    def __init__(self, folder):
        super().__init__()
        self.local_dir: str = folder
        self.STORAGE_NAMES: list = ["test"]
        self.GLOBAL_FLAGS: list = [
        "--use-json-log", "--cache-dir", "{}/.cache".format(self.local_dir),
        "--cache-chunk-path", "{}/.cache-chunk".format(self.local_dir),
        "--cache-db-path", "{}/.cache-db".format(self.local_dir),
        "--drive-chunk-size=64M", "-u", "--fast-list"
        ]
        self.BACKEND_FLAGS: list = [
        "--recursive", "--drive-use-trash",
         "--drive-acknowledge-abuse", "--exclude", "/.sync_ignore/**"
        ]

        self.file_event_handler = FileEventHandler()
        self.fileObserver = Observer()
        self.fileObserver.schedule(
            self.file_event_handler, folder, recursive=True)
        self.fileObserver.start()
        self.threadpoolExecutor = concurrent.futures.thread.ThreadPoolExecutor(
            max_workers=8)
        self.__is_hashing__ = False
        self.__is_caching__ = False
        self.__is_syncing__: bool = False

        atexit.register(self.__cleanup__)  # register cleanup

    def __cleanup__(self):
        self.fileObserver.stop()
        self.fileObserver.join()
        self.__is_hashing__ = False
        self.__is_caching__ = False
        self.threadpoolExecutor.shutdown()

    def __check_install__(self):

        try:
            folder = Path(self.local_dir)
            if not folder.joinpath(".cache").exists():
                folder.joinpath(".cache").mkdir()
            if not folder.joinpath(".cache-db").exists():
                folder.joinpath(".cache-db").mkdir()
            if not folder.joinpath(".cache-chunks").exists():
                folder.joinpath(".cache-chunks").mkdir()
            if not folder.joinpath(".logs").exists():
                folder.joinpath(".logs").mkdir()
        except FileExistsError as exi:
            print(exi)

        call_result = subprocess.run(
            ["rclone", *self.GLOBAL_FLAGS, "version"], capture_output=True, encoding="UTF-8")
        self.match_version = re.search(
            r"rclone v(?:.||\n)*- os/arch:(?:.||\n)*- go version", call_result.stdout)
        # print("stdout", call_result.stdout)
        # print("stderr", call_result.stderr)

        # print(" ".join(["rclone", *self.GLOBAL_FLAGS, "version"]))

        if self.match_version != None:
            return 1
        else:
            return 0

    def generate_hashsum(self, path: Path):
        '''Generates a checksum file in the ".sync_ignore" folder for the files below 
        "path"  '''

        if not self.__is_hashing__:
            return

        files = list()
        folders = list()
        for x in path.iterdir():
            if x.is_file():
                files.append(x)
            elif x.is_dir() and x.name not in IGNORE_FOLDS:
                folders.append(x)
                self.threadpoolExecutor.submit(self.generate_hashsum, x)

        jj = dict()
        jj["ModTime"] = str(datetime.datetime.now())
        jj["Path"] = str(path.resolve())
        jj["files"] = list()
        for fs in files:
            if(fs.name in IGNORE_FILES):
                continue
            m = hashlib.md5()
            try:
                with open(fs.resolve(), "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        m.update(chunk)
            except IOError as e:
                print(e)
                pass
            h = m.hexdigest()
            data: dict = dict()
            data["Name"] = fs.name
            data["Hash"] = h
            data["ModTime"] = str(
                datetime.datetime.fromtimestamp(fs.stat().st_mtime))
            jj["files"].append(data)
            print(fs.name, h)

        if len(jj["files"]) > 0:
            
            m = hashlib.md5(str(path.resolve()).encode("UTF-8"))
            try:
                with open(Path(self.local_dir).joinpath(".sync_ignore", 
                    "hashsum_local", m.hexdigest()), "w") as f:
                    json.dump(jj, f, ensure_ascii=False)
            except IOError as e:
                print("write local hashsum error")
                print(e)

    async def cache_remote(self, paths: list):
        '''Cache the remote hash of path "paths" on local directory 

        '''

        if not self.__is_caching__:
            return

        popen = subprocess.Popen(
            ["rclone", *self.GLOBAL_FLAGS, "lsjson", "--hash", "--recursive",
            "{0}:/{1}".format("GsuiteDrive", "/".join(paths))],
            encoding="UTF-8", stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        while popen.poll() == None:
            print(popen.stdout.readline())
            await asyncio.sleep(1)
        print("write complete")
        
        if 1:    
            k = Path(self.local_dir).joinpath(".sync_ignore", "cache.json")
            j: list = json.load(k.open("r"))
            files: list = list()
            folders: list = list() 
            for i in j:
                if i["IsDir"]:
                    folders.append(i["Name"])
                else:
                    files.append(i)

            try:

                syncIGP = Path(self.local_dir).joinpath(".sync_ignore", "hashsum_remote").resolve()
                m = hashlib.md5(str(syncIGP.joinpath(*paths)).encode("UTF-8"))
                with syncIGP.joinpath(m.hexdigest()).open(mode="w", encoding="UTF-8") as f:
                    json.dump(files, f, ensure_ascii=False)
            except IOError as e:
                print("write remote hashsum error")
                print(e)

            for folder in folders:
                newpath = list(paths)
                newpath.append(folder)
                self.threadpoolExecutor.submit(self.cache_remote, newpath)
        else:
            print(call_result.stderr)

    async def __check_changes__(self):

        self.__is_hashing__ = True
        self.__is_caching__ = True
        # self.file_event_handler.togglestate(True)
        # self.generate_hashsum(Path(self.local_dir))
        # for i in self.file_event_handler.getModified():
        #     print(i)
        await self.cache_remote([])
        
        print("done")
        await asyncio.sleep(6)

    def start(self):
        '''This is the function to start syncing '''

        if not self.__check_install__():
            print("must install first")
            exit(1)
        root = Path(self.local_dir)
        if not root.exists():
            print("invalid destination folder")
            exit(2)
        ignore = root.joinpath(".sync_ignore")
        ignore.mkdir(parents=False, exist_ok=True)
        local = ignore.joinpath("hashsum_local")
        remote = ignore.joinpath("hashsum_remote")
        local.mkdir(parents=False, exist_ok=True)
        remote.mkdir(parents=False, exist_ok=True)
        while True:

            try:
                asyncio.run(self.__check_changes__())

            except KeyboardInterrupt:
                print("exit")
                exit(0)

    async def __sync__(self):

        self.__is_syncing__ = True

        call_result = subprocess.run(["rclone", *self.GLOBAL_FLAGS,
                                      "lsjson", "{0}:/test_rclone".format(self.STORAGE_NAMES[3])], capture_output=True, encoding="UTF-8")

        print("stdout", call_result.stdout)
        print("stderr", call_result.stderr)
        print("return code:", call_result.returncode)

        if call_result.returncode == 0:
            # print(call_result.stdout)
            j = json.loads(call_result.stdout, encoding="UTF-8")
            print(j[0])
        else:
            print(call_result.stderr)

        self.__is_syncing__ = False


if __name__ == "__main__":
    r = DriveSync("/home/kie/test")
    r.start()
