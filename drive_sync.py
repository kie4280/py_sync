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
import queue
import datetime
from pathlib import *
from GDrive import DriveClient
import signal
from errors import DriveError
import time


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
                
                self.__modified_files__.add(file.parent)

        return super().on_modified(event)


class DriveSync:

    # some static variables

    def __init__(self, folder):
        super().__init__()
        self.local_dir: str = folder
        self.cache_dir: str = ""
        self.file_event_handler = FileEventHandler()
        self.searchFolderUnfinished: int = 0
        self.fileObserver = Observer()
        self.fileObserver.schedule(
            self.file_event_handler, folder, recursive=True)
        self.fileObserver.start()
        self.searchFolderQueue: queue.Queue = queue.Queue(10000)
        self.fileWriteQueue: queue.Queue = queue.Queue(1000)
        self.folderWriteQueue: queue.Queue = queue.Queue(1000)
        self.threadpoolExecutor = concurrent.futures.thread.ThreadPoolExecutor(
            max_workers=8)
        self.__is_hashing__: bool = False
        self.__is__checking__: bool = False
        self.__is_syncing__: bool = False
        self.driveclient = DriveClient(self.cache_dir)
        signal.signal(signal.SIGINT, self.__keyboardINT__)
        signal.signal(signal.SIGTERM, self.__terminate__)
        atexit.register(self.__cleanup__)  # register cleanup

    def __terminate__(self, signal, frame):
        self.__cleanup__()

    def __cleanup__(self):
        self.fileObserver.stop()
        self.fileObserver.join()
        self.__is_hashing__ = False
        self.__is__checking__ = False
        self.threadpoolExecutor.shutdown(True)

    def __keyboardINT__(self, signal, frame):
        print("keyboard interrupt received")
        self.__cleanup__()

    def __startup_check__(self):
        
        folder = Path(self.local_dir)
        if not folder.exists() or not folder.is_dir():
            raise DriveError(DriveError.INVALID_SYNC_FOLDER)           
        if not os.access(self.local_dir, os.R_OK|os.W_OK|os.X_OK):
            raise DriveError(DriveError.NO_PERMISSION)

        IGN = Path(self.local_dir).joinpath(".sync_ignore")

        if not IGN.exists():
            IGN.mkdir()
        self.cache_dir = str(IGN.resolve())      

        
    def writeToCache(self, q: queue.Queue, cache_file_name: str = "drive_cache"):

        first: bool = True
        with Path(self.cache_dir).joinpath(cache_file_name).open(mode="w", encoding="UTF-8") as f:
            f.write("[")
            while self.__is_hashing__ == True:
                if q.qsize() == 0:
                    time.sleep(0.1)
                else:
                    t: str = q.get(block=True)
                    if t == None:
                        q.task_done()
                        break
                    else:
                        if first:
                            first = False
                        else:
                            f.write(",")
                        f.write(t)
                        q.task_done()
            f.write("]")

    def _searchFolder(self):

        
        sq = self.searchFolderQueue
        while self.__is_hashing__:
            files: list = list()
            folders: list = list()
            try:
                path = sq.get(block=False)
                for x in path.iterdir():
                    if x.is_file():
                        m = hashlib.md5()
                        try:
                            
                            with x.open("rb") as f:
                                for chunk in iter(lambda: f.read(4096), b""):
                                    m.update(chunk)
                        except IOError as e:
                            print(e)
                            pass
                        h = m.hexdigest()   
                        data: dict = dict()                     
                        data["name"] = x.name
                        data["md5Checksum"] = h
                        data["path"] = str(x.parent)
                        data["isDir"] = False
                        data["modifiedTime"] = str(
                            datetime.datetime.utcfromtimestamp(x.stat().st_mtime))   
                        files.append(data)

                    elif x.is_dir() and x.name not in IGNORE_FOLDS:
                        data: dict = dict()
                        data["name"] = x.name                        
                        data["path"] = str(x.parent)
                        data["isDir"] = True
                        data["modifiedTime"] = str(
                            datetime.datetime.utcfromtimestamp(x.stat().st_mtime))
                        folders.append(data)
                        sq.put(x, block=False)
                        self.searchFolderUnfinished += 1
                ri: str = json.dumps(files, ensure_ascii=False)[1:-1]
                ro: str = json.dumps(folders, ensure_ascii=False)[1:-1]
                if len(ri) > 0:
                    self.fileWriteQueue.put(ri, block=False)
                if len(ro) > 0:
                    self.folderWriteQueue.put(ro, block=False)

                  
            except queue.Empty:
                pass
            except FileNotFoundError as e:
                print(e)
                sq.task_done()
                self.searchFolderUnfinished -= 1
            except Exception as e:
                sq.task_done()
                self.searchFolderUnfinished -= 1
                raise
            else:
                sq.task_done()
                self.searchFolderUnfinished -= 1

    async def generate_hashsum(self, path: Path):
        '''Generates a checksum file in the ".sync_ignore" folder for the files below 
        "path"  '''
        self.__is_hashing__ = True
        self.__is__checking__ = True
        self.searchFolderQueue.put(path, block=False)
        self.searchFolderUnfinished += 1
        self.threadpoolExecutor.submit(self.writeToCache, self.fileWriteQueue, "local_cache_files")
        self.threadpoolExecutor.submit(self.writeToCache, self.folderWriteQueue, "local_cache_folders")
        self.threadpoolExecutor.submit(self._searchFolder)
        while self.searchFolderUnfinished > 0:
            await asyncio.sleep(0.5)

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

                syncIGP = Path(self.local_dir).joinpath(
                    ".sync_ignore", "hashsum_remote").resolve()
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

        self.file_event_handler.togglestate(True)
        while self.__is__checking__:            
            
            gen = asyncio.create_task(self.generate_hashsum(Path(self.local_dir)), name="generate_hashsum")
            
            for i in self.file_event_handler.getModified():
                print(i)
            # await self.cache_remote([])
            await asyncio.sleep(6)
            if gen.done():
                break
        print("done")
        

    

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

    def start(self):
        '''This is the function to start syncing '''

        self.__startup_check__()
        asyncio.run(self.__check_changes__())
        
    def stop(self):
        self.__cleanup__()

if __name__ == "__main__":
    r = DriveSync("/home/kie/test")
    r.start()
