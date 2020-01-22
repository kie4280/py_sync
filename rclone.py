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

class FileEventHandler(FileSystemEventHandler):

    def __init__(self):
        super().__init__()
        self.monitoring = False
        self.__modified_files__:set = set()
    
    def getModified(self):
        return self.__modified_files__
    def clearModified(self):
        self.__modified_files__.clear()
    def togglestate(self, monitor:bool = False):
        self.monitoring = monitor

    def on_modified(self, event):
        if(self.monitoring):
            print(event, event.src_path)
            if(type(event) == Events.DirModifiedEvent):
                print("dir modified")
                self.__modified_files__.add(Path(event.src_path))
            else if(type(event) == Events.FileModifiedEvent):
                print("file modified")
                self.__modified_files__.add(Path(event.src_path).parent)
                
        return super().on_modified(event)    



class Rclone:

    # some static variables

    __is_syncing__:bool = False
    
    
    def __init__(self, folder):
        super().__init__()
        self.folder:str = folder
        self.STORAGE_NAMES:list = ["test"]
        self.GLOBAL_FLAGS:list = ["--use-json-log",
         "--cache-dir", "{}/.cache".format(self.folder),
         "--cache-chunk-path", "{}/.cache-chunk".format(self.folder),
         "--cache-db-path", "{}/.cache-db".format(self.folder),
         "--drive-chunk-size=256M", "-u"
         ]
        self.BACKEND_FLAGS:list = ["--recursive"]

        self.file_event_handler= FileEventHandler()
        self.fileObserver = Observer()
        self.fileObserver.schedule(self.file_event_handler, folder, recursive=True)
        self.fileObserver.start()
        self.threadpoolExecutor = concurrent.futures.thread.ThreadPoolExecutor(max_workers=8)   
        self.__is_hashing__ = False
        


        atexit.register(self.__cleanup__) # register cleanup
    
    
    def __cleanup__(self):
        self.fileObserver.stop()
        self.fileObserver.join()
        self.__is_hashing__ = False
        self.threadpoolExecutor.shutdown()

    def __check_install__(self):
        try:
            if not os.path.exists(self.folder + "/.cache/"):
                os.mkdir(self.folder + "/.cache/")
            if not os.path.exists(self.folder + "/.cache-chunk/"):
                os.mkdir(self.folder + "/.cache-chunk/")
            if not os.path.exists(self.folder + "/.cache-db/"):
                os.mkdir(self.folder + "/.cache-db/")
            if not os.path.exists(self.folder + "/.logs/"):
                os.mkdir(self.folder + "/.logs/")
        except FileExistsError as exi:
            print(exi)

        
        call_result = subprocess.run(["rclone", *self.GLOBAL_FLAGS, "version"], capture_output=True, encoding="UTF-8")
        self.match_version = re.search(r"rclone v(?:.||\n)*- os/arch:(?:.||\n)*- go version", call_result.stdout)
        # print("stdout", call_result.stdout)
        # print("stderr", call_result.stderr)

        # print(" ".join(["rclone", *self.GLOBAL_FLAGS, "version"]))
        
        if self.match_version != None:
            return 1
        else:
            return 0

    def __generate_hashsum__(self, path:Path):
        '''Generates a checksum file for the files below 
        this directory'''  

        if not self.__is_hashing__: return
        p = Path(path)
        files = [x for x in p.iterdir() if x.is_file()]
        folders = [x for x in p.iterdir() if x.is_dir()] 
        for folds in folders:
            self.threadpoolExecutor.submit(self.__generate_hashsum__, folds)
        
        jj = dict()
        jj["mtime"] = str(datetime.datetime.now())
        for fs in files:
            if fs.name == ".hashsum": continue
            m = hashlib.md5()
            try:
                with open(fs.resolve(), "rb") as f:            
                    for chunk in iter(lambda: f.read(4096), b""):
                        m.update(chunk)
            except IOError as e:
                print(e)
                pass
            h = m.hexdigest()
            jj[fs.name] = h
            print(fs.name, h)
        
        if len(jj) > 1:
            try:
                with open(path.joinpath(".hashsum"), "w") as f:            
                    json.dump(jj, f)
            except IOError as e:
                print("write hashsum error")
                print(e)

                
            

        

        
    async def __check_changes__(self):
        
        self.__is_hashing__ = True
        self.__generate_hashsum__(Path(self.folder))
        
        await asyncio.sleep(6)


    def start(self):
        '''This is the program entry "start" '''

        if not self.__check_install__():
            print("must install first")
            exit(1)

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
            j = json.loads(call_result.stdout)
            print(j[0])
        else:
            print(call_result.stderr)
        

        self.__is_syncing__ = False


if __name__ == "__main__":
    r = Rclone("/home/kie/test")
    r.start()    

