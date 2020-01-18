import subprocess
import re
import json
import os
import atexit
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import watchdog.events as Events



class FileEventHandler(FileSystemEventHandler):

    def __init__(self):
        super().__init__()
        self.__is_modifying__ = False
        self.__modified_list__ = []
    
    def getModified(self):
        return self.__modified_list__

    def on_modified(self, event):

        print(event, event.src_path)
        if(type(event) == Events.DirModifiedEvent):
            print("dir modified")
        return super().on_modified(event)    



class Rclone:

    # some static variables

    __is_syncing__ = False
    
    
    def __init__(self, folder):
        super().__init__()
        self.folder:str = folder
        self.STORAGE_NAMES:list = ["nctu", "MyProject", "test", "Gsuite"]
        self.GLOBAL_FLAGS:list = ["--use-json-log",
         "--cache-dir", "{}/.cache".format(self.folder),
         "--cache-chunk-path", "{}/.cache-chunk".format(self.folder),
         "--cache-db-path", "{}/.cache-db".format(self.folder),
         "--drive-chunk-size=256M", "-u"
         ]
        self.BACKEND_FLAGS:list = ["--recursive"]

        self.file_event_watcher= FileEventHandler()
        self.fileObserver = Observer()
        self.fileObserver.schedule(self.file_event_watcher, folder, recursive=True)
        self.fileObserver.start()

        atexit.register(self.cleanup)
    
    
    def cleanup(self):
        self.fileObserver.stop()
        self.fileObserver.join()

    def check_install(self):
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
        pass
    def check_changes(self):

        pass

    def start(self):
        try:
            while True:
                self.__sync__()
                time.sleep(60)
        except KeyboardInterrupt:
            pass

    def __sync__(self):
        if not self.check_install():
            print("must install first")
            exit(1)
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

        # call_result = subprocess.run(["rclone", *self.GLOBAL_FLAGS, 
        #     "check", "{0}:/test_rclone".format(self.STORAGE_NAMES[3]), "/media/kie/VM/VMfolder"], capture_output=True, encoding="UTF-8")
        # num_diff_files = 0
        # for diffs in str(call_result.stderr).split("\n"):
        #     if len(diffs) != 0:
        #         error = json.loads(diffs)
        #         print(error)




if __name__ == "__main__":
    r = Rclone("/media/kie/VM/VMfolder")
    r.start()    

