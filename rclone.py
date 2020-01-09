import subprocess
import re
import json
import os


class Rclone:

    
    
    def __init__(self, folder):
        super().__init__()
        self.folder:str = folder
        self.STORAGE_NAMES:list = ["nctu", "MyProject", "test"]
        self.flags:list = ["--use-json-log", "--log-file", "{}/.logs/log1".format(self.folder),
         "--cache-dir", "{}/.cache".format(self.folder),
         "--cache-chunk-path", "{}/.cache-chunk".format(self.folder),
         "--cache-db-path", "{}/.cache-db".format(self.folder),
         "--drive-chunk-size=256M"
         ]
    

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

        
        call_result = subprocess.run(["rclone", *self.flags, "version"], capture_output=True, encoding="UTF-8")
        self.match_version = re.search(r"rclone v(?:.||\n)*- os/arch:(?:.||\n)*- go version", call_result.stdout)
        # print("stdout", call_result.stdout)
        # print("stderr", call_result.stderr)

        # print(" ".join(["rclone", *self.flags, "version"]))
        
        if self.match_version != None:
            return 1
        else:
            return 0
        pass
    def check_changes(self):
        pass

    def sync(self):
        if not self.check_install():
            print("must install first")
            exit(1)
        call_result = subprocess.run(["rclone", *self.flags, 
            "lsjson", "{0}:/".format(self.STORAGE_NAMES[2])], capture_output=True, encoding="UTF-8")

        
        
        if call_result.stderr == "":        
            # print(call_result.stdout)
            j = json.loads(call_result.stdout)
            print(j[0])
        else:
            print(call_result.stderr)

        call_result = subprocess.run(["rclone", *self.flags, 
            "md5sum", "{0}:/".format(self.STORAGE_NAMES[2])], capture_output=True, encoding="UTF-8")
        print(call_result)



if __name__ == "__main__":
    r = Rclone("G:/GsuiteDrive/test")
    r.sync()
