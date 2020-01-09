import subprocess
import re


class Rclone:

    STORAGE_NAMES:list = ["nctu", "MyProject"]
    
    def __init__(self):
        super().__init__()
    

    def check_install(self):
        call_result = subprocess.run(["rclone", "--version"], capture_output=True, encoding="UTF-8")
        self.match_version = re.search(r"rclone v(?:.||\n)*- os/arch:(?:.||\n)*- go version", call_result.stdout)
        # print("stdout", call_result.stdout)
        # print("stderr", call_result.stdout)
        if self.match_version != None:
            return 1
        else:
            return 0
        pass

    def sync(self):
        if not self.check_install():
            print("must install first")
            exit(1)
        call_result = subprocess.run(["rclone", "lsf", "{0}:/".format(self.STORAGE_NAMES[0])], capture_output=True, encoding="UTF-8")
        print(call_result.stderr)
        print(call_result.stdout)


r = Rclone()
r.sync()
