import json
import pathlib

path = "/home/kie/test/.sync_ignore"
r = pathlib.Path(path)

rclone = json.load(r.joinpath("remote_cache_files1").open("r"))

my = json.load(r.joinpath("remote_cache_files").open("r"))

set1 = set()
set2 = set()

for i in rclone:
    if i["id"] in set1:
        print("same 1")
    else:
        set1.add(i["id"])
for i in my:
    if i["id"] in set2:
        print("same 2")
    else:
        set2.add(i["id"])

s1 = set2.difference(set1)
s2 = set1.difference(set2)
print(s1, s2)

if len(s1) > 0:
    for i in my:
        if i["id"] in s1:
            print(i)
if len(s2) > 0:
    for i in rclone:
        if i["id"] in s2:
            print(i)
print(len(rclone), len(my))