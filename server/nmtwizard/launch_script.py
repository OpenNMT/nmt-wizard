from threading import Thread, Lock
import time
import re
import os
import subprocess
from subprocess import Popen as pop
import json

task_id = "%s"
cmd = """
%s
""".strip().split("\n")
storage_config = json.loads(%s)
log_file = "%s"
callback_url = "%s"
myenv = json.loads(%s)


def ensure_str(s, encoding="utf-8", errors="ignore"):
    try:
        if s is None:
            return ""
        if not isinstance(s, (str, bytes)):
            raise TypeError("not expecting type \"{}\"".format(type(s)))
        if isinstance(s, bytes):
            s = s.decode(encoding, errors)
        return s
    except Exception as e:
        print ("Exception LOG truncated when encoding")
        print (str(e))
        return ""


def displaycmd(lst):
    s = ""
    for t in lst:
        p = t.find("[[private:")
        while p != -1:
            q = t.find("]]", p)
            if q != -1:
                t = t[0:p + 9] + t[q:]
            else:
                t = t[0:p + 9]
            p = t.find("[[private:", p + 11)
        if s != "":
            s += " "
        if re.search(r"[ \"!{};$]", t):
            s += chr(39) + t + chr(39)
        else:
            s += t
    return s


def rmprivate(lst):
    r = []
    for t in lst:
        p = t.find("[[private:")
        while p != -1:
            t = t[0:p] + t[p + 10:]
            q = t.find("]]")
            if q != -1:
                t = t[0:q] + t[q + 2:]
                p = t.find("[[private:", q)
            else:
                p = -1
        r.append(t)
    return r


f = open(log_file, "w")
f.write(ensure_str("COMMAND: " + displaycmd(cmd) + "\n"))

if storage_config:
    p1 = pop(rmprivate(cmd),
             stdin=subprocess.PIPE,
             stdout=subprocess.PIPE,
             stderr=subprocess.STDOUT,
             universal_newlines=True,
             env=dict(os.environ, **myenv))
    p1.stdin.write(rmprivate([json.dumps(storage_config)])[0])
    p1.stdin.close()
else:
    p1 = pop(rmprivate(cmd),
             stdout=subprocess.PIPE,
             stderr=subprocess.STDOUT,
             universal_newlines=True,
             env=dict(os.environ, **myenv))

current_log = ""

mutex = Lock()
completed = False


def _update_log_loop():
    global current_log
    while True:
        for i in range(60):
            time.sleep(1)
            if completed:
                return
        mutex.acquire()
        copy_log = current_log
        current_log = ""
        mutex.release()
        if copy_log:
            copy_log = ensure_str("COMMAND: " + displaycmd(cmd) + "\n") + copy_log
            try:
                p = subprocess.Popen(["curl", "--retry", "3", "-X", "PATCH",
                                      callback_url+"/task/log/"+task_id+"?duration=%d",
                                      "--data-binary", "@-"],
                                     stdin=subprocess.PIPE)
                p.communicate(copy_log.encode())
            except Exception:
                pass


if callback_url:
    log_thread = Thread(target=_update_log_loop)
    log_thread.daemon = True
    log_thread.start()

while p1.poll() is None:
    try:
        line = ensure_str(p1.stdout.readline())
        f.write(line)
        f.flush()
        mutex.acquire()
        current_log += line
        mutex.release()
    except Exception as e:
        print ("Exception LOG truncated when polling")
        print (str(e))

print ("AFTER while ")
print(p1.returncode)

completed = True

try:
    line = p1.stdout.read()
    f.write(ensure_str(line))
    f.flush()
except Exception as e:
    print("Exception LOG truncated when getting last log")
    print(str(e))
finally:
    f.close()

if p1.returncode == 0:
    phase = "completed"
else:
    phase = "error"

if callback_url:
    mutex.acquire()
    current_log = ""
    mutex.release()
    subprocess.call(["curl", "--retry", "3", "-X", "POST", callback_url + "/task/log/" + task_id,
                     "--data-binary", "@" + log_file])
    subprocess.call(["curl", "-X", "GET", callback_url + "/task/terminate/" + task_id + "?phase=" + phase])
