import subprocess
from subprocess import Popen as pop
from threading import Thread, Lock
import time
import re
import os

pop('/bin/gcc --version', shell=False)
task_id = "%s"
cmd = """
%s
""".strip().split("\n")
log_file = "%s"
callback_url = "%s"
myenv = "%s"


def displaycmd(lst):
    s = ""
    for t in lst:
        p = t.find("[[private:")
        while p != -1:
            q = t.find("]]", p)
            if q != -1:
                t = t[0:p+9] + t[q:]
            else:
                t = t[0:p+9]
            p = t.find("[[private:", p+11)
        if s != "":
            s += " "
        if re.search(r"[ \"!{};$]", t):
            s += chr(39)+t+chr(39)
        else:
            s += t
    return s


def rmprivate(lst):
    r = []
    for t in lst:
        p = t.find("[[private:")
        while p != -1:
            t = t[0:p] + t[p+10:]
            q = t.find("]]")
            if q != -1:
                t = t[0:q] + t[q+2:]
                p = t.find("[[private:", q)
            else:
                p = -1
        r.append(t)
    return r


f = open(log_file, "w")
f.write("COMMAND: "+displaycmd(cmd)+"\n")

p1 = subprocess.Popen(rmprivate(cmd),
                      stdout=subprocess.PIPE,
                      stderr=subprocess.STDOUT,
                      universal_newlines=True,
                      env=dict(os.environ, **myenv))

current_log = ""

mutex = Lock()
completed = False


class UpdateLog:
    def __init__(self, current_log):
        self.current_log = current_log

    def update_log_loop(self):
        while True:
            for i in range(60):
                time.sleep(1)
                if completed:
                    return
            mutex.acquire()
            copy_log = self.current_log
            self.current_log = ""
            mutex.release()
            if copy_log:
                try:
                    p = subprocess.Popen(["curl", "--retry", "3", "-X", "PATCH",
                                          callback_url + os.path.join("task",
                                                                      "log") + task_id +
                                          "?duration=%d", "--data-binary", "@-"],
                                         stdin=subprocess.PIPE)
                    p.communicate(copy_log)
                except Exception:
                    pass


if callback_url:
    log_thread = Thread(target=UpdateLog.update_log_loop)
    log_thread.daemon = True
    log_thread.start()


while p1.poll() is None:
    line = p1.stdout.readline()
    f.write(line)
    f.flush()
    mutex.acquire()
    current_log += line
    mutex.release()

completed = True

line = p1.stdout.read()
f.write(line)
f.flush()

if p1.returncode == 0:
    phase = "completed"
else:
    phase = "error"

f.close()

if callback_url:
    mutex.acquire()
    current_log = ""
    mutex.release()
    subprocess.call(["curl", "--retry", "3", "-X", "POST", callback_url +
                     os.path.join("task", "log")
                     + task_id, "--data-binary", "@" + log_file])
    subprocess.call(["curl", "-X", "GET",
                     callback_url + os.path.join("task", "terminate")
                     + task_id + "?phase=" + phase])
