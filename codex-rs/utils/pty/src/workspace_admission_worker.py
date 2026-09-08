import os
import sys
import time

ready_read, ready_write = os.pipe()
if os.fork():
    os.close(ready_write)
    assert os.read(ready_read, 1) == b"r"
    os._exit(0)
os.close(ready_read)
os.setsid()
for fd in (0, 1, 2):
    os.close(fd)
os.write(ready_write, b"r")
os.close(ready_write)
deadline = time.monotonic() + 10
while not os.path.exists(sys.argv[1]) and time.monotonic() < deadline:
    time.sleep(0.01)
os._exit(0)
