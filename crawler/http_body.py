"""Bounded streamed response reads; keep pooled HTTP requests in the worker."""
import threading,time
from urllib3.exceptions import HTTPError
from requests.exceptions import ConnectionError,Timeout

def read_body(response,limit,*,timeout=40,cancelled=lambda:False):
    raw=response.raw;done=threading.Event();expired=threading.Event();aborted=threading.Event()
    deadline=time.monotonic()+timeout
    def watch():
        while not done.wait(.05):
            if cancelled():aborted.set()
            elif time.monotonic()>=deadline:expired.set()
            else:continue
            # Public urllib3 API interrupts a blocked socket read, including a
            # compressed stream which has not yet yielded a decoded chunk.
            try:raw.shutdown()
            except (OSError,ValueError,RuntimeError):pass
            return
    watcher=threading.Thread(target=watch,name='http-body-deadline',daemon=True);watcher.start()
    data=bytearray()
    try:
        while True:
            if cancelled() or aborted.is_set():raise Timeout('Response read cancelled')
            if expired.is_set() or time.monotonic()>=deadline:return bytes(data),True
            try:chunk=raw.read1(min(65536,limit+1-len(data)),decode_content=True)
            except HTTPError as exc:
                if aborted.is_set():raise Timeout('Response read cancelled') from exc
                if expired.is_set():return bytes(data),True
                raise ConnectionError(exc) from exc
            if aborted.is_set():raise Timeout('Response read cancelled')
            if expired.is_set():return bytes(data),True
            if not chunk:return bytes(data),False
            data.extend(chunk)
            if len(data)>limit:return bytes(data[:limit]),True
    finally:
        done.set();watcher.join()
