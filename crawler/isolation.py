"""Short-lived extraction processes; one expensive task at a time per worker.

Browser request approval stays in the parent so robots checks and host pacing
are shared with HTTP retrieval. Killing a process group also stops Chromium.
"""
import json,os,selectors,signal,subprocess,sys,tempfile,threading,time
from pathlib import Path
from contextlib import contextmanager

HEAVY_SLOT=threading.Lock()
ROOT=Path(__file__).resolve().parent
WAIT=threading.local()

def queue_wait_seconds():return getattr(WAIT,'seconds',0.)

@contextmanager
def heavy_slot():
    started=time.monotonic()
    with HEAVY_SLOT:
        WAIT.seconds=queue_wait_seconds()+time.monotonic()-started
        yield


class IsolationError(RuntimeError):pass

def memory_pressure():
    # Account for Chromium descendants as well as the Python controller.
    try:
        current=int(Path('/sys/fs/cgroup/memory.current').read_text())
        limit=int(Path('/sys/fs/cgroup/memory.max').read_text())
        return current>limit*.85
    except (OSError,ValueError):return False

def stop(proc):
    try:os.killpg(proc.pid,signal.SIGKILL)
    except ProcessLookupError:pass
    proc.wait(timeout=10)

def run_task(kind,body=None,url=None,authorize=None,timeout=90):
    with heavy_slot(),tempfile.TemporaryDirectory(prefix='article-') as directory:
        root=Path(directory)
        if body is not None:(root/'input.html').write_bytes(body)
        (root/'request.json').write_text(json.dumps({'url':url}))
        proc=subprocess.Popen([sys.executable,str(ROOT/'isolated_task.py'),kind,directory],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True,bufsize=1,start_new_session=True)
        selector=selectors.DefaultSelector();selector.register(proc.stdout,selectors.EVENT_READ)
        deadline=time.monotonic()+timeout
        try:
            while proc.poll() is None:
                if time.monotonic()>deadline:raise IsolationError(kind+' exceeded time limit')
                if memory_pressure():raise IsolationError(kind+' interrupted at container memory safety threshold')
                for key,_ in selector.select(.2):
                    line=key.fileobj.readline()
                    if not line:selector.unregister(key.fileobj);continue
                    event=json.loads(line)
                    if event.get('type')!='authorize':raise IsolationError('Unexpected subprocess message')
                    try:allowed=bool(authorize(event['url'],event['document']))
                    except Exception:allowed=False
                    proc.stdin.write(json.dumps({'allowed':allowed})+'\n');proc.stdin.flush()
            if proc.returncode:raise IsolationError(kind+' process failed (exit '+str(proc.returncode)+')')
            output=json.loads((root/'output.json').read_text())
            if output.get('error'):raise IsolationError(output['error'])
            if kind=='render':return (root/'rendered.html').read_bytes(),output['url']
            return output
        finally:
            if proc.poll() is None:stop(proc)
            else:
                # Clean up any descendant browser process even after normal exit.
                try:os.killpg(proc.pid,signal.SIGKILL)
                except ProcessLookupError:pass
            selector.close();proc.stdin.close();proc.stdout.close()

def extract_isolated(body,url):return run_task('extract',body=body,url=url,timeout=60)
def render_isolated(url,authorize):return run_task('render',url=url,authorize=authorize,timeout=150)
