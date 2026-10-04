"""Short-lived extraction processes; one expensive task at a time per worker.

Browser request approval stays in the parent so robots checks and host pacing
are shared with HTTP retrieval. Killing a process group also stops Chromium.
"""
import json
import os
import selectors
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from contextlib import contextmanager
from result_store import memory_usage

HEAVY_SLOT = threading.Lock()
ROOT = Path(__file__).resolve().parent
WAIT = threading.local()
MEMORY_LIMIT_FRACTION = .85
WATCHDOG_INTERVAL = .1


def queue_wait_seconds():
    return getattr(WAIT, 'seconds', 0.)


@contextmanager
def heavy_slot():
    started = time.monotonic()
    with HEAVY_SLOT:
        WAIT.seconds = queue_wait_seconds() + time.monotonic() - started
        yield


class IsolationError(RuntimeError):
    pass


def memory_pressure():
    # Both cgroup versions include Chromium descendants and the temporary files.
    memory = memory_usage()
    limit = memory.get('limit_bytes')
    return bool(limit and memory.get('current_bytes', 0) >= limit * MEMORY_LIMIT_FRACTION)


def kill_group(proc):
    # Popen creates a new session, so this cannot signal the parent worker.
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def stop(proc, already_signaled=False):
    # Also clean up descendant browsers when the direct child has already exited.
    if not already_signaled:
        kill_group(proc)
    proc.wait(timeout=10)


def run_task(kind, body=None, url=None, authorize=None, timeout=90):
    with heavy_slot(), tempfile.TemporaryDirectory(prefix='article-') as directory:
        root = Path(directory)
        if body is not None:
            (root / 'input.html').write_bytes(body)
        (root / 'request.json').write_text(json.dumps({'url': url}))
        try:
            proc = subprocess.Popen(
                [sys.executable, str(ROOT / 'isolated_task.py'), kind, directory],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                text=True, bufsize=1, start_new_session=True)
        except OSError as exc:
            raise IsolationError(f'{kind} process could not start: {exc}') from exc
        selector = selectors.DefaultSelector()
        finished = threading.Event()
        failure = []
        signaled = threading.Event()
        deadline = time.monotonic() + timeout

        def watch():
            # The main thread may be blocked inside authorize (robots or host
            # pacing), or reading a child message. Resource checks must continue.
            while not finished.is_set() and proc.poll() is None:
                reason = None
                if time.monotonic() >= deadline:
                    reason = kind + ' exceeded time limit'
                elif memory_pressure():
                    reason = kind + ' interrupted at container memory safety threshold'
                if reason:
                    failure.append(reason)
                    kill_group(proc)
                    signaled.set()
                    return
                if finished.wait(WATCHDOG_INTERVAL):
                    return

        def check_guard():
            if failure:
                raise IsolationError(failure[0])

        watchdog = threading.Thread(target=watch, name='article-memory-watchdog', daemon=True)
        watchdog.start()
        try:
            selector.register(proc.stdout, selectors.EVENT_READ)
            while proc.poll() is None:
                check_guard()
                for key, _ in selector.select(.2):
                    line = key.fileobj.readline()
                    if not line:
                        selector.unregister(key.fileobj)
                        continue
                    event = json.loads(line)
                    if not isinstance(event, dict) or event.get('type') != 'authorize':
                        raise IsolationError('Unexpected subprocess message')
                    try:
                        allowed = bool(authorize(event['url'], event['document']))
                    except Exception:
                        allowed = False
                    check_guard()
                    proc.stdin.write(json.dumps({'allowed': allowed}) + '\n')
                    proc.stdin.flush()
            check_guard()
            if proc.returncode:
                raise IsolationError(f'{kind} process failed (exit {proc.returncode})')
            output = json.loads((root / 'output.json').read_text())
            if not isinstance(output, dict):
                raise IsolationError('Unexpected subprocess output')
            if output.get('error'):
                raise IsolationError(output['error'])
            if kind == 'render':
                return (root / 'rendered.html').read_bytes(), output['url']
            return output
        except (OSError, ValueError, TypeError, KeyError) as exc:
            # Abrupt OOM exits can break a pipe or leave output absent/truncated.
            # Keep them on the same bounded retry path as explicit stage failures.
            check_guard()
            raise IsolationError(f'{kind} subprocess communication failed: {exc}') from exc
        finally:
            finished.set()
            watchdog.join()
            try:
                stop(proc, already_signaled=signaled.is_set())
            finally:
                selector.close()
                for stream in (proc.stdin, proc.stdout):
                    try:
                        stream.close()
                    except OSError:
                        # Closing a buffered reply may report the same broken
                        # pipe again after an abrupt child exit.
                        pass


def extract_isolated(body, url):
    return run_task('extract', body=body, url=url, timeout=60)


def render_isolated(url, authorize):
    return run_task('render', url=url, authorize=authorize, timeout=150)
