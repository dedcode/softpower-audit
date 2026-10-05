import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from isolation import (extract_isolated, run_task, IsolationError, memory_pressure,
                       heavy_slot, queue_wait_seconds, slot_count)
import isolation
from extract import extract
from pipeline import Pipeline
from crawl import Fetcher
from test_pipeline import Bucket


class IsolationTests(unittest.TestCase):
    @contextmanager
    def child_script(self, script):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'isolated_task.py').write_text(script)
            processes = []
            popen = subprocess.Popen

            def launch(*args, **kwargs):
                proc = popen(*args, **kwargs)
                processes.append(proc)
                return proc

            with patch('isolation.ROOT', root), patch('isolation.subprocess.Popen', side_effect=launch):
                yield processes
            for proc in processes:
                self.assertIsNotNone(proc.returncode)
                with self.assertRaises(ChildProcessError):
                    os.waitpid(proc.pid, os.WNOHANG)
            self.assertFalse(any(t.name == 'article-memory-watchdog' for t in threading.enumerate()))

    def assert_next_task_succeeds(self):
        self.assertEqual(extract_isolated(b'', 'https://example.org/story')['quality'], 'missing')

    def test_fidelity_same_parser_in_fresh_process(self):
        bodies = [
            b'<html><title>404 Not Found</title></html>',
            ('<article><p>' + ('Real reporting. ' * 70) + '</p></article>').encode(),
            ('<div class="story-content"><p>' + ('Introduction. ' * 60)
             + '</p><p>Get full access for Ksh299.</p></div>').encode(),
        ]
        for body in bodies:
            self.assertEqual(extract_isolated(body, 'https://example.org/story'),
                             extract(body, 'https://example.org/story'))

    def test_timeout_reaps_process(self):
        with self.child_script('import time; time.sleep(30)'), self.assertRaisesRegex(IsolationError, 'time limit'):
            run_task('extract', body=b'<html/>', url='https://example.org/story', timeout=-1)
        self.assert_next_task_succeeds()

    def test_memory_guard_is_retryable(self):
        with self.child_script('import time; time.sleep(30)'):
            with patch('isolation.memory_pressure', return_value=True), self.assertRaisesRegex(IsolationError, 'memory safety'):
                extract_isolated(b'<html/>', 'https://example.org/story')
        self.assert_next_task_succeeds()

    def test_host_queue_extends_watchdog_budget_and_is_reported_to_child(self):
        script = '''import json,sys
from pathlib import Path
print(json.dumps({'type':'authorize','url':'https://example.org','document':True}),flush=True)
reply=json.loads(sys.stdin.readline());root=Path(sys.argv[2])
(root/'rendered.html').write_text(str(reply['queue_wait_seconds']))
(root/'output.json').write_text(json.dumps({'url':'https://example.org'}))
'''
        state = [(0., None)]

        def observer():
            done, started = state[0]
            return done + (time.monotonic() - started if started is not None else 0.)

        def authorize(*_):
            started = time.monotonic()
            state[0] = (0., started)
            time.sleep(.7)
            state[0] = (time.monotonic() - started, None)
            return True

        with self.child_script(script), patch('isolation.memory_pressure', return_value=False):
            body, _ = run_task('render', authorize=authorize, timeout=.5, queue_wait=observer)
        self.assertGreaterEqual(float(body), .69)

    def test_authorization_work_without_host_queue_still_times_out(self):
        script = '''import json,sys,time
print(json.dumps({'type':'authorize','url':'https://example.org','document':True}),flush=True)
sys.stdin.readline();time.sleep(30)
'''
        with self.child_script(script), patch('isolation.memory_pressure', return_value=False):
            with self.assertRaisesRegex(IsolationError, 'time limit'):
                run_task('render', authorize=lambda *_: time.sleep(.5) or True,
                         timeout=.2, queue_wait=lambda: 0.)

    def test_browser_rate_limit_is_reported_and_acknowledged_before_child_continues(self):
        script = '''import json,sys
from pathlib import Path
print(json.dumps({'type':'response','url':'https://example.org','status':429,'retry_after':'90'}),flush=True)
reply=json.loads(sys.stdin.readline());root=Path(sys.argv[2])
(root/'rendered.html').write_text(str(reply['received']))
(root/'output.json').write_text(json.dumps({'url':'https://example.org'}))
'''
        events=[]
        with self.child_script(script), patch('isolation.memory_pressure', return_value=False):
            body,_=run_task('render',response=lambda *args:events.append(args))
        self.assertEqual(events,[('https://example.org',429,'90')])
        self.assertEqual(body,b'True')

    def test_memory_guard_reads_both_cgroup_versions(self):
        for paths in [
            {'memory.current': '90', 'memory.max': '100'},
            {'memory/memory.usage_in_bytes': '90', 'memory/memory.limit_in_bytes': '100'},
        ]:
            def read(path):
                relative = str(path).removeprefix('/sys/fs/cgroup/')
                if relative not in paths:
                    raise FileNotFoundError(relative)
                return paths[relative]
            with self.subTest(paths=paths), patch.object(Path, 'read_text', read):
                self.assertTrue(memory_pressure())
        with patch('isolation.memory_usage', return_value={}):
            self.assertFalse(memory_pressure())

    def test_guard_kills_child_during_blocked_authorization(self):
        authorizing = threading.Event()
        killed = threading.Event()
        callback_saw_kill = []
        killed_groups = []
        queue_started = [None]
        real_killpg = os.killpg
        script = '''import json,sys,time
print(json.dumps({'type':'authorize','url':'https://example.org','document':True}),flush=True)
sys.stdin.readline()
time.sleep(30)
'''

        def authorize(url, document):
            queue_started[0] = time.monotonic()
            authorizing.set()
            callback_saw_kill.append(killed.wait(3))
            return True

        def killpg(pid, sig):
            killed_groups.append((pid, sig))
            real_killpg(pid, sig)
            killed.set()

        with self.child_script(script) as processes:
            with patch('isolation.memory_pressure', side_effect=authorizing.is_set), patch('isolation.os.killpg', side_effect=killpg):
                with self.assertRaisesRegex(IsolationError, 'memory safety'):
                    run_task('render', url='https://example.org', authorize=authorize,
                             queue_wait=lambda: time.monotonic()-queue_started[0]
                             if queue_started[0] is not None else 0.)
            self.assertEqual(callback_saw_kill, [True])
            self.assertTrue(killed_groups)
            self.assertTrue(all(pid == processes[0].pid and sig == signal.SIGKILL for pid, sig in killed_groups))
            self.assertNotEqual(processes[0].pid, os.getpgrp())
        self.assert_next_task_succeeds()

    def test_child_crash_is_retryable(self):
        script = 'import os,signal; os.kill(os.getpid(),signal.SIGKILL)'
        with self.child_script(script), self.assertRaisesRegex(IsolationError, 'process failed'):
            run_task('extract', body=b'')
        self.assert_next_task_succeeds()

    def test_malformed_ipc_is_retryable(self):
        with self.child_script('import time; print("not json",flush=True); time.sleep(30)'):
            with self.assertRaisesRegex(IsolationError, 'communication failed'):
                run_task('render')
        self.assert_next_task_succeeds()

    def test_missing_or_truncated_output_is_retryable(self):
        scripts = ['pass', "import sys; from pathlib import Path; (Path(sys.argv[2])/'output.json').write_text('{')"]
        for script in scripts:
            with self.subTest(script=script), self.child_script(script):
                with self.assertRaisesRegex(IsolationError, 'communication failed'):
                    run_task('extract', body=b'')
        self.assert_next_task_succeeds()

    def test_child_exits_before_authorization_reply(self):
        script = '''import json,os,sys
print(json.dumps({'type':'authorize','url':'https://example.org','document':True}),flush=True)
os.close(0)
os._exit(9)
'''
        with self.child_script(script):
            with self.assertRaises(IsolationError):
                run_task('render', authorize=lambda *_: time.sleep(.1) or True)
        self.assert_next_task_succeeds()

    def test_parser_failure_not_overwritten(self):
        fetcher = Pipeline(Bucket(), 'run', max_attempts=1)
        got = {'status': 'retrieved', 'attempts': [], 'http_status': 200, 'raw_uri': 'gs://test/raw',
               'final_url': 'https://example.org/x'}
        with patch.object(Fetcher, 'fetch', return_value=got), patch.object(fetcher, 'read', return_value=b'<html/>'), patch('pipeline.extract', side_effect=IsolationError('memory guard')), patch.object(fetcher, 'render', return_value=(b'<html/>', 'https://example.org/x')), patch.object(fetcher, 'one', return_value=(200, {}, b'{"archived_snapshots":{}}', False)):
            result = fetcher.fetch({'url': 'https://example.org/x', 'outlet': 'example.org'}, 'run', 'KE')
        self.assertEqual(result['status'], 'deferred')


class StageConcurrencyTests(unittest.TestCase):
    def test_slot_configuration_defaults_and_validation(self):
        for name in ('CRAWL_HEAVY_SLOTS', 'CRAWL_BROWSER_SLOTS'):
            with self.subTest(name=name), patch.dict(os.environ, {}, clear=True):
                self.assertEqual(slot_count(name), 1)
                os.environ[name] = '3'
                self.assertEqual(slot_count(name), 3)
                for value in ('0', '-1', 'invalid'):
                    os.environ[name] = value
                    with self.assertRaises(ValueError):
                        slot_count(name)

    def test_browser_wait_does_not_take_parser_capacity(self):
        entered = {name: threading.Event() for name in
                   ('browser1', 'browser2', 'parser1', 'parser2', 'parser3')}
        started = {name: threading.Event() for name in entered}
        browser_waiting = threading.Event()
        release = threading.Event()
        counts = {'all': 0, 'browser': 0, 'peak_all': 0, 'peak_browser': 0}
        lock = threading.Lock()

        class BrowserSlots(threading.BoundedSemaphore):
            def __enter__(self):
                if self.acquire(blocking=False):
                    return True
                browser_waiting.set()
                return self.acquire()

        def task(name, kind):
            started[name].set()
            with heavy_slot(kind):
                with lock:
                    counts['all'] += 1
                    counts['browser'] += kind == 'render'
                    counts['peak_all'] = max(counts['peak_all'], counts['all'])
                    counts['peak_browser'] = max(counts['peak_browser'], counts['browser'])
                entered[name].set()
                try:
                    if not release.wait(5):
                        raise AssertionError('stage release timed out')
                finally:
                    with lock:
                        counts['all'] -= 1
                        counts['browser'] -= kind == 'render'

        with patch('isolation.HEAVY_SLOT', threading.BoundedSemaphore(3)), \
                patch('isolation.BROWSER_SLOT', BrowserSlots(1)), \
                ThreadPoolExecutor(max_workers=5) as pool:
            futures = []
            try:
                futures.append(pool.submit(task, 'browser1', 'render'))
                self.assertTrue(entered['browser1'].wait(2))
                futures.append(pool.submit(task, 'browser2', 'render'))
                self.assertTrue(started['browser2'].wait(2))
                self.assertTrue(browser_waiting.wait(2))
                futures.extend(pool.submit(task, name, 'extract') for name in
                               ('parser1', 'parser2', 'parser3'))
                # Both spare heavy slots remain usable even with a browser
                # waiting. The third parser and second browser must queue.
                deadline = time.monotonic() + 2
                while sum(entered[name].is_set() for name in
                          ('parser1', 'parser2', 'parser3')) < 2 and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertEqual(sum(entered[name].is_set() for name in
                                     ('parser1', 'parser2', 'parser3')), 2)
                self.assertFalse(entered['browser2'].is_set())
                with lock:
                    self.assertEqual(counts['all'], 3)
                    self.assertEqual(counts['browser'], 1)
            finally:
                release.set()
                for future in futures:
                    future.result(timeout=3)
        self.assertTrue(all(event.is_set() for event in entered.values()))
        self.assertEqual(counts['peak_all'], 3)
        self.assertEqual(counts['peak_browser'], 1)

    def test_both_slot_waits_accumulate_without_execution_time(self):
        with patch('isolation.HEAVY_SLOT', threading.BoundedSemaphore(1)), \
                patch('isolation.BROWSER_SLOT', threading.BoundedSemaphore(1)), \
                patch('isolation.WAIT', threading.local()), \
                patch('isolation.time.monotonic', side_effect=[10, 12, 20, 25]):
            isolation.WAIT.seconds = 3
            with heavy_slot('render'):
                self.assertEqual(queue_wait_seconds(), 10)
            self.assertEqual(queue_wait_seconds(), 10)

    def test_failure_releases_browser_and_heavy_capacity(self):
        heavy = threading.BoundedSemaphore(2)
        browser = threading.BoundedSemaphore(1)
        with patch('isolation.HEAVY_SLOT', heavy), patch('isolation.BROWSER_SLOT', browser):
            with self.assertRaisesRegex(RuntimeError, 'failed'):
                with heavy_slot('render'):
                    raise RuntimeError('failed')
        self.assertTrue(browser.acquire(blocking=False))
        self.assertTrue(heavy.acquire(blocking=False))
        self.assertTrue(heavy.acquire(blocking=False))
        self.assertFalse(heavy.acquire(blocking=False))


if __name__ == '__main__':
    unittest.main()
