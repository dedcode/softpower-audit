import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from isolation import extract_isolated, run_task, IsolationError, memory_pressure
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
        real_killpg = os.killpg
        script = '''import json,sys,time
print(json.dumps({'type':'authorize','url':'https://example.org','document':True}),flush=True)
sys.stdin.readline()
time.sleep(30)
'''

        def authorize(url, document):
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
                    run_task('render', url='https://example.org', authorize=authorize)
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


if __name__ == '__main__':
    unittest.main()
