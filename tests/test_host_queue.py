"""Host contention must preserve pacing without exhausting article budgets."""
import sys
import threading
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from crawl import Fetcher
from pipeline import Pipeline
from test_pipeline import Bucket


class Clock:
    def __init__(self):
        self.value = 100.

    def now(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class TimedLock:
    def __init__(self, clock, delay):
        self.clock, self.delay = clock, delay
        self.held = False

    def acquire(self):
        self.clock.advance(self.delay)
        self.held = True

    def release(self):
        self.held = False


class HostQueueTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.fetcher = Pipeline(Bucket(), 'run', max_attempts=1)
        self.item = {'url': 'https://example.org/x', 'outlet': 'example.org'}
        self.addCleanup(patch.stopall)
        patch('crawl.time.monotonic', side_effect=self.clock.now).start()
        patch('crawl.time.sleep', side_effect=self.clock.advance).start()

    def test_http_requests_keep_spacing_and_do_not_count_network_time_as_queue(self):
        starts = []
        fetcher, clock = self.fetcher, self.clock

        class Session:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def get(self, *args, **kwargs):
                starts.append(clock.now())
                self.assert_host_held = fetcher.hostlock('example.org').locked()
                if not self.assert_host_held:
                    raise AssertionError('HTTP request must retain the host lock')
                clock.advance(.5)
                return nullcontext(SimpleNamespace(status_code=200, headers={},
                    iter_content=lambda *_: [b'article']))

        with patch('crawl.public_url', return_value=SimpleNamespace(hostname='example.org')), patch('crawl.requests.Session', Session):
            fetcher.one(self.item['url'])
            fetcher.one(self.item['url'])
        self.assertEqual(starts, [100., 103.])
        self.assertAlmostEqual(fetcher.host_queue_wait_seconds(), 2.5)
        self.assertFalse(fetcher.hostlock('example.org').locked())

    def test_nested_robots_and_host_waits_are_counted_once(self):
        locks = {'robots:https://example.org': TimedLock(self.clock, 2),
                 'example.org': TimedLock(self.clock, 5)}
        with patch.object(self.fetcher, 'hostlock', side_effect=locks.__getitem__):
            with self.fetcher.queued_host_lock('robots:https://example.org'):
                with self.fetcher.host_slot('example.org'):
                    self.clock.advance(11)  # This URL's own network operation.
        self.assertEqual(self.fetcher.host_queue_wait_seconds(), 7)
        self.assertTrue(all(not lock.held for lock in locks.values()))

    def test_wait_accounting_is_specific_to_article_thread(self):
        self.fetcher.queue_wait.seconds = 30
        seen = []

        def other_article():
            seen.append(self.fetcher.host_queue_wait_seconds())
            self.fetcher.queue_wait.seconds = 12
            seen.append(self.fetcher.host_queue_wait_seconds())

        thread = threading.Thread(target=other_article)
        thread.start()
        thread.join()
        self.assertEqual(seen, [0., 12])
        self.assertEqual(self.fetcher.host_queue_wait_seconds(), 30)

    def test_long_host_queue_does_not_skip_recovery_stages(self):
        # Prior work in the same pool thread must not be subtracted twice.
        self.fetcher.queue_wait.seconds = 1000

        def retrieve(*_):
            with patch.object(self.fetcher, 'hostlock', return_value=TimedLock(self.clock, 360)):
                with self.fetcher.host_slot('example.org'):
                    self.clock.advance(1)
            return {'status': 'unavailable', 'attempts': [], 'http_status': 404, 'raw_uri': None}

        with patch.object(Fetcher, 'fetch', side_effect=retrieve), patch.object(self.fetcher, 'one', return_value=(200, {}, b'{"archived_snapshots":{}}', False)) as archive:
            result = self.fetcher.fetch({**self.item, 'first_observed': '2020-01-02'}, 'run', 'KE')
        self.assertEqual(archive.call_count, 2)
        self.assertEqual(result['status'], 'exhausted')
        self.assertFalse(any(event.get('reason') == 'Per-URL time budget' for event in result['attempts']))

    def test_real_work_still_consumes_article_budget(self):
        def retrieve(*_):
            self.clock.advance(360)
            return {'status': 'unavailable', 'attempts': [], 'http_status': 404, 'raw_uri': None}

        with patch.object(Fetcher, 'fetch', side_effect=retrieve), patch.object(self.fetcher, 'one') as archive:
            result = self.fetcher.fetch(self.item, 'run', 'KE')
        archive.assert_not_called()
        self.assertEqual(result['status'], 'deferred')
        self.assertTrue(any(event.get('reason') == 'Per-URL time budget' for event in result['attempts']))

    def test_browser_document_uses_same_pacing_and_wait_accounting(self):
        self.fetcher.last['example.org'] = self.clock.now()

        def renderer(url, authorize, queue_wait=None, response=None):
            self.assertTrue(authorize(url, True))
            self.assertEqual(queue_wait(),3.)
            return b'rendered', url

        with patch('pipeline.public_url', return_value=SimpleNamespace(hostname='example.org')), patch.object(self.fetcher, 'policy', return_value=(None, True, 3, None)), patch('pipeline.render_isolated', side_effect=renderer):
            self.fetcher.render(self.item['url'])
        self.assertEqual(self.fetcher.last['example.org'], 103.)
        self.assertEqual(self.fetcher.host_queue_wait_seconds(), 3.)

    def test_observer_reads_live_queue_from_another_thread(self):
        observer=self.fetcher.host_queue_observer()
        with self.fetcher.queued_wait():
            self.clock.advance(17)
            seen=[]
            thread=threading.Thread(target=lambda:seen.append(observer()))
            thread.start();thread.join()
            self.assertEqual(seen,[17.])
        self.clock.advance(50)  # Own work after acquisition is not queue time.
        self.assertEqual(observer(),17.)


if __name__ == '__main__':
    unittest.main()
