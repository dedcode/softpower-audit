"""A throttled host pauses every article thread, including archive requests."""
import sys
import threading
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
import crawl
from test_crawler import Bucket
from test_host_queue import Clock


class HostCooldownTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.fetcher = crawl.Fetcher(Bucket(), 'run', max_attempts=3)
        self.url = 'https://example.org/story'
        self.starts = []
        self.replies = []
        self.addCleanup(patch.stopall)
        patch('crawl.time.monotonic', side_effect=self.clock.now).start()
        self.sleep = patch('crawl.time.sleep', side_effect=self.clock.advance).start()
        patch('crawl.public_url', side_effect=urlsplit).start()
        owner = self

        class Session:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def get(self, url, **kwargs):
                owner.starts.append((url, owner.clock.now()))
                status, headers = owner.replies.pop(0)
                return nullcontext(SimpleNamespace(status_code=status, headers=headers,
                    iter_content=lambda *_: [b'response']))

        patch('crawl.requests.Session', Session).start()

    def test_second_thread_waits_for_other_articles_retry_after(self):
        self.replies = [(429, {'Retry-After': '30'}), (200, {})]
        self.fetcher.one(self.url)
        sleeping, release = threading.Event(), threading.Event()
        failures = []

        def sleep(seconds):
            sleeping.set()
            if not release.wait(2):
                raise AssertionError('Test did not release cooldown wait')
            self.clock.advance(seconds)

        def other_article():
            try:
                self.fetcher.one('https://example.org/another-story')
            except Exception as exc:
                failures.append(exc)

        self.sleep.side_effect = sleep
        thread = threading.Thread(target=other_article)
        thread.start()
        try:
            self.assertTrue(sleeping.wait(2))
            self.assertEqual(len(self.starts), 1, 'No request is sent while Retry-After is active')
        finally:
            release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual([stamp for _, stamp in self.starts], [100., 130.])

    def test_long_cooldown_is_retryable_without_network_or_unbounded_sleep(self):
        self.replies = [(429, {'Retry-After': '3600'})]
        self.fetcher.one(self.url)
        with self.assertRaises(crawl.HostCooldown) as caught:
            self.fetcher.one('https://example.org/another-story')
        self.assertEqual(caught.exception.retry_after_seconds, 3600)
        self.assertEqual(len(self.starts), 1)
        self.sleep.assert_not_called()

        with patch.object(self.fetcher, 'policy', return_value=(None, True, 3, None)):
            result = self.fetcher.fetch({'url': 'https://example.org/new-story', 'outlet': 'example.org'}, 'run', 'KE')
        self.assertEqual(result['status'], 'temporary_error')
        self.assertEqual(result['retry_after_seconds'], 3600)
        self.assertEqual(len(result['attempts']), 1)
        self.assertEqual(len(self.starts), 1)
        self.sleep.assert_not_called()

    def test_service_unavailable_also_cools_host(self):
        for headers, expected in [({'Retry-After': '12'}, 12), ({}, 10)]:
            with self.subTest(headers=headers):
                self.fetcher = crawl.Fetcher(Bucket(), 'run')
                self.clock.value = 100
                self.starts.clear()
                self.replies = [(503, headers), (200, {})]
                self.fetcher.one(self.url)
                self.fetcher.one(self.url)
                self.assertEqual([stamp for _, stamp in self.starts], [100., 100. + expected])

    def test_archive_lookups_share_cooldown_without_pausing_unrelated_hosts(self):
        self.replies = [(429, {'Retry-After': '3600'}), (200, {})]
        self.fetcher.one('https://archive.org/wayback/available?url=first')
        self.fetcher.one(self.url)
        with self.assertRaises(crawl.HostCooldown):
            self.fetcher.one('https://archive.org/wayback/available?url=second')
        self.assertEqual(len(self.starts), 2)
        self.assertEqual(self.starts[1], (self.url, 100.))

    def test_cooldown_cannot_be_shortened_by_later_signal(self):
        with self.fetcher.hostlock('example.org'):
            self.fetcher.defer_host('example.org', 60)
            self.clock.advance(10)
            self.fetcher.defer_host('example.org', 5)
        self.assertEqual(self.fetcher.cooldowns['example.org'], 160)


if __name__ == '__main__':
    unittest.main()
