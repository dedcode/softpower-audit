"""Browser documents retain shared download capacity until render or redirect ends."""
import threading
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

from test_crawler import Bucket
from test_host_queue import Clock
from test_shared_hosts import FirestoreMemory
from pipeline import Pipeline
from shared_hosts import FirestoreHostStore, SharedHostCoordinator


class BrowserHostCapacityTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.database = FirestoreMemory(self.clock)
        transaction_patch = patch('google.cloud.firestore.transactional', FirestoreMemory.transactional)
        transaction_patch.start()
        self.addCleanup(transaction_patch.stop)
        self.store = FirestoreHostStore(self.database)
        self.coordinator = SharedHostCoordinator(self.store, max_concurrency=1,
            clock=self.clock.now, sleep=self.clock.advance)
        self.other = SharedHostCoordinator(FirestoreHostStore(self.database), max_concurrency=1,
            clock=self.clock.now, sleep=self.clock.advance)
        self.pipeline = Pipeline(Bucket(), 'browser-permits', delay=1, host_coordinator=self.coordinator)
        self.pipeline.robots_delays = {'example.org': 0., 'redirect.org': 0.}

    def active(self):
        return {owner for value in self.database.documents.values()
                for owner, expiry in value.get('leases', {}).items() if expiry > self.clock.now()}

    def render(self, renderer, policy=None):
        with patch('pipeline.render_isolated', side_effect=renderer), \
             patch('pipeline.public_url', side_effect=urlsplit), \
             patch('crawl.public_url', side_effect=urlsplit), \
             patch.object(self.pipeline, 'policy', side_effect=policy,
                          return_value=(None, True, 1, None)):
            return self.pipeline.render('https://example.org/story')

    def test_browser_holds_capacity_against_http_until_render_finishes(self):
        def renderer(url, authorize, **_):
            self.assertTrue(authorize(url, True))
            self.assertEqual(len(self.active()), 1)
            self.clock.advance(5)  # Start spacing has elapsed; only capacity blocks the peer.
            lease, admission = self.other.try_acquire('example.org', delay=1, robots_delay=0)
            self.assertIsNone(lease)
            self.assertFalse(admission.acquired)
            self.assertTrue(authorize('https://example.org/article.js', False))
            self.assertEqual(len(self.active()), 1)
            return b'full article', url

        self.assertEqual(self.render(renderer)[0], b'full article')
        self.assertEqual(self.active(), set())
        lease, admission = self.other.try_acquire('example.org', delay=1, robots_delay=0)
        self.assertTrue(admission.acquired)
        lease.release()

    def test_each_redirect_releases_before_policy_then_reacquires_with_spacing(self):
        starts = []
        owners = []
        def policy(url):
            self.assertEqual(self.active(), set(), 'Old document must not block redirect robots retrieval')
            return None, True, 1, None

        def renderer(url, authorize, **_):
            for target in (url, 'https://example.org/second', 'https://redirect.org/final'):
                self.assertTrue(authorize(target, True))
                starts.append(self.clock.now())
                self.assertEqual(len(self.active()), 1)
                owners.extend(self.active())
            return b'article', 'https://redirect.org/final'

        self.render(renderer, policy)
        self.assertEqual(starts, [100., 101., 101.])
        self.assertEqual(len(set(owners)), 3)
        self.assertEqual(self.active(), set())

    def test_render_failure_releases_active_document(self):
        def renderer(url, authorize, **_):
            self.assertTrue(authorize(url, True))
            raise TimeoutError('Browser was terminated')

        with self.assertRaisesRegex(TimeoutError, 'terminated'):
            self.render(renderer)
        self.assertEqual(self.active(), set())

    def test_denied_redirect_releases_old_document_without_acquiring_another(self):
        def policy(url):
            self.assertEqual(self.active(), set())
            return None, not url.endswith('/denied'), 1, None

        def renderer(url, authorize, **_):
            self.assertTrue(authorize(url, True))
            self.assertFalse(authorize('https://example.org/denied', True))
            self.assertEqual(self.active(), set())
            return b'', url

        self.render(renderer, policy)

    def test_callback_thread_can_acquire_redirect_and_release_from_render_thread(self):
        errors = []
        def renderer(url, authorize, **_):
            def callback():
                try:
                    self.assertTrue(authorize(url, True))
                    self.assertTrue(authorize('https://example.org/next', True))
                except BaseException as exc:
                    errors.append(exc)
            thread = threading.Thread(target=callback)
            thread.start()
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(len(self.active()), 1)
            return b'article', url

        self.render(renderer)
        self.assertEqual(self.active(), set())

    def test_browser_retry_after_cools_other_requests_and_releases_own_permit(self):
        def renderer(url, authorize, response, **_):
            self.assertTrue(authorize(url, True))
            response(url, 429, '600')
            self.assertEqual(len(self.active()), 1)
            return b'', url

        self.render(renderer)
        self.assertEqual(self.active(), set())
        lease, admission = self.other.try_acquire('example.org', delay=1, robots_delay=0)
        self.assertIsNone(lease)
        self.assertEqual(admission.cooldown_seconds, 600)


if __name__ == '__main__':
    unittest.main()
