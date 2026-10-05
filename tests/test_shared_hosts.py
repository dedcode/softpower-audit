"""Distributed host coordination, tested without network or cloud credentials."""
import copy
import sys
import threading
import unittest
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from crawl import Fetcher
from pipeline import Pipeline
from shared_hosts import FirestoreHostStore, SharedHostCoordinator, SharedHostCooldown, HostLeaseLost
from test_crawler import Bucket
from test_host_queue import Clock


class FirestoreMemory:
    """Serial transactions exposing Firestore's server-assigned read_time."""
    def __init__(self, clock):
        self.clock, self.documents, self.lock = clock, {}, threading.Lock()

    def collection(self, name):
        client = self

        class Collection:
            def document(self, key):
                path = name + '/' + key

                class Reference:
                    def get(self, transaction):
                        data = copy.deepcopy(client.documents.get(path))
                        return SimpleNamespace(to_dict=lambda: data,
                            read_time=datetime.fromtimestamp(client.clock.now(), timezone.utc))

                result = Reference()
                result.path = path
                return result
        return Collection()

    def transaction(self, **_):
        return SimpleNamespace(client=self,
            set=lambda reference, data: self.documents.update({reference.path: copy.deepcopy(data)}))

    @staticmethod
    def transactional(function):
        def invoke(transaction):
            with transaction.client.lock:
                return function(transaction)
        return invoke


class SharedHostTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.database = FirestoreMemory(self.clock)
        self.addCleanup(patch.stopall)
        patch('google.cloud.firestore.transactional',FirestoreMemory.transactional).start()
        self.store = FirestoreHostStore(self.database)
        self.coordinator = SharedHostCoordinator(self.store, clock=self.clock.now, sleep=self.clock.advance)
        self.other = SharedHostCoordinator(FirestoreHostStore(self.database), clock=self.clock.now, sleep=self.clock.advance)

    def document(self):
        return next(iter(self.database.documents.values()))

    def test_transaction_admits_only_one_owner_under_simultaneous_claims(self):
        replies = []
        barrier = threading.Barrier(8)

        def contender(index):
            barrier.wait()
            replies.append(self.store.acquire('example.org', str(index), 3, 180))

        threads = [threading.Thread(target=contender, args=(i,)) for i in range(8)]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        self.assertEqual(sum(reply.acquired for reply in replies), 1)

    def test_crashed_owner_expires_and_stale_release_cannot_release_replacement(self):
        first = self.coordinator.acquire('example.org')
        self.clock.advance(181)
        second = self.other.acquire('example.org')
        first.release()
        self.assertEqual(self.document()['owner'], second.owner)
        self.assertFalse(self.store.defer('example.org', first.owner, 3600))
        second.release()
        self.assertIsNone(self.document()['owner'])

    def test_old_owner_cannot_renew_after_expiry_even_before_replacement(self):
        first = self.coordinator.acquire('example.org')
        self.clock.advance(181)
        self.assertFalse(self.store.renew('example.org', first.owner, 180))
        with self.assertRaises(HostLeaseLost): first.check()

    def test_robots_delay_persists_across_containers_and_survives_release(self):
        with self.coordinator.acquire('example.org', 20):
            self.clock.advance(1)
        with self.other.acquire('example.org', 3):
            self.assertEqual(self.clock.now(), 120)
        self.assertEqual(self.document()['delay_seconds'], 20)

    def test_stricter_robots_delay_is_shared_even_if_its_request_cannot_yet_acquire(self):
        first=self.coordinator.acquire('example.org',3)
        denied=self.store.acquire('example.org','waiting',20,180)
        self.assertFalse(denied.acquired)
        self.assertEqual(self.document()['delay_seconds'],20)
        self.clock.advance(1);first.release()
        with self.other.acquire('example.org',3):
            self.assertEqual(self.clock.now(),120)

    def test_network_lock_and_spacing_use_firestore_time_not_local_clock(self):
        local = Clock()
        local.value = 999999
        coordinator = SharedHostCoordinator(self.store, clock=local.now,
            sleep=lambda seconds: (local.advance(seconds), self.clock.advance(seconds)))
        with coordinator.acquire('EXAMPLE.org.'):
            self.assertEqual(self.document()['last_started_at'], 100)
            self.assertEqual(self.document()['host'], 'example.org')
        with self.other.acquire('example.org'):
            self.assertEqual(self.clock.now(), 103)

    def test_shared_cooldown_is_not_shortened_or_bypassed_by_another_container(self):
        with self.coordinator.acquire('archive.org') as lease:
            lease.defer(3600)
            self.clock.advance(10)
            lease.defer(5)
        with self.assertRaises(SharedHostCooldown) as caught:
            self.other.acquire('archive.org')
        self.assertEqual(caught.exception.retry_after_seconds, 3590)
        with self.other.acquire('example.org'):
            self.assertEqual(self.clock.now(), 110)

    def test_cancellation_interrupts_polling_before_another_request_is_admitted(self):
        first = self.coordinator.acquire('example.org')
        cancelled = threading.Event()
        self.other.sleep = lambda _: cancelled.set()
        with self.assertRaises(HostLeaseLost):
            self.other.acquire('example.org', cancelled=cancelled.is_set)
        self.assertEqual(self.document()['owner'], first.owner)
        first.release()

    def test_cancellation_during_successful_claim_releases_that_claim(self):
        cancelled = threading.Event()
        real_acquire = self.store.acquire

        def acquire(*args):
            result = real_acquire(*args)
            cancelled.set()
            return result

        with patch.object(self.store, 'acquire', side_effect=acquire):
            with self.assertRaises(HostLeaseLost):
                self.coordinator.acquire('example.org', cancelled=cancelled.is_set)
        self.assertIsNone(self.document()['owner'])

    def test_renewal_failure_fails_closed_and_release_preserves_shared_cooldown(self):
        lease = self.coordinator.acquire('example.org')
        lease.defer(30)
        with patch.object(self.store, 'renew', side_effect=OSError('Firestore unavailable')):
            with self.assertRaises(OSError): lease.renew()
        with self.assertRaises(HostLeaseLost): lease.check()
        lease.release()
        self.assertEqual(self.document()['cooldown_until'], 130)

    def test_request_latency_that_exhausts_lease_never_admits_network(self):
        real_acquire = self.store.acquire

        def delayed(*args):
            result = real_acquire(*args)
            self.clock.advance(181)
            return result

        with patch.object(self.store, 'acquire', side_effect=delayed):
            with self.assertRaises(HostLeaseLost): self.coordinator.acquire('example.org')
        self.assertIsNone(self.document()['owner'])

    def test_fetchers_share_http_lock_spacing_and_retry_after(self):
        fetchers = [Fetcher(Bucket(), 'a', host_coordinator=self.coordinator),
                    Fetcher(Bucket(), 'b', host_coordinator=self.other)]
        starts, replies = [], [(200, {}), (429, {'Retry-After': '3600'})]
        owner = self

        class Session:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def get(self, url, **kwargs):
                starts.append(owner.clock.now())
                # The lease remains held while the request body is received.
                self_denied = owner.store.acquire('example.org', 'contender', 3, 180)
                owner.assertFalse(self_denied.acquired)
                owner.clock.advance(.5)
                status, headers = replies.pop(0)
                return nullcontext(SimpleNamespace(status_code=status, headers=headers,
                    iter_content=lambda _: [b'article']))

        with patch('crawl.requests.Session', Session), patch('crawl.public_url', side_effect=urlsplit), \
             patch('crawl.time.monotonic', side_effect=self.clock.now):
            fetchers[0].one('https://example.org/first')
            fetchers[1].one('https://example.org/second')
            with self.assertRaises(SharedHostCooldown):
                fetchers[0].one('https://example.org/third')
        self.assertEqual(starts, [100., 103.])
        self.assertEqual(fetchers[1].host_queue_wait_seconds(), 2.5)
        self.assertIsNone(self.document()['owner'])
        self.assertEqual(self.document()['cooldown_until'], 3703.5)

    def test_shared_store_failure_never_falls_back_to_uncoordinated_http(self):
        fetcher = Fetcher(Bucket(), 'a', host_coordinator=self.coordinator)
        with patch.object(self.store, 'acquire', side_effect=OSError('Firestore unavailable')), \
             patch('crawl.public_url', side_effect=urlsplit), patch('crawl.requests.Session') as session:
            with self.assertRaises(OSError): fetcher.one('https://example.org/story')
        session.assert_not_called()

    def test_lost_queue_ownership_stops_http_and_browser_authorization(self):
        fetcher = Pipeline(Bucket(), 'a', host_coordinator=self.coordinator)
        fetcher.abort_event.set()
        with patch('crawl.requests.Session') as session:
            with self.assertRaisesRegex(RuntimeError, 'ownership lost'):
                fetcher.one('https://example.org/story')
        session.assert_not_called()

        def renderer(url, authorize, **_):
            with self.assertRaisesRegex(RuntimeError, 'ownership lost'):
                authorize(url, True)
            return b'', url

        with patch('pipeline.render_isolated', side_effect=renderer):
            fetcher.render('https://example.org/story')

    def test_browser_document_starts_share_the_same_global_pacing(self):
        starts = []
        def renderer(url, authorize, **_):
            self.assertTrue(authorize(url, True))
            starts.append(self.clock.now())
            return b'<html></html>', url

        for coordinator in [self.coordinator, self.other]:
            pipeline = Pipeline(Bucket(), 'a', host_coordinator=coordinator)
            with patch('pipeline.render_isolated', side_effect=renderer), \
                 patch('pipeline.public_url', side_effect=urlsplit), \
                 patch.object(pipeline, 'policy', return_value=(None, True, 3, None)):
                pipeline.render('https://example.org/story')
        self.assertEqual(starts, [100., 103.])

    def test_browser_retry_after_extends_cooldown_without_releasing_an_active_http_owner(self):
        pipeline=Pipeline(Bucket(),'a',host_coordinator=self.coordinator)
        lease=self.other.acquire('example.org')

        def renderer(url,authorize,response,**_):
            response(url,503,'600')
            return b'',url

        with patch('pipeline.render_isolated',side_effect=renderer),patch('crawl.public_url',side_effect=urlsplit):
            pipeline.render('https://example.org/story')
        self.assertEqual(self.document()['owner'],lease.owner)
        self.assertEqual(self.document()['cooldown_until'],700)
        lease.release()
        with self.assertRaises(SharedHostCooldown):self.coordinator.acquire('example.org')

    def test_article_guard_expires_while_waiting_for_a_busy_host(self):
        first=self.other.acquire('example.org')
        pipeline=Pipeline(Bucket(),'a',host_coordinator=self.coordinator)
        def guard():
            if self.clock.now()>=105:raise RuntimeError('Article lease expired')
        pipeline.request_context.guard=guard
        with patch('crawl.public_url',side_effect=urlsplit),patch('crawl.requests.Session') as session:
            with self.assertRaisesRegex(RuntimeError,'Article lease expired'):
                pipeline.one('https://example.org/story')
        session.assert_not_called()
        self.assertEqual(self.document()['owner'],first.owner)
        first.release()

    def test_browser_callback_retains_article_guard_when_called_on_another_thread(self):
        pipeline=Pipeline(Bucket(),'a',host_coordinator=self.coordinator)
        def guard():raise RuntimeError('Article lease expired')
        pipeline.request_context.guard=guard
        failures=[]
        def renderer(url,authorize,**_):
            def other_thread():
                try:authorize(url,True)
                except Exception as exc:failures.append(str(exc))
            thread=threading.Thread(target=other_thread);thread.start();thread.join()
            return b'',url
        with patch('pipeline.render_isolated',side_effect=renderer):
            pipeline.render('https://example.org/story')
        self.assertEqual(failures,['Article lease expired'])


if __name__ == '__main__': unittest.main()
