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
from shared_hosts import (FirestoreHostStore, SharedHostCoordinator, SharedHostCooldown,
                         HostLeaseLost, acquire_state, Admission, publisher_in_cooldown)
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
                result.id = key
                return result
        return Collection()

    def get_all(self, references):
        for reference in references:
            data = copy.deepcopy(self.documents.get(reference.path))
            yield SimpleNamespace(id=reference.id, to_dict=lambda data=data: data,
                read_time=datetime.fromtimestamp(self.clock.now(), timezone.utc))

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

    def test_archive_first_requires_explicit_shared_cooldown(self):
        self.assertTrue(publisher_in_cooldown(Admission(False, 300, 300)))
        for state in (None, Admission(True), Admission(True, 0, 300),
                      Admission(False, 2), Admission(False, 180),
                      Admission(False, float('nan'), float('nan'))):
            self.assertFalse(publisher_in_cooldown(state))

    def test_archive_override_matches_admission_and_keeps_publisher_limit(self):
        coordinator = SharedHostCoordinator(self.store, clock=self.clock.now,
            sleep=self.clock.advance, max_concurrency=4, host_limits={'web.archive.org': 8})
        leases = []
        for host, limit in [('web.archive.org', 8), ('publisher.org', 4)]:
            for _ in range(limit):
                lease, state = coordinator.try_acquire(host, delay=1, robots_delay=0)
                self.assertTrue(state.acquired)
                leases.append(lease)
                self.clock.advance(1)
            self.assertFalse(coordinator.availability([host], delay=1)[host].acquired)
            self.assertIsNone(coordinator.try_acquire(host, delay=1)[0])
        for lease in leases:
            lease.release()
        self.assertTrue(all(s.acquired for s in coordinator.availability(
            ['web.archive.org', 'publisher.org'], delay=1).values()))

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

    def test_parallel_policy_admits_exact_capacity_under_simultaneous_claims(self):
        replies = []
        barrier = threading.Barrier(12)

        def contender(index):
            barrier.wait()
            replies.append(self.store.acquire('example.org', str(index), 0, 180, 4, 0))

        threads = [threading.Thread(target=contender, args=(i,)) for i in range(12)]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        self.assertEqual(sum(reply.acquired for reply in replies), 4)
        self.assertEqual(len(self.document()['leases']), 4)

    def test_start_spacing_allows_overlap_before_a_slow_response_finishes(self):
        coordinator = SharedHostCoordinator(self.store, max_concurrency=4,
            clock=self.clock.now, sleep=self.clock.advance)
        leases = []
        for index in range(4):
            lease, result = coordinator.try_acquire('example.org', delay=1, robots_delay=0)
            self.assertTrue(result.acquired)
            leases.append(lease)
            denied, result = coordinator.try_acquire('example.org', delay=1, robots_delay=0)
            self.assertIsNone(denied)
            self.assertGreater(result.wait_seconds, 0)
            self.clock.advance(1)
        self.assertEqual(len(self.document()['leases']), 4)
        # The first request is still downloading while three others start.
        leases[0].check()
        self.assertEqual(self.document()['last_started_at'], 103)
        denied, result = coordinator.try_acquire('example.org', delay=1, robots_delay=0)
        self.assertIsNone(denied)
        self.assertEqual(result.wait_seconds, 176)
        leases[1].release()
        replacement, result = coordinator.try_acquire('example.org', delay=1, robots_delay=0)
        self.assertTrue(result.acquired)
        self.assertEqual(len(self.document()['leases']), 4)
        for lease in leases + [replacement]: lease.release()

    def test_individual_renewal_release_expiry_and_stale_fencing(self):
        coordinator = SharedHostCoordinator(self.store, max_concurrency=4, lease_seconds=10,
            clock=self.clock.now, sleep=self.clock.advance)
        first = coordinator.acquire('example.org', delay=0, robots_delay=0)
        second = coordinator.acquire('example.org', delay=0, robots_delay=0)
        self.clock.advance(5)
        second.renew()
        self.clock.advance(6)
        self.assertFalse(self.store.renew('example.org', first.owner, 10))
        self.assertFalse(self.store.defer('example.org', first.owner, 1000))
        replacement = coordinator.acquire('example.org', delay=0, robots_delay=0)
        first.release()
        self.assertEqual(set(self.document()['leases']), {second.owner, replacement.owner})
        second.release()
        self.assertEqual(set(self.document()['leases']), {replacement.owner})
        replacement.check()
        replacement.release()
        self.assertEqual(self.document()['leases'], {})

    def test_one_owner_cooldown_blocks_new_peers_without_revoking_other_downloads(self):
        coordinator = SharedHostCoordinator(self.store, max_concurrency=4,
            clock=self.clock.now, sleep=self.clock.advance)
        first = coordinator.acquire('example.org', delay=0, robots_delay=0)
        second = coordinator.acquire('example.org', delay=0, robots_delay=0)
        first.defer(600)
        first.release()
        second.check()
        denied, result = coordinator.try_acquire('example.org', delay=0, robots_delay=0)
        self.assertIsNone(denied)
        self.assertEqual(result.cooldown_seconds, 600)
        self.assertEqual(set(self.document()['leases']), {second.owner})
        with self.assertRaises(SharedHostCooldown):
            coordinator.acquire('example.org', delay=0, robots_delay=0)
        second.release()

    def test_legacy_exclusive_owner_is_not_overlapped_during_migration(self):
        state = {'owner': 'old-worker', 'lease_until': 150., 'last_started_at': 100.,
                 'delay_seconds': 3., 'next_allowed_at': 103.}
        denied, changed = acquire_state(state, 104., 'new-worker', 'example.org', 1, 180, 4, 0)
        self.assertFalse(denied.acquired)
        self.assertEqual(denied.wait_seconds, 46.)
        self.assertNotIn('leases', changed)
        admitted, changed = acquire_state(changed, 151., 'new-worker', 'example.org', 1, 180, 4, 0)
        self.assertTrue(admitted.acquired)
        self.assertEqual(changed['leases'], {'new-worker': 331.})

    def test_late_legacy_writer_preserving_new_empty_map_is_still_exclusive(self):
        state = {'host_policy_version': 2, 'leases': {}, 'owner': 'late-old-worker',
                 'lease_until': 150., 'last_started_at': 100., 'delay_seconds': 3.}
        denied, changed = acquire_state(state, 104., 'new-worker', 'example.org', 1, 180, 4, 0)
        self.assertFalse(denied.acquired)
        self.assertEqual(denied.wait_seconds, 46.)
        admitted, changed = acquire_state(changed, 151., 'new-worker', 'example.org', 1, 180, 4, 0)
        self.assertTrue(admitted.acquired)
        self.assertEqual(changed['leases'], {'new-worker': 331.})

    def test_multi_owner_compatibility_gate_covers_latest_active_expiration(self):
        coordinator = SharedHostCoordinator(self.store, max_concurrency=4, lease_seconds=10,
            clock=self.clock.now, sleep=self.clock.advance)
        first = coordinator.acquire('example.org', delay=0, robots_delay=0)
        self.clock.advance(2)
        second = coordinator.acquire('example.org', delay=0, robots_delay=0)
        self.assertEqual(self.document()['owner'], first.owner)
        self.assertEqual(self.document()['lease_until'], 112)
        first.release()
        self.assertEqual(self.document()['owner'], second.owner)
        self.assertEqual(self.document()['lease_until'], 112)
        second.release()

    def test_fresh_robots_policy_can_lower_legacy_default_even_after_unknown_policy_fetch(self):
        legacy = {'owner': None, 'lease_until': 0., 'last_started_at': 100.,
                  'delay_seconds': 3., 'next_allowed_at': 103.}
        # A robots.txt request does not yet know the site's actual rule.
        _, state = acquire_state(legacy, 104., 'robots-fetch', 'example.org', 1, 180, 4)
        self.assertEqual(state['delay_seconds'], 3)
        self.assertEqual(state['robots_delay_seconds'], 0)
        admitted, state = acquire_state(state, 105., 'article', 'example.org', 1, 180, 4, 0)
        self.assertTrue(admitted.acquired)
        self.assertEqual(state['delay_seconds'], 1)
        self.assertEqual(len(state['leases']), 2)

    def test_actual_robots_policy_survives_lower_defaults_and_peer_unknown_policy(self):
        for legacy_delay in (0, 20):
            with self.subTest(legacy_delay=legacy_delay):
                state = {'delay_seconds': legacy_delay}
                admitted, state = acquire_state(state, 100., 'first', 'example.org', 1, 180, 4, 20)
                self.assertTrue(admitted.acquired)
                denied, state = acquire_state(state, 101., 'second', 'example.org', 1, 180, 4, 0)
                self.assertFalse(denied.acquired)
                self.assertEqual(denied.wait_seconds, 19)
                # The unchanged policy does not require another Firestore write.
                self.assertIsNone(state)

    def test_legacy_stricter_delay_survives_first_explicit_no_rule_request(self):
        admitted, state = acquire_state({'delay_seconds': 20}, 100., 'first', 'example.org', 1, 180, 4, 0)
        self.assertTrue(admitted.acquired)
        self.assertEqual(state['robots_delay_seconds'], 20)
        self.assertEqual(state['delay_seconds'], 20)

    def test_readiness_batches_hosts_uses_server_time_and_does_not_mutate_documents(self):
        coordinator = SharedHostCoordinator(self.store, max_concurrency=4,
            clock=lambda: 999999, sleep=self.clock.advance)
        self.store.acquire('busy.org', 'old', 1, 180, 4, 20)
        self.store.cooldown('cooldown.org', 600)
        before = copy.deepcopy(self.database.documents)
        ready = coordinator.availability(['BUSY.org.', 'cooldown.org', 'new.org'], delay=1,
                                          robots_delays={'busy.org': 0, 'new.org': 0})
        self.assertFalse(ready['busy.org'].acquired)
        self.assertEqual(ready['busy.org'].wait_seconds, 20)
        self.assertEqual(ready['cooldown.org'].cooldown_seconds, 600)
        self.assertTrue(ready['new.org'].acquired)
        self.assertEqual(self.database.documents, before)

    def test_advisory_readiness_cannot_bypass_a_new_concurrent_acquisition(self):
        ready = self.coordinator.availability(['example.org'])
        self.assertTrue(ready['example.org'].acquired)
        lease = self.other.acquire('example.org')
        candidate, denied = self.coordinator.try_acquire('example.org')
        self.assertIsNone(candidate)
        self.assertFalse(denied.acquired)
        lease.release()

    def test_lowered_capacity_waits_for_enough_existing_leases_to_expire(self):
        state = {'leases': {'one': 110., 'two': 120., 'three': 130., 'four': 140.},
                 'robots_delay_seconds': 0}
        denied, _ = acquire_state(state, 100., 'next', 'example.org', 0, 180, 2, 0)
        self.assertFalse(denied.acquired)
        self.assertEqual(denied.wait_seconds, 30.)

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
