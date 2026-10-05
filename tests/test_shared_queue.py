import copy
import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from result_store import ResultIndex
from shared_queue import SharedQueue, article_key, SHARDS


class Snapshot:
    def __init__(self, reference, value=None, version=0):
        self.reference = reference
        self.id = reference.id
        self.exists = value is not None
        self._value = copy.deepcopy(value)
        self.update_time = version

    def to_dict(self):
        return copy.deepcopy(self._value)


class Reference:
    def __init__(self, database, path):
        self.database = database
        self.path = path
        self.id = path.rsplit('/', 1)[-1]

    def collection(self, name):
        return Query(self.database, self.path + '/' + name)

    def get(self, transaction=None, **kwargs):
        with self.database.lock:
            return Snapshot(self, self.database.data.get(self.path), self.database.versions.get(self.path, 0))

    def set(self, value, **kwargs):
        with self.database.lock:
            self.database.data[self.path] = copy.deepcopy(value)
            self.database.versions[self.path] = self.database.versions.get(self.path, 0) + 1

    def update(self, value, **kwargs):
        with self.database.lock:
            self.database.data[self.path].update(copy.deepcopy(value))
            self.database.versions[self.path] += 1


class Query:
    def __init__(self, database, path, filters=None, ordering=None, maximum=None, cursor=None):
        self.database = database
        self.path = path
        self.filters = filters or []
        self.ordering = ordering
        self.maximum = maximum
        self.cursor = cursor

    def document(self, key):
        return Reference(self.database, self.path + '/' + key)

    def clone(self, **values):
        args = dict(filters=self.filters, ordering=self.ordering, maximum=self.maximum, cursor=self.cursor)
        args.update(values)
        return Query(self.database, self.path, **args)

    def where(self, filter):
        return self.clone(filters=self.filters + [filter])

    def order_by(self, field):
        return self.clone(ordering=field)

    def limit(self, value):
        return self.clone(maximum=value)

    def start_after(self, snapshot):
        return self.clone(cursor=snapshot)

    def stream(self, **kwargs):
        with self.database.lock:
            self.database.query_count += 1
            records = []
            for path, value in self.database.data.items():
                if path.rsplit('/', 1)[0] != self.path:
                    continue
                keep = True
                for condition in self.filters:
                    if condition.field_path not in value:
                        keep = False
                        break
                    actual = value[condition.field_path]
                    if condition.op_string == '==':
                        matches = actual == condition.value
                    elif condition.op_string == '<=':
                        matches = actual <= condition.value
                    elif condition.op_string == '>':
                        matches = actual > condition.value
                    else:
                        raise AssertionError(condition.op_string)
                    keep &= matches
                if keep:
                    records.append(Snapshot(Reference(self.database, path), value, self.database.versions[path]))
            def key(snapshot):
                if self.ordering:
                    return (snapshot.to_dict()[self.ordering], snapshot.id)
                return snapshot.id
            records.sort(key=key)
            if self.cursor is not None:
                records = [snapshot for snapshot in records if key(snapshot) > key(self.cursor)]
            if self.maximum is not None:
                records = records[:self.maximum]
            self.database.query_results += len(records)
            return iter(records)


class Batch:
    def __init__(self, database):
        self.database = database
        self.operations = []

    def set(self, reference, value):
        self.operations.append(('set', reference, copy.deepcopy(value), None))

    def update(self, reference, value, option=None):
        self.operations.append(('update', reference, copy.deepcopy(value), option))

    def commit(self, **kwargs):
        with self.database.lock:
            if self.database.fail_next_batch:
                self.database.fail_next_batch = False
                raise RuntimeError('Simulated batch interruption')
            for operation, reference, value, option in self.operations:
                if option is not None and self.database.versions.get(reference.path) != option:
                    raise RuntimeError('Precondition failed')
            for operation, reference, value, option in self.operations:
                getattr(reference, operation)(value)
            self.operations.clear()


class Database:
    def __init__(self):
        self.data = {}
        self.versions = {}
        self.lock = threading.RLock()
        self.query_count = 0
        self.query_results = 0
        self.fail_next_batch = False
        self.get_all_calls = 0

    def collection(self, name):
        return Query(self, name)

    def transaction(self):
        return Batch(self)

    def batch(self):
        return Batch(self)

    def write_option(self, last_update_time):
        return last_update_time

    def get_all(self, references, **kwargs):
        self.get_all_calls += 1
        # Firestore does not guarantee get_all returns the requested ordering.
        return [reference.get(**kwargs) for reference in reversed(references)]


def transactional(function):
    def wrapped(transaction):
        with transaction.database.lock:
            result = function(transaction)
            transaction.commit()
            return result
    return wrapped


def article(number, outlet='news.ke'):
    return {'url': f'https://{outlet}/article/{number}', 'outlet': outlet,
            'first_observed': '2025-01-01', 'source_table': 'project.dataset.articles'}


def result(item, status='saved'):
    return {'article_id': article_key(item['url']), 'outlet': item['outlet'],
            'status': status, 'updated_at': '2026-10-05T12:00:00+00:00',
            'response_bytes': 100, 'stored_bytes': 50,
            'attempts': [{'http_attempts': [{}, {}, {}]}]}


class SharedQueueTests(unittest.TestCase):
    def setUp(self):
        self.patch = patch('shared_queue.firestore.transactional', transactional)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.database = Database()
        self.now = 1000.0
        self.queue = self.make_queue('worker-a')

    def make_queue(self, owner):
        return SharedQueue(self.database, 'test-run', owner, clock=lambda: self.now)

    def seed(self, items, done=None):
        config = {'input_count': len(items), 'country': 'KE', 'phase': 'full',
                  'source_table': 'project.dataset.articles', 'max_response_bytes': 1000000,
                  'max_total_attempts': 1000}
        return self.queue.initialize(items, done or ResultIndex(), config)

    def test_import_preserves_terminal_counts_and_never_requeues_saved_urls(self):
        items = [article(i, 'one.ke' if i < 3 else 'two.ke') for i in range(5)]
        done = ResultIndex()
        done.record(result(items[0]))
        done.record(result(items[1], 'partial'))
        done.record(result(items[4], 'deferred'))
        self.seed(items, done)
        summary = self.queue.summary()
        self.assertEqual((summary['total'], summary['processed'], summary['pending']), (5, 2, 3))
        self.assertEqual(summary['counts'], {'saved': 1, 'partial': 1})
        self.assertEqual(summary['attempts'], 6)
        self.assertEqual(summary['retries'], 4)
        self.assertEqual(summary['response_bytes'], 200)
        claimed = self.queue.claim(limit=5)
        self.assertEqual(len(claimed), 3)
        self.assertNotIn(article_key(items[0]['url']), {claim.article_id for claim in claimed})
        self.assertEqual(self.queue.export_pending(), [])

    def test_initialization_is_idempotent_after_ready_and_resumes_partial_seed(self):
        items = [article(i) for i in range(3)]
        with patch.object(self.queue, '_seed_batch', side_effect=RuntimeError('Simulated batch interruption')):
            with self.assertRaisesRegex(RuntimeError, 'batch interruption'):
                self.seed(items)
        self.assertEqual(self.queue.control()['state'], 'initializing')
        self.assertEqual(self.queue.claim(), [])
        self.seed(items)
        claimed = self.queue.claim(limit=1)[0]
        self.assertTrue(self.queue.complete(claimed, result(claimed.item), 'gs://bucket/result.json'))
        self.seed(items)
        self.assertEqual(self.queue.summary()['processed'], 1)
        self.assertEqual(len(self.queue.claim(limit=3)), 2)

    def test_duplicate_manifest_and_input_count_mismatch_block_activation(self):
        item = article(0)
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            self.seed([item, item])
        with self.assertRaisesRegex(RuntimeError, 'not been initialized'):
            self.queue.control()

    def test_manifest_identity_cannot_change_during_or_after_initialization(self):
        original = [article(0)]
        with patch.object(self.queue, '_seed_batch', side_effect=RuntimeError('interruption')):
            with self.assertRaises(RuntimeError):
                self.seed(original)
        with self.assertRaisesRegex(ValueError, 'does not match'):
            self.seed([article(1)])
        self.seed(original)
        with self.assertRaisesRegex(ValueError, 'does not match'):
            self.seed([article(2)])
        self.assertEqual(self.queue.summary()['total'], 1)

    def test_parallel_claimers_never_receive_same_live_article(self):
        items = [article(i, f'outlet-{i % 20}.ke') for i in range(200)]
        self.seed(items)
        queues = [self.make_queue(f'worker-{i}') for i in range(4)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            batches = list(pool.map(lambda queue: queue.claim(48), queues))
        identities = [claim.article_id for batch in batches for claim in batch]
        self.assertEqual(len(identities), 192)
        self.assertEqual(len(set(identities)), 192)
        self.assertTrue(all(len(batch) == 48 for batch in batches))

    def test_expired_lease_is_reclaimed_and_stale_owner_cannot_complete_or_release(self):
        self.seed([article(0)])
        original = self.queue.claim(1)[0]
        other = self.make_queue('worker-b')
        self.assertEqual(other.claim(1), [])
        self.now += 601
        replacement = other.claim(1)[0]
        self.assertNotEqual(original.token, replacement.token)
        self.assertFalse(self.queue.complete(original, result(original.item), 'gs://bucket/stale.json'))
        self.assertFalse(self.queue.release(original))
        self.assertEqual(self.queue.heartbeat([original]), {original.article_id})
        self.assertTrue(other.complete(replacement, result(replacement.item), 'gs://bucket/new.json'))
        self.assertEqual(self.queue.summary()['processed'], 1)

    def test_heartbeat_extends_all_claims_atomically_and_never_resurrects_expired_claim(self):
        self.seed([article(i, f'outlet-{i}.ke') for i in range(48)])
        claims = self.queue.claim(48)
        self.now += 500
        lost = self.queue.heartbeat(claims, {'state': 'running', 'active_stages': [{'stage': 'http'}]})
        self.assertEqual(lost, set())
        self.assertTrue(all(claim.lease_until == self.now + 600 for claim in claims))
        workers = self.queue.list_workers()
        self.assertEqual(len(workers), 1)
        self.assertEqual(len(workers[0]['active']), 48)
        self.assertEqual(workers[0]['active_stages'], [{'stage': 'http'}])
        self.now += 601
        lost = self.queue.heartbeat(claims)
        self.assertEqual(len(lost), 48)
        self.assertTrue(all(not self.queue.owns(claim) for claim in claims))

    def test_fenced_completion_and_stats_are_idempotent_and_require_evidence(self):
        self.seed([article(0)])
        claim = self.queue.claim(1)[0]
        with self.assertRaisesRegex(ValueError, 'durable'):
            self.queue.complete(claim, result(claim.item), '')
        with self.assertRaisesRegex(ValueError, 'terminal'):
            self.queue.complete(claim, result(claim.item, 'deferred'), 'gs://bucket/first.json')
        self.assertTrue(self.queue.complete(claim, result(claim.item), 'gs://bucket/first.json'))
        self.assertTrue(self.queue.complete(claim, result(claim.item), 'gs://bucket/first.json'))
        summary = self.queue.summary()
        self.assertEqual((summary['processed'], summary['counts'], summary['attempts']), (1, {'saved': 1}, 3))
        stored = self.queue.articles.document(claim.article_id).get().to_dict()
        self.assertNotIn('due_at', stored)
        self.assertEqual(self.make_queue('worker-b').claim(1), [])

    def test_outbox_survives_export_failure_and_acknowledges_only_exact_result(self):
        self.seed([article(0)])
        claim = self.queue.claim(1)[0]
        self.queue.complete(claim, result(claim.item), 'gs://bucket/result.json')
        records = self.queue.export_pending()
        self.assertEqual(len(records), 1)
        wrong = [{**records[0], 'result_uri': 'gs://bucket/other.json'}]
        self.assertEqual(self.queue.mark_exported(wrong), set())
        self.assertEqual(self.queue.export_pending(), records)
        self.assertEqual(self.queue.mark_exported(records), {claim.article_id})
        self.assertEqual(self.queue.export_pending(), [])
        self.assertEqual(self.queue.mark_exported(records), {claim.article_id})
        self.assertEqual(self.queue.summary()['processed'], 1)

    def test_fresh_and_expired_worker_counts_cannot_mark_pending_work_done(self):
        self.seed([article(i) for i in range(4)])
        claims = self.queue.claim(2)
        self.queue.heartbeat(claims)
        summary = self.queue.summary()
        self.assertEqual((summary['processed'], summary['pending'], summary['downloading']), (0, 2, 2))
        self.queue.complete(claims[0], result(claims[0].item), 'gs://bucket/first.json')
        # A stale heartbeat can only overstate the bounded in-flight count;
        # completed rows remain authoritative, and pending never goes negative.
        self.now += 91
        summary = self.queue.summary()
        self.assertEqual((summary['processed'], summary['pending'], summary['downloading']), (1, 3, 0))
        self.assertEqual(len(self.queue.list_workers(include_expired=True)), 1)
        self.assertEqual(self.queue.list_workers(include_expired=False), [])
        self.assertEqual(self.queue.list_workers(execution='other'), [])

    def test_stop_control_prevents_new_claims_but_allows_checkpointed_completion(self):
        self.seed([article(i) for i in range(2)])
        claim = self.queue.claim(1)[0]
        self.queue.set_control(stop=True, state='paused_by_operator')
        self.assertEqual(self.queue.claim(1), [])
        self.assertTrue(self.queue.complete(claim, result(claim.item), 'gs://bucket/completed.json'))
        self.assertEqual(self.queue.summary()['processed'], 1)

    def test_bounded_claim_query_and_per_instance_outlet_cap(self):
        self.seed([article(i, 'heavy.ke' if i < 990 else f'rare-{i}.ke') for i in range(1000)])
        self.database.query_results = 0
        claimed = self.queue.claim(8, per_outlet=2, inflight={'heavy.ke': 1})
        self.assertLessEqual(sum(claim.item['outlet'] == 'heavy.ke' for claim in claimed), 1)
        self.assertLessEqual(self.database.query_results, 2 * 48)
        # Further calls rotate the cursor, allowing rare outlets beyond the first page.
        seen = {claim.article_id for claim in claimed}
        for _ in range(12):
            seen.update(claim.article_id for claim in self.queue.claim(8, excluded_outlets={'heavy.ke'}))
        self.assertGreater(len(seen), 5)
        self.database.query_count = 0
        self.queue.summary()
        self.assertEqual(self.database.query_count, 1)  # worker heartbeats only

    def test_release_preserves_unfinished_work_for_another_instance(self):
        self.seed([article(0)])
        claim = self.queue.claim(1)[0]
        self.assertTrue(self.queue.release(claim))
        replacement = self.make_queue('worker-b').claim(1)[0]
        self.assertEqual(replacement.article_id, claim.article_id)
        self.assertNotEqual(replacement.token, claim.token)
        self.assertFalse(self.queue.release(claim))


if __name__ == '__main__':
    unittest.main()
