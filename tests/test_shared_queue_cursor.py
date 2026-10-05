"""Dispersed initial scans retain bounded reads and whole-queue coverage."""
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from google.auth.credentials import AnonymousCredentials
from google.cloud import firestore

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from shared_queue import SharedQueue, article_key
from result_store import ResultIndex
import test_shared_queue as fake


class InitialCursorTests(unittest.TestCase):
    def setUp(self):
        self.database = fake.Database()
        self.now = 1000.0
        self.addCleanup(patch.stopall)
        patch('shared_queue.firestore.transactional', fake.transactional).start()

    def queue(self, index=None, count=10):
        return SharedQueue(self.database, 'test-run', 'worker-' + str(index), clock=lambda: self.now,
                           start_partition=None if index is None else (index, count))

    def seed(self, items):
        self.queue().initialize(items, ResultIndex(), {'input_count': len(items), 'country': 'KE'})

    def test_later_task_reaches_unsaturated_outlets_on_first_bounded_scan(self):
        items = [fake.article(i) for i in range(1200)]
        for index, item in enumerate(items):
            item['outlet'] = 'saturated.ke' if article_key(item['url']) < 'c' * 64 else f'available-{index}.ke'
        self.seed(items)
        before = self.database.query_count, self.database.query_results
        self.assertEqual(self.queue().claim(8, excluded_outlets={'saturated.ke'}), [])
        self.assertEqual(self.database.query_count - before[0], 2)
        self.assertLessEqual(self.database.query_results - before[1], 96)
        before = self.database.query_count, self.database.query_results
        claims = self.queue(8).claim(8, excluded_outlets={'saturated.ke'})
        self.assertEqual(len(claims), 8)
        self.assertEqual(len({claim.item['outlet'] for claim in claims}), 8)
        self.assertEqual(self.database.query_count - before[0], 1)
        self.assertLessEqual(self.database.query_results - before[1], 48)

    def test_ten_initial_cursors_are_in_distinct_strata_without_phase_restrictions(self):
        points = []
        for index in range(10):
            queue = self.queue(index)
            cursor = queue._cursors['publisher']
            point = int(cursor['__name__'].id, 16)
            self.assertEqual(len(cursor['__name__'].id), 64)
            self.assertEqual(cursor['due_at'], 0.0)
            self.assertGreaterEqual(point * 10, index * 2**256)
            self.assertLess(point * 10, (index + 1) * 2**256)
            self.assertIsNone(queue._cursors['browser'])
            self.assertIsNone(queue._cursors['archive'])
            points.append(point)
        self.assertEqual(len(set(points)), 10)

    def test_wrap_eventually_claims_every_lower_and_upper_id_including_retries(self):
        items = [fake.article(i, f'outlet-{i}.ke') for i in range(173)]
        self.seed(items)
        queue = self.queue(8)
        boundary = queue._cursors['publisher']['__name__'].id
        # Positive due dates (expired leases/retry scheduling) must also remain
        # reachable after the initial jump through the due_at=0 records.
        delayed_id = article_key(items[0]['url'])
        queue.articles.document(delayed_id).update({'due_at': self.now - 10})
        observed = set()
        expected = {article_key(item['url']) for item in items}
        for _ in range(len(items) + 2):
            queries_before = self.database.query_count
            rows_before = self.database.query_results
            claims = queue.claim(7)
            self.assertLessEqual(self.database.query_count - queries_before, 2)
            self.assertLessEqual(self.database.query_results - rows_before, 96)
            for claim in claims:
                self.assertNotIn(claim.article_id, observed)
                self.assertTrue(queue.complete(claim, fake.result(claim.item), 'gs://bucket/' + claim.article_id))
                observed.add(claim.article_id)
            if observed == expected:
                break
        self.assertTrue(any(aid < boundary for aid in observed))
        self.assertTrue(any(aid > boundary for aid in observed))
        self.assertEqual(observed, expected)
        self.assertEqual(queue.summary()['processed'], len(items))

    def test_future_work_is_not_admitted_by_jump_and_becomes_available_when_due(self):
        item = fake.article(1)
        self.seed([item])
        queue = self.queue(9)
        queue.articles.document(article_key(item['url'])).update({'due_at': self.now + 10})
        self.assertEqual(queue.claim(1), [])
        self.now += 10
        self.assertEqual([claim.article_id for claim in queue.claim(1)], [article_key(item['url'])])

    def test_omitting_partition_keeps_existing_first_page_behavior(self):
        items = [fake.article(i, f'outlet-{i}.ke') for i in range(100)]
        self.seed(items)
        queue = self.queue()
        self.assertEqual(queue._cursors, {'publisher': None, 'browser': None, 'archive': None})
        claim = queue.claim(1)[0]
        first_page = sorted(article_key(item['url']) for item in items)[:48]
        self.assertIn(claim.article_id, first_page)

    def test_invalid_partition_fails_before_admission(self):
        for partition in ((0, 0), (-1, 10), (10, 10), (True, 10), ('1', 10), (1,), '1,10'):
            with self.subTest(partition=partition), self.assertRaises(ValueError):
                SharedQueue(self.database, 'run', 'owner', start_partition=partition)

    def test_real_firestore_sdk_encodes_document_reference_cursor(self):
        # Uses real SDK query serialization, with anonymous credentials and no
        # transport call, so the fake cannot conceal a malformed start_after.
        client = firestore.Client(project='test-project', credentials=AnonymousCredentials())
        queue = SharedQueue(client, 'test-run', 'worker-5', start_partition=(5, 10))
        query = (queue.articles.where(filter=firestore.FieldFilter('due_at', '<=', self.now))
                 .order_by('due_at').order_by('__name__').limit(48)
                 .start_after(queue._cursors['publisher']))
        wire = query._to_protobuf()
        self.assertEqual([order.field.field_path for order in wire.order_by], ['due_at', '__name__'])
        self.assertEqual(wire.start_at.values[0].double_value, 0)
        self.assertTrue(wire.start_at.values[1].reference_value.endswith(
            '/crawl_runs/test-run/articles/' + queue._cursors['publisher']['__name__'].id))
        self.assertFalse(wire.start_at.before)


if __name__ == '__main__':
    unittest.main()
