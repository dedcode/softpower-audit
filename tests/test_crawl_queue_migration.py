import copy
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from crawl_queue_migration import migrate_paused_publisher_retries
from result_store import ResultIndex
from shared_queue import SharedQueue, article_key, SHARDS
from test_shared_queue import Database, article, transactional


class PausedQueueMigrationTests(unittest.TestCase):
    def setUp(self):
        self.patch = patch('shared_queue.firestore.transactional', transactional)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.database = Database()
        self.old = 'softpower-crawler-old'
        self.reason = 'Owned repair of archive service retries'
        self.queue = SharedQueue(self.database, 'test-run', self.old + '-0-0-' + 'a' * 32, clock=lambda: 1000)
        self.items = [article(i) for i in range(4)]
        self.queue.initialize(self.items, ResultIndex(), {'input_count': 4, 'country': 'KE', 'phase': 'full',
                                                        'source_table': 'project.dataset.articles'})
        self.queue.set_control(stop=True, stop_reason=self.reason)
        self.replacements = []
        for item in self.items:
            aid = article_key(item['url'])
            prefix = 'gs://private-bucket/runs/test-run/distributed/checkpoints/' + aid + '/'
            self.replacements.append({'article_id': aid, 'expected_checkpoint_uri': prefix + 'old-token.json.gz',
                                      'new_checkpoint_uri': prefix + 'migration-token.json.gz'})
            row = self.queue.articles.document(aid).get().to_dict()
            row.update(phase='publisher', checkpoint_uri=prefix + 'old-token.json.gz',
                       accounted_metrics={'attempts': 12, 'retries': 3, 'response_bytes': 45, 'stored_bytes': 22},
                       result_uri='gs://private-bucket/preserved-result.json', needs_export=False,
                       status='queued', last_handoff={'old': 'stale publisher handoff'})
            self.queue.articles.document(aid).set(row)

    def run_migration(self, replacements=None, **kwargs):
        return migrate_paused_publisher_retries(self.queue, self.reason, self.old,
                                                replacements or self.replacements, now=1000, **kwargs)

    def row(self, index):
        return self.queue.articles.document(self.replacements[index]['article_id'])

    def stats(self):
        return {key: copy.deepcopy(value) for key, value in self.database.data.items() if '/stats/' in key}

    def test_exact_owned_pause_moves_ready_and_retired_leases_preserving_accounting_and_evidence(self):
        row = self.row(1).get().to_dict()
        row.update(state='leased', owner=self.old + '-0-0-' + 'b' * 32, claim_token='old-claim',
                   lease_until=2000, due_at=2000)
        self.row(1).set(row)
        foreign = self.row(2).get().to_dict()
        foreign.update(state='leased', owner='replacement-execution-0-0-live', claim_token='foreign', due_at=2000)
        self.row(2).set(foreign)
        done = self.row(3).get().to_dict()
        done.update(state='done', status='saved')
        self.row(3).set(done)
        before = copy.deepcopy(self.database.data)
        report = self.run_migration()
        self.assertEqual(report['migrated'], [entry['article_id'] for entry in self.replacements[:2]])
        self.assertEqual(report['skipped'], {self.replacements[2]['article_id']: 'foreign_owner',
                                           self.replacements[3]['article_id']: 'done'})
        for index in range(2):
            after = self.row(index).get().to_dict()
            original = before[self.row(index).path]
            self.assertEqual((after['state'], after['phase'], after['archive_due_at']), ('ready', 'archive', 1000))
            self.assertEqual(after['checkpoint_uri'], self.replacements[index]['new_checkpoint_uri'])
            for field in ('item', 'outlet', 'accounted_metrics', 'result_uri', 'needs_export'):
                self.assertEqual(after[field], original[field])
            for field in ('due_at', 'browser_due_at', 'owner', 'claim_token', 'lease_until', 'last_handoff'):
                self.assertNotIn(field, after)
            self.assertEqual(after['last_phase_migration'], {
                'old_checkpoint_uri': self.replacements[index]['expected_checkpoint_uri'],
                'new_checkpoint_uri': self.replacements[index]['new_checkpoint_uri'],
                'old_execution': self.old, 'reason': self.reason, 'at': 1000})
        self.assertEqual(self.row(2).get().to_dict(), foreign)
        self.assertEqual(self.row(3).get().to_dict(), done)
        for path, stats in self.stats().items():
            original = before[path]
            count = sum(int(entry['article_id'][:8], 16) % SHARDS == int(path.rsplit('/', 1)[-1])
                        for entry in self.replacements[:2])
            self.assertEqual(stats, {**original, 'archive_remaining': original['archive_remaining'] + count})
        self.assertEqual(self.database.data[self.queue.run_ref.path], before[self.queue.run_ref.path])

    def test_repeated_operator_call_is_idempotent_and_uses_one_bulk_read(self):
        report = self.run_migration()
        self.assertEqual(len(report['migrated']), 4)
        before = copy.deepcopy(self.database.data)
        self.database.get_all_calls = 0
        report = self.run_migration()
        self.assertEqual(report['migrated'], [])
        self.assertEqual(len(report['already_migrated']), 4)
        self.assertEqual(self.database.data, before)
        self.assertEqual(self.database.get_all_calls, 1)

    def test_ready_future_retry_is_preserved_but_retired_future_lease_is_cleared(self):
        self.row(0).update({'due_at': 2000})
        self.row(1).update({'state': 'leased', 'due_at': 3000, 'lease_until': 3000,
                            'owner': self.old + '-0-0-retired', 'claim_token': 'retired-token'})
        report = self.run_migration(self.replacements[:2])
        self.assertEqual(len(report['migrated']), 2)
        self.assertEqual(self.row(0).get().to_dict()['archive_due_at'], 2000)
        retired = self.row(1).get().to_dict()
        self.assertEqual(retired['archive_due_at'], 1000)
        for field in ('due_at', 'lease_until', 'owner', 'claim_token', 'last_handoff'):
            self.assertNotIn(field, retired)

    def test_changed_stop_or_control_identity_blocks_every_write(self):
        original = self.queue.run_ref.get().to_dict()
        for changes in ({'stop': False}, {'stop_reason': 'another operation'}, {'run_id': 'another-run'},
                        {'execution': 'replacement-execution'}):
            with self.subTest(changes=changes):
                self.queue.run_ref.set({**original, **changes})
                before = copy.deepcopy(self.database.data)
                with self.assertRaisesRegex(RuntimeError, 'exact owned stopped'):
                    self.run_migration()
                self.assertEqual(self.database.data, before)

    def test_changed_checkpoint_wrong_phase_and_unrecognized_owner_are_skipped(self):
        cases = [(0, {'checkpoint_uri': self.replacements[0]['new_checkpoint_uri']}, 'checkpoint_changed'),
                 (1, {'phase': 'browser'}, 'phase_changed'),
                 (2, {'state': 'leased', 'owner': 'foreign-expired', 'due_at': 0}, 'foreign_owner'),
                 (3, {'state': 'ready', 'owner': 'foreign-live', 'due_at': 2000}, 'foreign_owner')]
        for index, changes, _ in cases:
            self.row(index).update(changes)
        before = copy.deepcopy(self.database.data)
        report = self.run_migration()
        self.assertEqual(report['migrated'], [])
        self.assertEqual(report['skipped'], {self.replacements[index]['article_id']: reason for index, _, reason in cases})
        self.assertEqual(self.database.data, before)

    def test_missing_counter_aborts_atomically_before_article_writes(self):
        aid = self.replacements[0]['article_id']
        shard = f'{int(aid[:8], 16) % SHARDS:02d}'
        del self.database.data[self.queue.stats.document(shard).path]
        before = copy.deepcopy(self.database.data)
        with self.assertRaisesRegex(RuntimeError, 'Counter shard is missing'):
            self.run_migration()
        self.assertEqual(self.database.data, before)

    def test_all_control_article_and_counter_reads_finish_before_any_write(self):
        original = self.database.get_all
        reads_finished = []
        def bulk(references, **kwargs):
            self.assertIn(self.queue.run_ref, references)
            self.assertTrue(any('/stats/' in ref.path for ref in references))
            self.assertTrue(any('/articles/' in ref.path for ref in references))
            records = original(references, **kwargs)
            reads_finished.append(True)
            return records
        transaction_factory = self.database.transaction
        def checked_transaction():
            transaction = transaction_factory()
            original_set = transaction.set
            def set_after_reads(reference, value):
                self.assertTrue(reads_finished)
                return original_set(reference, value)
            transaction.set = set_after_reads
            return transaction
        with patch.object(self.database, 'get_all', side_effect=bulk), \
             patch.object(self.database, 'transaction', side_effect=checked_transaction):
            report = self.run_migration()
        self.assertEqual(len(report['migrated']), 4)
        self.assertEqual(len(reads_finished), 1)

    def test_invalid_foreign_or_ambiguous_evidence_is_rejected_before_transaction(self):
        entry = self.replacements[0]
        bad = [dict(entry, expected_checkpoint_uri=entry['expected_checkpoint_uri'].replace('test-run', 'foreign-run')),
               dict(entry, new_checkpoint_uri=entry['new_checkpoint_uri'].replace(entry['article_id'], 'a' * 64)),
               dict(entry, new_checkpoint_uri=entry['new_checkpoint_uri'].replace('private-bucket', 'foreign-bucket')),
               dict(entry, new_checkpoint_uri=entry['expected_checkpoint_uri']),
               dict(entry, new_checkpoint_uri=entry['new_checkpoint_uri'] + '?generation=1'),
               dict(entry, new_checkpoint_uri=entry['new_checkpoint_uri'].replace('migration-token', '../escape'))]
        before = copy.deepcopy(self.database.data)
        for replacement in bad:
            with self.subTest(replacement=replacement), self.assertRaises(ValueError):
                self.run_migration([replacement])
        self.assertEqual(self.database.data, before)
        self.assertEqual(self.database.get_all_calls, 0)

    def test_batch_bounds_and_duplicate_article_ids_are_rejected(self):
        for replacements in ([], self.replacements * 13, [self.replacements[0], self.replacements[0]]):
            with self.subTest(size=len(replacements)), self.assertRaises(ValueError):
                migrate_paused_publisher_retries(self.queue, self.reason, self.old, replacements, now=1000)
        self.assertEqual(self.database.get_all_calls, 0)


if __name__ == '__main__':
    unittest.main()
