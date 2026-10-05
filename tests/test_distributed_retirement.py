import copy
import json
import unittest
from unittest.mock import Mock, patch

from google.api_core.exceptions import NotFound, PreconditionFailed
from test_distributed_worker import bare_run, worker_record
import distributed_worker as distributed


class Blob:
    def __init__(self, bucket, name):
        self.bucket, self.name, self.generation = bucket, name, None

    def exists(self, **kwargs):
        if self.name in self.bucket.errors:
            raise self.bucket.errors[self.name]
        return self.name in self.bucket.objects

    def reload(self, **kwargs):
        if not self.exists():
            raise NotFound(self.name)
        self.generation = self.bucket.objects[self.name][0]

    def download_as_text(self, if_generation_match=None, **kwargs):
        self.reload()
        if if_generation_match is not None and self.generation != if_generation_match:
            raise PreconditionFailed(self.name)
        return json.dumps(self.bucket.objects[self.name][1])

    def upload_from_string(self, data, if_generation_match=None, **kwargs):
        generation = self.bucket.objects.get(self.name, (0, None))[0]
        if if_generation_match is not None and if_generation_match != generation:
            raise PreconditionFailed(self.name)
        self.generation = generation + 1
        self.bucket.objects[self.name] = (self.generation, json.loads(data))
        self.bucket.writes.append(self.name)
        if self.bucket.on_write:
            self.bucket.on_write(self.name)

    def delete(self, if_generation_match, **kwargs):
        self.reload()
        if if_generation_match != self.generation:
            raise PreconditionFailed(self.name)
        self.bucket.writes.append('delete:' + self.name)
        del self.bucket.objects[self.name]


class Bucket:
    name = 'private-test'

    def __init__(self):
        self.objects, self.errors, self.writes = {}, {}, []
        self.on_write = None

    def blob(self, name):
        return Blob(self, name)

    def seed(self, name, value):
        generation = self.objects.get(name, (0, None))[0] + 1
        self.objects[name] = (generation, copy.deepcopy(value))


def fixture():
    run = bare_run()
    run.verification = False
    run.bucket = Bucket()
    run.lease = run.bucket.blob('control/worker-lease.json')
    guard = {'run_id': run.run_id, 'execution': run.execution, 'owner': run.execution,
             'distributed': True, 'task_count': 2,
             'expires_at': (distributed.datetime.now(distributed.timezone.utc) + distributed.timedelta(minutes=10)).isoformat()}
    run.bucket.seed(run.lease.name, guard)
    summary = {'total': 10, 'processed': 2, 'pending': 8, 'downloading': 0,
               'domains': [], 'counts': {'saved': 2}, 'response_bytes': 100, 'attempts': 2}
    run.queue.summary.return_value = copy.deepcopy(summary)
    run.queue.list_workers.return_value = [worker_record(0, 'running'), worker_record(1, 'running')]
    return run, guard


def terminal(run):
    return {'run_id': run.run_id, 'execution': run.execution, 'state': 'completed',
            'total': 10, 'processed': 10, 'pending': 0, 'downloading': 0,
            'counts': {'saved': 10}, 'response_bytes': 100, 'attempts': 2,
            'workers': [worker_record(0, 'completed'), worker_record(1, 'completed')]}


class RetirementTests(unittest.TestCase):
    def test_stop_at_startup_does_not_acquire_or_publish(self):
        run, _ = fixture()
        run.bucket.seed(run.prefix + 'STOP', {})
        run.lease_update = Mock()
        self.assertEqual(run.run(), 'paused_by_operator')
        run.lease_update.assert_not_called()
        run.queue.claim.assert_not_called()
        run.queue.heartbeat_worker.assert_not_called()
        run.queue.summary.assert_not_called()
        self.assertEqual(run.bucket.writes, [])

    def test_firestore_stop_at_startup_also_prevents_acquisition(self):
        run, _ = fixture()
        run.queue.control.return_value = {'state': 'ready', 'stop': True}
        run.lease_update = Mock()
        self.assertEqual(run.run(), 'paused_by_operator')
        run.lease_update.assert_not_called()
        run.queue.claim.assert_not_called()
        self.assertEqual(run.bucket.writes, [])

    def test_retired_startup_returns_without_guard_claims_or_status_mutation(self):
        run, _ = fixture()
        run.bucket.seed(run.prefix + 'retired-executions/' + run.execution + '.json', {'reason': 'replaced'})
        run.lease_update = Mock()
        self.assertEqual(run.run(), 'retired')
        run.lease_update.assert_not_called()
        run.queue.control.assert_not_called()
        run.queue.heartbeat_worker.assert_not_called()
        self.assertEqual(run.bucket.writes, [])

    def test_retirement_blocks_renewal_and_direct_initial_claim(self):
        for initial in (False, True):
            with self.subTest(initial=initial):
                run, _ = fixture()
                run.bucket.seed(run.prefix + 'retired-executions/' + run.execution + '.json', {})
                with self.assertRaisesRegex(RuntimeError, 'retired'):
                    run.lease_update(initial=initial)
                self.assertTrue(run.fetcher.abort_event.is_set())
                self.assertEqual(run.bucket.writes, [])

    def test_direct_initial_acquisition_also_rechecks_stop(self):
        run, _ = fixture()
        run.bucket.seed(run.prefix + 'STOP', {})
        with self.assertRaisesRegex(RuntimeError, 'paused before startup'):
            run.lease_update(initial=True)
        self.assertEqual(run.bucket.writes, [])

    def test_marker_read_failure_fails_closed(self):
        run, _ = fixture()
        path = run.prefix + 'retired-executions/' + run.execution + '.json'
        run.bucket.errors[path] = RuntimeError('Storage unavailable')
        with self.assertRaisesRegex(RuntimeError, 'Storage unavailable'):
            run.run()
        self.assertTrue(run.fetcher.abort_event.is_set())
        run.queue.claim.assert_not_called()
        run.queue.heartbeat_worker.assert_not_called()
        self.assertEqual(run.bucket.writes, [])


class PublicationOwnershipTests(unittest.TestCase):
    def test_nonowner_expired_and_missing_guards_cannot_publish(self):
        for mode in ('other', 'expired', 'missing'):
            with self.subTest(mode=mode):
                run, guard = fixture()
                run.bucket.seed('progress/ZZ.json', {'execution': 'replacement', 'state': 'running'})
                if mode == 'missing':
                    del run.bucket.objects[run.lease.name]
                else:
                    if mode == 'other':
                        guard.update(execution='replacement', owner='replacement')
                    else:
                        guard['expires_at'] = '2000-01-01T00:00:00+00:00'
                    run.bucket.seed(run.lease.name, guard)
                with self.assertRaises(Exception):
                    run.publish('old failure')
                run.queue.summary.assert_not_called()
                self.assertEqual(run.bucket.writes, [])
                self.assertEqual(run.bucket.objects['progress/ZZ.json'][1]['execution'], 'replacement')

    def test_retired_execution_cannot_publish_even_if_guard_still_matches(self):
        run, _ = fixture()
        run.bucket.seed(run.prefix + 'retired-executions/' + run.execution + '.json', {})
        with self.assertRaisesRegex(RuntimeError, 'retired'):
            run.publish()
        run.queue.summary.assert_not_called()
        self.assertEqual(run.bucket.writes, [])

    def test_ownership_is_rechecked_after_expensive_counter_reads(self):
        run, guard = fixture()
        original = run.queue.summary.return_value
        def lose_ownership():
            run.bucket.seed(run.lease.name, {**guard, 'execution': 'replacement', 'owner': 'replacement'})
            return original
        run.queue.summary.side_effect = lose_ownership
        with self.assertRaisesRegex(RuntimeError, 'ownership was lost'):
            run.publish()
        self.assertEqual(run.bucket.writes, [])
        self.assertTrue(run.fetcher.abort_event.is_set())

    def test_losing_ownership_after_private_write_cannot_overwrite_replacement_public_status(self):
        run, guard = fixture()
        def replace(name):
            if name == run.prefix + 'progress.json':
                run.bucket.seed(run.lease.name, {**guard, 'execution': 'replacement', 'owner': 'replacement'})
                run.bucket.seed('progress/ZZ.json', {'execution': 'replacement', 'state': 'running'})
        run.bucket.on_write = replace
        with self.assertRaisesRegex(RuntimeError, 'ownership was lost'):
            run.publish()
        self.assertEqual(run.bucket.writes, [run.prefix + 'progress.json'])
        self.assertEqual(run.bucket.objects['progress/ZZ.json'][1]['execution'], 'replacement')

    def test_fresh_owned_guard_allows_private_and_public_publication(self):
        run, _ = fixture()
        summary = run.publish()
        self.assertEqual(summary['execution'], run.execution)
        self.assertEqual(run.bucket.writes, [run.prefix + 'progress.json', 'progress/ZZ.json'])
        self.assertFalse(run.fetcher.abort_event.is_set())

    def test_terminal_peer_release_is_read_only_and_does_not_create_failed_heartbeat(self):
        run, _ = fixture()
        snapshot = terminal(run)
        run.bucket.seed(run.prefix + 'progress.json', snapshot)
        del run.bucket.objects[run.lease.name]
        # A peer released the guard after it durably published all task outcomes.
        run.lease_update = Mock()
        run.heartbeat = Mock()
        run.queue.claim.return_value = []
        run.queue.export_pending.return_value = []
        with patch.object(distributed, 'memory_usage', return_value={}), patch.object(distributed.worker, 'STOP', False):
            self.assertEqual(run.run(), 'completed')
        run.queue.heartbeat_worker.assert_not_called()
        self.assertEqual(run.bucket.writes, [])
        self.assertFalse(run.fetcher.abort_event.is_set())

    def test_terminal_fallback_never_accepts_other_execution_or_new_lease_owner(self):
        for mode in ('wrong-execution', 'new-owner', 'retired', 'bad-counts'):
            with self.subTest(mode=mode):
                run, guard = fixture()
                snapshot = terminal(run)
                if mode == 'wrong-execution':
                    snapshot['execution'] = 'other'
                if mode == 'bad-counts':
                    snapshot['pending'] = 3
                if mode == 'new-owner':
                    run.bucket.seed(run.lease.name, {**guard, 'execution': 'replacement', 'owner': 'replacement'})
                else:
                    del run.bucket.objects[run.lease.name]
                if mode == 'retired':
                    run.bucket.seed(run.prefix + 'retired-executions/' + run.execution + '.json', {})
                run.bucket.seed(run.prefix + 'progress.json', snapshot)
                with self.assertRaises(RuntimeError):
                    run.publish()
                self.assertEqual(run.bucket.writes, [])

    def test_failed_old_attempt_cleanup_cannot_publish_or_heartbeat_after_handover(self):
        run, guard = fixture()
        run.bucket.seed(run.lease.name, {**guard, 'execution': 'replacement', 'owner': 'replacement'})
        with self.assertRaisesRegex(RuntimeError, 'Another crawl execution'):
            run.run()
        run.queue.heartbeat_worker.assert_not_called()
        self.assertEqual(run.bucket.writes, [])


if __name__ == '__main__':
    unittest.main()
