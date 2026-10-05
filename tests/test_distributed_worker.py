import copy
import gzip
import json
import os
from pathlib import Path
import sys
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
import distributed_worker as distributed
from google.api_core.exceptions import NotFound, PreconditionFailed
from shared_queue import Claim


def worker_record(index, state, **extra):
    return {'task_index': str(index), 'task_attempt': 0, 'state': state,
            'updated_at': 100, 'execution': 'execution', 'expires_at': time.time() + 90, **extra}


def bare_run():
    run = distributed.DistributedRun.__new__(distributed.DistributedRun)
    run.config = {'workers': 1, 'per_outlet_workers': 4, 'phase': 'verification',
                  'dataset': 'test.crawl', 'source_table': 'test.articles',
                  'max_runtime_seconds': None, 'max_response_bytes': 1000, 'max_total_attempts': 1000}
    run.configured_workers = 1
    run.run_id = 'verify-distributed-test'
    run.country = 'ZZ'
    run.prefix = 'runs/' + run.run_id + '/'
    run.execution = 'execution'
    run.task_index = '0'
    run.task_attempt = 0
    run.task_count = 2
    run.verification = True
    run.active = {}
    run.claims = {}
    run.deadlines = {}
    run.claim_lock = threading.Lock()
    run.started = time.monotonic()
    run.last_heartbeat = time.monotonic()
    run.last_publish = time.monotonic()
    run.last_export = time.monotonic()
    run.summary_snapshot = None
    run.worker_state = 'running'
    run.queue = Mock()
    run.queue.control.return_value = {'state': 'ready', 'stop': False}
    run.bucket = Mock(name='bucket')
    run.bucket.name = 'private-test'
    run.bq = Mock()
    run.fetcher = SimpleNamespace(abort_event=threading.Event(), request_context=threading.local(),
                                  lock=threading.Lock(), live={}, fetch=Mock(), fetch_phase=Mock())
    run.recovery = SimpleNamespace(attempt_elapsed=0, reserve_restart=Mock(return_value=True))
    run.lease = Mock()
    run.lease.name = 'control/test-lease.json'
    run.lease.generation = 1
    run.lease.download_as_text.return_value = json.dumps({'execution': run.execution, 'run_id': run.run_id,
        'owner': run.execution, 'distributed': True,
        'expires_at': (distributed.datetime.now(distributed.timezone.utc) + distributed.timedelta(minutes=10)).isoformat()})
    default_blob = run.bucket.blob.return_value
    default_blob.exists.return_value = False
    run.bucket.blob.side_effect = lambda name: run.lease if name == 'control/test-lease.json' else default_blob
    return run


def claim():
    return Claim('abc123', 'claim-token', {'outlet': 'example.test', 'url': 'https://example.test/a'}, time.time() + 600)


class GenerationBlob:
    """Model GCS handles pinning their previously loaded object generation."""
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name
        self.generation = None

    def reload(self, **kwargs):
        current = self.bucket.current
        if not current or self.generation not in (None, current['generation']):
            raise NotFound('Cached generation no longer exists')
        self.generation = current['generation']

    def download_as_text(self, if_generation_match=None, **kwargs):
        if self.bucket.read_conflict:
            error = self.bucket.read_conflict
            self.bucket.read_conflict = None
            self.bucket.current['generation'] += 1
            raise error('Generation replaced between reload and download')
        wanted = self.generation if if_generation_match is None else if_generation_match
        if wanted is not None and wanted != self.bucket.current['generation']:
            raise PreconditionFailed('Generation changed')
        return self.bucket.current['payload']

    def upload_from_string(self, payload, if_generation_match, **kwargs):
        generation = self.bucket.current['generation'] if self.bucket.current else 0
        if if_generation_match != generation:
            raise PreconditionFailed('Generation changed')
        self.generation = generation + 1
        self.bucket.current = {'generation': self.generation, 'payload': payload}

    def delete(self, if_generation_match):
        if if_generation_match != self.bucket.current['generation']:
            raise PreconditionFailed('Generation changed')
        self.bucket.current = None


class GenerationBucket:
    def __init__(self):
        self.current = None
        self.read_conflict = None
        self.handles = []

    def blob(self, name):
        if not name.startswith('control/'):
            # STOP and retirement are separate, absent objects in these lease fixtures.
            missing = Mock()
            missing.exists.return_value = False
            missing.reload.side_effect = NotFound('Absent control marker')
            missing.download_as_text.side_effect = NotFound('Absent control marker')
            return missing
        blob = GenerationBlob(self, name)
        self.handles.append(blob)
        return blob


class AggregateTests(unittest.TestCase):
    def test_public_pointer_reads_fresh_progress_after_peer_replaces_uploaded_generation(self):
        run = bare_run()
        run.verification = False
        run.queue.summary.return_value = {'processed': 2, 'total': 10, 'domains': [],
                                           'downloading': 1, 'pending': 7, 'counts': {'saved': 2}}
        run.queue.list_workers.return_value = [worker_record(0, 'running'), worker_record(1, 'running')]
        original = Mock()
        original.generation = 4
        original.download_as_text.side_effect = NotFound('Generation replaced by a peer')
        fresh = Mock()
        fresh.download_as_text.return_value = json.dumps({'processed': 3, 'state': 'running',
                                                        'run_id': run.run_id, 'execution': run.execution})
        public = Mock()
        public.generation = 5
        reads = []
        marker = Mock()
        marker.exists.return_value = False
        def blob(path):
            if path == run.lease.name:
                return run.lease
            if '/retired-executions/' in path:
                return marker
            if path == 'progress/ZZ.json':
                return public
            reads.append(path)
            return original if len(reads) == 1 else fresh
        run.bucket.blob.side_effect = blob
        run.publish()
        original.download_as_text.assert_not_called()
        self.assertEqual(json.loads(public.upload_from_string.call_args.args[0])['processed'], 3)

    def test_completion_waits_for_every_task_and_outbox_export(self):
        complete = {'processed': 10, 'total': 10}
        self.assertEqual(distributed.aggregate_state(complete, [worker_record(0, 'completed')], 2), 'running')
        self.assertEqual(distributed.aggregate_state(complete, [worker_record(0, 'completed'), worker_record(1, 'exporting')], 2), 'running')
        self.assertEqual(distributed.aggregate_state(complete, [worker_record(0, 'completed'), worker_record(1, 'completed')], 2), 'completed')

    def test_memory_recovery_never_makes_cohort_terminal(self):
        records = [worker_record(0, 'continuing'), worker_record(1, 'recovering_memory', expires_at=0)]
        self.assertEqual(distributed.aggregate_state({'processed': 10, 'total': 10}, records, 2), 'running')

    def test_mixed_terminal_states_keep_failure_or_operator_stop(self):
        summary = {'processed': 9, 'total': 10}
        for state in ('failed', 'paused_by_operator', 'paused_limit', 'recovery_failed'):
            with self.subTest(state=state):
                self.assertEqual(distributed.aggregate_state(summary, [worker_record(0, 'continuing'), worker_record(1, state)], 2), state)
        self.assertEqual(distributed.aggregate_state(summary, [worker_record(0, 'continuing'), worker_record(1, 'completed')], 2), 'continuing')

    def test_replacement_attempt_outranks_late_old_heartbeat(self):
        records = [worker_record(0, 'failed', task_attempt=0, updated_at=1000),
                   worker_record(0, 'running', task_attempt=1, updated_at=500),
                   worker_record(3, 'completed')]
        self.assertEqual(distributed.latest_workers(records, 1), [records[1]])

    def test_publish_preserves_domain_pending_and_expires_individual_workers(self):
        run = bare_run()
        domains = [{'outlet': 'example.test', 'saved': 2, 'pending': 7, 'downloading': 1}]
        run.queue.summary.return_value = {'processed': 2, 'total': 10, 'domains': copy.deepcopy(domains),
                                           'downloading': 1, 'pending': 7, 'counts': {'saved': 2}}
        run.queue.list_workers.return_value = [worker_record(0, 'running'), worker_record(1, 'running', expires_at=0)]
        run.bucket.blob.return_value.generation = 4
        summary = run.publish()
        self.assertEqual(summary['domains'], domains)
        self.assertEqual(summary['active_instances'], 1)
        self.assertEqual(summary['pending'], 7)
        run.queue.list_workers.assert_called_once_with(execution='execution')

    def test_terminal_publish_retries_cas_before_releasing_cohort(self):
        run = bare_run()
        run.queue.summary.return_value = {'processed': 2, 'total': 10, 'domains': [],
                                           'downloading': 0, 'pending': 8, 'counts': {'saved': 2}}
        run.queue.list_workers.return_value = [worker_record(0, 'continuing'), worker_record(1, 'continuing')]
        target = run.bucket.blob.return_value
        target.generation = 4
        events = []
        def uploading(*args, **kwargs):
            events.append('upload')
            if len(events) == 1:
                target.generation = 5
                raise PreconditionFailed('A peer published its older running snapshot')
        target.upload_from_string.side_effect = uploading
        run.lease.download_as_text.return_value = json.dumps({'execution': run.execution,
            'run_id': run.run_id, 'owner': run.execution, 'distributed': True,
            'expires_at': (distributed.datetime.now(distributed.timezone.utc) + distributed.timedelta(minutes=10)).isoformat()})
        run.lease.delete.side_effect = lambda **kwargs: events.append('release')
        summary = run.publish()
        run.release_cohort_lease(summary)
        self.assertEqual(summary['state'], 'continuing')
        self.assertEqual(events, ['upload', 'upload', 'release'])
        self.assertEqual(run.queue.summary.call_count, 2)
        self.assertEqual(target.upload_from_string.call_args.kwargs['if_generation_match'], 5)

    def test_terminal_publish_fails_closed_after_bounded_cas_conflicts(self):
        run = bare_run()
        run.queue.summary.return_value = {'processed': 10, 'total': 10, 'domains': [],
                                           'downloading': 0, 'pending': 0, 'counts': {'saved': 10}}
        run.queue.list_workers.return_value = [worker_record(0, 'completed'), worker_record(1, 'completed')]
        target = run.bucket.blob.return_value
        target.generation = 4
        target.upload_from_string.side_effect = PreconditionFailed('Concurrent write')
        with self.assertRaisesRegex(RuntimeError, 'Could not persist terminal collection status'):
            run.release_cohort_lease(run.publish())
        self.assertEqual(target.upload_from_string.call_count, 5)
        run.lease.delete.assert_not_called()


class FencingTests(unittest.TestCase):
    def test_ten_tasks_share_fresh_guard_and_only_first_renews_near_expiry(self):
        bucket = GenerationBucket()
        runs = [bare_run() for _ in range(10)]
        for run in runs:
            run.bucket = bucket
            run.lease = bucket.blob('control/shared-test.json')
            run.task_count = 10
            run.last_heartbeat = 0
            run.lease_update(initial=True)
            run.last_heartbeat = 1
        self.assertEqual(bucket.current['generation'], 1)
        previous = json.loads(bucket.current['payload'])
        previous['expires_at'] = (distributed.datetime.now(distributed.timezone.utc) + distributed.timedelta(minutes=4)).isoformat()
        bucket.current['payload'] = json.dumps(previous)
        for run in runs:
            run.lease_update()
        self.assertEqual(bucket.current['generation'], 2)

    def test_peer_renewals_and_release_use_fresh_generation_handles(self):
        bucket = GenerationBucket()
        first, second = bare_run(), bare_run()
        for run in (first, second):
            run.bucket = bucket
            run.lease = bucket.blob('control/shared-test.json')
            run.last_heartbeat = 0
        first.lease_update(initial=True)
        second.lease_update(initial=True)
        first.last_heartbeat = second.last_heartbeat = 1
        first.lease_update()
        second.lease_update()
        # One shared guard is sufficient; the other tasks do not rewrite it.
        self.assertEqual(bucket.current['generation'], 1)
        self.assertEqual(len(bucket.handles), 6)
        first.queue.list_workers.return_value = [worker_record(0, 'continuing'), worker_record(1, 'continuing')]
        first.release_cohort_lease({'state': 'continuing', 'workers': [worker_record(0, 'continuing'), worker_record(1, 'continuing')]})
        self.assertIsNone(bucket.current)

    def test_reload_download_race_retries_404_and_precondition_failures(self):
        for error in (NotFound, PreconditionFailed):
            with self.subTest(error=error):
                bucket = GenerationBucket()
                run = bare_run()
                run.bucket = bucket
                run.lease = bucket.blob('control/shared-test.json')
                run.last_heartbeat = 0
                run.lease_update(initial=True)
                run.last_heartbeat = 1
                bucket.read_conflict = error
                with patch.object(distributed.time, 'sleep'):
                    run.lease_update()
                self.assertEqual(bucket.current['generation'], 2)

    def test_failed_claim_renewal_stops_request_admissions(self):
        run = bare_run()
        run.lease_update = Mock()
        run.queue.heartbeat.side_effect = RuntimeError('Firestore unavailable')
        with self.assertRaisesRegex(RuntimeError, 'Firestore unavailable'):
            run.heartbeat('running')
        self.assertTrue(run.fetcher.abort_event.is_set())

    def test_lost_claim_stops_request_admissions(self):
        run = bare_run()
        run.lease_update = Mock()
        run.queue.heartbeat.return_value = {'abc123'}
        with self.assertRaisesRegex(RuntimeError, 'ownership lost'):
            run.heartbeat('running')
        self.assertTrue(run.fetcher.abort_event.is_set())

    def test_stale_completion_is_not_treated_as_success(self):
        run = bare_run()
        item = claim()
        run.claims[item.article_id] = item
        run.queue.complete.return_value = False
        with self.assertRaisesRegex(RuntimeError, 'completion rejected'):
            run.persist_result(item, {'article_id': item.article_id, 'status': 'saved'})
        self.assertIn(item.article_id, run.claims)
        self.assertTrue(run.fetcher.abort_event.is_set())

    def test_aborted_pipeline_failure_remains_unfinished(self):
        run = bare_run()
        run.verification = False
        item = claim()
        run.deadlines[item.article_id] = time.monotonic() + 500
        def aborted(*args, **kwargs):
            run.fetcher.abort_event.set()
            return {'status': 'failed'}
        run.fetcher.fetch_phase.side_effect = aborted
        with self.assertRaisesRegex(RuntimeError, 'lost queue ownership'):
            run.fetch_claim(item)

    def test_claim_outputs_are_scoped_and_context_cleared_after_failure(self):
        run = bare_run()
        run.verification = False
        item = claim()
        run.deadlines[item.article_id] = time.monotonic() + 500
        seen = []
        def fetching(*args, **kwargs):
            seen.append(run.fetcher.request_context.output_scope)
            raise RuntimeError('interrupted')
        run.fetcher.fetch_phase.side_effect = fetching
        with self.assertRaisesRegex(RuntimeError, 'interrupted'):
            run.fetch_claim(item)
        self.assertEqual(seen, [item.article_id + '/' + item.token])
        self.assertIsNone(run.fetcher.request_context.output_scope)
        self.assertIsNone(run.fetcher.request_context.guard)

    def test_cohort_release_requires_exact_owner_and_run(self):
        run = bare_run()
        summary = {'state': 'continuing', 'workers': [worker_record(0, 'continuing'), worker_record(1, 'continuing')]}
        run.queue.list_workers.return_value = summary['workers']
        for value in ({'execution': run.execution, 'run_id': 'other', 'owner': run.execution, 'distributed': True},
                      {'execution': run.execution, 'run_id': run.run_id, 'owner': 'other', 'distributed': True}):
            run.lease.download_as_text.return_value = json.dumps(value)
            run.release_cohort_lease(summary)
            run.lease.delete.assert_not_called()
        run.lease.download_as_text.return_value = json.dumps({'execution': run.execution, 'run_id': run.run_id, 'owner': run.execution, 'distributed': True})
        run.lease.generation = 4
        run.release_cohort_lease(summary)
        run.lease.delete.assert_called_once_with(if_generation_match=4)

    def test_missing_established_cohort_lease_stops_renewal(self):
        run = bare_run()
        run.lease.reload.side_effect = NotFound('gone')
        with self.assertRaisesRegex(RuntimeError, 'disappeared'):
            run.lease_update()
        run.lease.upload_from_string.assert_not_called()

    def test_publish_error_aborts_before_executor_drains_and_releases_work(self):
        run = bare_run()
        run.task_count = 1
        run.verification = False
        run.last_publish = 0
        run.lease_update = Mock()
        run.heartbeat = Mock()
        run.queue.control.return_value = {'state': 'ready', 'stop': False}
        run.bucket.blob.return_value.exists.return_value = False
        item = claim()
        run.queue.claim.side_effect = [[item]]
        saw_abort = threading.Event()
        def fetching(_):
            if run.fetcher.abort_event.wait(5):
                saw_abort.set()
                return {'status': 'failed'}
            raise RuntimeError('Executor waited without cancelling request admissions')
        run.fetch_claim = fetching
        def publishing(*args):
            if run.active:
                raise RuntimeError('status persistence unavailable')
            return {'processed': 0, 'total': 1, 'response_bytes': 0, 'attempts': 0}
        run.publish = publishing
        with patch.object(distributed, 'memory_usage', return_value={}), patch.object(distributed.worker, 'STOP', False):
            with self.assertRaisesRegex(RuntimeError, 'status persistence unavailable'):
                run.run()
        self.assertTrue(saw_abort.is_set())
        run.queue.complete.assert_not_called()
        run.queue.release.assert_called_once_with(item)


class OutboxTests(unittest.TestCase):
    def prepare(self):
        run = bare_run()
        run.heartbeat = Mock()
        evidence = {'article_id': 'abc123', 'run_id': run.run_id, 'outlet': 'example.test', 'status': 'saved', 'attempts': []}
        record = {'article_id': 'abc123', 'claim_token': 'token',
                  'result_uri': 'gs://private-test/' + run.prefix + 'distributed/results/abc123/token.json.gz'}
        run.queue.export_pending.return_value = [record]
        run.bucket.blob.return_value.download_as_bytes.return_value = gzip.compress(json.dumps(evidence).encode())
        return run, record

    def prepare_many(self, count):
        run = bare_run()
        run.heartbeat = Mock()
        records = []
        evidence = {}
        for index in range(count):
            article_id = 'article-' + str(index)
            path = run.prefix + 'distributed/results/' + article_id + '/token.json.gz'
            records.append({'article_id': article_id, 'claim_token': 'token',
                            'result_uri': 'gs://private-test/' + path})
            evidence[path] = gzip.compress(json.dumps({'article_id': article_id, 'run_id': run.run_id,
                'outlet': 'example.test', 'status': 'saved', 'attempts': []}).encode())
        def blob(path):
            result = Mock()
            result.download_as_bytes.return_value = evidence[path]
            return result
        run.bucket.blob.side_effect = blob
        run.queue.export_pending.return_value = records
        return run, records, evidence

    def test_failed_bigquery_load_keeps_durable_outbox(self):
        run, record = self.prepare()
        run.bq.load_table_from_file.return_value.result.side_effect = RuntimeError('BQ unavailable')
        with self.assertRaisesRegex(RuntimeError, 'BQ unavailable'):
            run.export_results()
        run.queue.mark_exported.assert_not_called()

    def test_export_acknowledges_only_after_successful_load(self):
        run, record = self.prepare()
        run.export_results()
        run.bq.load_table_from_file.return_value.result.assert_called_once_with(timeout=90)
        run.queue.mark_exported.assert_called_once_with([record])
        self.assertGreaterEqual(run.heartbeat.call_count, 2)
        run.queue.export_pending.assert_called_once_with(limit=500)

    def test_prefetched_records_beyond_byte_limit_remain_unacknowledged(self):
        run, records, _ = self.prepare_many(12)
        # JsonlBatch permits one oversized record, then requires a new load.
        with patch.object(distributed, 'JsonlBatch', return_value=distributed.JsonlBatch(500, 1)):
            run.export_results()
        run.queue.mark_exported.assert_called_once_with(records[:1])
        self.assertLessEqual(run.bucket.blob.call_count, 8)

    def test_parallel_reads_are_bounded_to_eight_and_heartbeat_between_chunks(self):
        run, records, evidence = self.prepare_many(9)
        lock = threading.Lock()
        barrier = threading.Barrier(8)
        reads = [0]
        active = [0]
        peak = [0]
        heartbeat_read_counts = []
        def blob(path):
            result = Mock()
            def download(**kwargs):
                index = int(path.split('/article-', 1)[1].split('/', 1)[0])
                with lock:
                    active[0] += 1
                    peak[0] = max(peak[0], active[0])
                if index < 8:
                    barrier.wait(timeout=5)
                with lock:
                    reads[0] += 1
                    active[0] -= 1
                return evidence[path]
            result.download_as_bytes.side_effect = download
            return result
        run.bucket.blob.side_effect = blob
        run.last_heartbeat = 0
        run.heartbeat.side_effect = lambda state: heartbeat_read_counts.append(reads[0])
        with patch.object(distributed.time, 'monotonic', return_value=100):
            run.export_results()
        self.assertEqual(peak[0], 8)
        self.assertEqual(heartbeat_read_counts, [0, 8, 9, 9])
        run.queue.mark_exported.assert_called_once_with(records)

    def test_empty_outbox_resets_poll_interval(self):
        run, _ = self.prepare()
        run.queue.export_pending.return_value = []
        run.last_export = 0
        with patch.object(distributed.time, 'monotonic', return_value=100):
            run.export_results()
        self.assertEqual(run.last_export, 100)
        run.bq.load_table_from_file.assert_not_called()

    def test_foreign_result_uri_is_rejected_before_reading_or_loading(self):
        run, record = self.prepare()
        record['result_uri'] = 'gs://other-bucket/' + run.prefix + 'distributed/results/abc123/token.json.gz'
        with self.assertRaisesRegex(RuntimeError, 'Unexpected result evidence'):
            run.export_results()
        run.bucket.blob.assert_not_called()
        run.bq.load_table_from_file.assert_not_called()


if __name__ == '__main__':
    unittest.main()
