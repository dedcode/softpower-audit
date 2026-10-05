import copy
import gzip
import io
import json
from pathlib import Path
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import recover_crawl_for_migration as recovery
from google.api_core.exceptions import NotFound, PreconditionFailed
from result_store import ResultIndex

RUN = 'example-run'
EXECUTION = 'softpower-crawler-old'
WORKFLOW = 'old-workflow-id'
OWNER = 'old-worker-owner'


def terminal_resources():
    execution = {'name': 'projects/citygraph/locations/us-central1/jobs/softpower-crawler/executions/' + EXECUTION,
                 'completionTime': '2026-10-05T11:00:00Z', 'runningCount': 0,
                 'conditions': [{'type': 'Completed', 'state': 'CONDITION_FAILED'}],
                 'template': {'containers': [{'env': [{'name': 'RUN_ID', 'value': RUN}]}]}}
    workflow = {'state': 'FAILED', 'endTime': '2026-10-05T11:01:00Z', 'argument': json.dumps({'run_id': RUN})}
    return execution, workflow


class Blob:
    def __init__(self, bucket, name, generation=None):
        self.bucket = bucket
        self.name = name
        self.generation = generation

    def exists(self):
        return self.name in self.bucket.objects

    def reload(self):
        if not self.exists():
            raise NotFound(self.name)
        self.generation = self.bucket.objects[self.name][0]

    def download_as_bytes(self, if_generation_match=None, **kwargs):
        if not self.exists():
            raise NotFound(self.name)
        generation, data = self.bucket.objects[self.name]
        if if_generation_match is not None and if_generation_match != generation:
            raise PreconditionFailed(self.name)
        return data

    def download_as_text(self, **kwargs):
        return self.download_as_bytes(**kwargs).decode()

    def open(self, mode):
        return io.BytesIO(self.download_as_bytes())

    def upload_from_string(self, data, if_generation_match=None, **kwargs):
        current = self.bucket.objects.get(self.name, (0, b''))[0]
        if if_generation_match is not None and if_generation_match != current:
            raise PreconditionFailed(self.name)
        self.generation = current + 1
        self.bucket.objects[self.name] = (self.generation, data.encode() if isinstance(data, str) else data)
        self.bucket.events.append(('write', self.name))

    def delete(self, if_generation_match, **kwargs):
        if self.bucket.objects[self.name][0] != if_generation_match:
            raise PreconditionFailed(self.name)
        self.bucket.events.append(('delete', self.name))
        del self.bucket.objects[self.name]


class Bucket:
    name = 'test-bucket'

    def __init__(self):
        self.objects = {}
        self.events = []

    def blob(self, name, generation=None):
        return Blob(self, name, generation)

    def list_blobs(self, prefix):
        return [self.blob(name, generation=value[0]) for name, value in self.objects.items() if name.startswith(prefix)]

    def add(self, name, value):
        data = value if isinstance(value, bytes) else json.dumps(value).encode()
        self.objects[name] = (1, data)


def fixture():
    bucket = Bucket()
    prefix = 'runs/' + RUN + '/'
    rows = [{'url': 'https://example.test/a', 'outlet': 'example.test'},
            {'url': 'https://example.test/b', 'outlet': 'example.test'}]
    config = {'run_id': RUN, 'country': 'KE', 'phase': 'full', 'input_count': 2, 'dataset': 'test.crawl'}
    previous = {'run_id': RUN, 'execution': EXECUTION, 'state': 'interrupted', 'processed': 2,
                'pending': 0, 'downloading': 1, 'active_stages': [{'stage': 'archive'}]}
    result = {**rows[0], 'article_id': recovery.key(rows[0]['url']), 'run_id': RUN, 'country': 'KE',
              'updated_at': '2026-10-05T10:00:00Z', 'status': 'saved', 'attempts': [], 'response_bytes': 10, 'stored_bytes': 5}
    bucket.add(prefix + 'STOP', b'')
    bucket.add(prefix + 'config.json', config)
    bucket.add(prefix + 'inputs.json.gz', gzip.compress(json.dumps(rows).encode()))
    bucket.add(prefix + 'progress.json', previous)
    bucket.add('progress/KE.json', previous)
    bucket.add(prefix + 'checkpoints/one.jsonl.gz', gzip.compress((json.dumps(result) + '\n').encode()))
    bucket.add('control/worker-lease.json', {'run_id': RUN, 'execution': EXECUTION, 'owner': OWNER})
    args = SimpleNamespace(run_id=RUN, execution=EXECUTION, workflow_execution=WORKFLOW,
                           lease_owner=OWNER, expected_count=2)
    session = Mock()
    execution, workflow = terminal_resources()
    def get(url, **kwargs):
        value = execution if 'run.googleapis.com' in url else workflow
        response = Mock()
        response.json.return_value = copy.deepcopy(value)
        return response
    session.get.side_effect = get
    bq = Mock()
    job = bq.load_table_from_file.return_value
    job.job_id = 'replay-job-1'
    job.result.side_effect = lambda **kwargs: bucket.events.append(('bq_replay_complete', 'replay-job-1'))
    return bucket, bq, session, args, config, previous, result


class GuardTests(unittest.TestCase):
    def test_cancel_request_is_not_terminal(self):
        execution, workflow = terminal_resources()
        execution.pop('completionTime')
        with self.assertRaisesRegex(RuntimeError, 'not terminal'):
            recovery.validate_terminal(execution, workflow, RUN, EXECUTION)

    def test_active_workflow_and_wrong_collection_are_rejected(self):
        execution, workflow = terminal_resources()
        workflow['state'] = 'ACTIVE'
        with self.assertRaisesRegex(RuntimeError, 'not terminal'):
            recovery.validate_terminal(execution, workflow, RUN, EXECUTION)
        execution, workflow = terminal_resources()
        workflow['argument'] = json.dumps({'run_id': 'different-run'})
        with self.assertRaisesRegex(RuntimeError, 'different collection'):
            recovery.validate_terminal(execution, workflow, RUN, EXECUTION)

    def test_only_exact_legacy_owner_can_be_released(self):
        for lease in ({'run_id': RUN, 'execution': EXECUTION, 'owner': 'new-owner'},
                      {'run_id': RUN, 'execution': 'new-execution', 'owner': OWNER},
                      {'run_id': RUN, 'execution': EXECUTION, 'owner': OWNER, 'distributed': True}):
            with self.subTest(lease=lease), self.assertRaisesRegex(RuntimeError, 'exact cancelled legacy worker'):
                recovery.validate_lease(lease, RUN, EXECUTION, OWNER)

    def test_missing_stop_blocks_all_mutations(self):
        bucket, bq, session, args, *_ = fixture()
        del bucket.objects['runs/' + RUN + '/STOP']
        with self.assertRaisesRegex(RuntimeError, 'STOP must remain'):
            recovery.recover(bucket, bq, session, args)
        self.assertFalse(bucket.events)
        bq.load_table_from_file.assert_not_called()


class RecoveryTests(unittest.TestCase):
    def test_durable_checkpoint_count_replaces_stale_progress_only_after_bq_replay(self):
        bucket, bq, session, args, config, *_ = fixture()
        report = recovery.recover(bucket, bq, session, args)
        self.assertEqual(report['processed'], 1)
        self.assertEqual(report['pending'], 1)
        snapshot, _ = recovery.read_json(bucket, 'runs/' + RUN + '/progress.json')
        self.assertEqual(snapshot['state'], 'stopped_for_migration')
        self.assertEqual(snapshot['downloading'], 0)
        self.assertEqual(snapshot['counts'], {'saved': 1})
        self.assertEqual(snapshot['domains'], [{'outlet': 'example.test', 'saved': 1, 'pending': 1}])
        replay_index = bucket.events.index(('bq_replay_complete', 'replay-job-1'))
        progress_index = bucket.events.index(('write', 'runs/' + RUN + '/progress.json'))
        delete_index = bucket.events.index(('delete', 'control/worker-lease.json'))
        self.assertLess(replay_index, progress_index)
        self.assertLess(progress_index, delete_index)
        recovery.validate_import_proof(bucket, snapshot, config)

    def test_failed_bq_replay_keeps_old_snapshot_and_lease(self):
        bucket, bq, session, args, _, previous, _ = fixture()
        bq.load_table_from_file.return_value.result.side_effect = RuntimeError('BQ unavailable')
        with self.assertRaisesRegex(RuntimeError, 'BQ unavailable'):
            recovery.recover(bucket, bq, session, args)
        self.assertEqual(recovery.read_json(bucket, 'runs/' + RUN + '/progress.json')[0], previous)
        self.assertIn('control/worker-lease.json', bucket.objects)

    def test_changed_checkpoint_inventory_blocks_snapshot_and_lease_mutations(self):
        bucket, bq, session, args, _, previous, result = fixture()
        def changed(**kwargs):
            bucket.add('runs/' + RUN + '/checkpoints/new.jsonl.gz', gzip.compress(json.dumps(result).encode()))
        bq.load_table_from_file.return_value.result.side_effect = changed
        with self.assertRaisesRegex(RuntimeError, 'Checkpoint inventory changed'):
            recovery.recover(bucket, bq, session, args)
        self.assertEqual(recovery.read_json(bucket, 'runs/' + RUN + '/progress.json')[0], previous)
        self.assertIn('control/worker-lease.json', bucket.objects)

    def test_replacement_lease_is_never_deleted(self):
        bucket, bq, session, args, *_ = fixture()
        def replaced(**kwargs):
            bucket.add('control/worker-lease.json', {'run_id': RUN, 'execution': 'new-execution', 'owner': 'new-owner'})
        bq.load_table_from_json.return_value.result.side_effect = replaced
        with self.assertRaisesRegex(RuntimeError, 'exact cancelled legacy worker'):
            recovery.recover(bucket, bq, session, args)
        self.assertEqual(recovery.read_json(bucket, 'control/worker-lease.json')[0]['owner'], 'new-owner')
        self.assertNotIn(('delete', 'control/worker-lease.json'), bucket.events)

    def test_import_requires_finalized_matching_recovery_proof(self):
        bucket, bq, session, args, config, *_ = fixture()
        recovery.recover(bucket, bq, session, args)
        snapshot, _ = recovery.read_json(bucket, 'runs/' + RUN + '/progress.json')
        snapshot['processed'] += 1
        with self.assertRaisesRegex(RuntimeError, 'does not match'):
            recovery.validate_import_proof(bucket, snapshot, config)

    def test_import_rechecks_stop_and_source_generations(self):
        bucket, bq, session, args, config, *_ = fixture()
        recovery.recover(bucket, bq, session, args)
        snapshot, _ = recovery.read_json(bucket, 'runs/' + RUN + '/progress.json')
        stop = bucket.objects.pop('runs/' + RUN + '/STOP')
        with self.assertRaisesRegex(RuntimeError, 'STOP must remain'):
            recovery.validate_import_proof(bucket, snapshot, config)
        bucket.objects['runs/' + RUN + '/STOP'] = stop
        name = 'runs/' + RUN + '/inputs.json.gz'
        generation, data = bucket.objects[name]
        bucket.objects[name] = (generation + 1, data)
        with self.assertRaisesRegex(RuntimeError, 'source generations changed'):
            recovery.validate_import_proof(bucket, snapshot, config)

    def test_failed_or_interrupted_snapshots_cannot_bypass_recovery(self):
        bucket, _, _, _, config, previous, _ = fixture()
        for state in ('failed', 'interrupted', 'running', 'recovering_memory'):
            with self.subTest(state=state), self.assertRaisesRegex(RuntimeError, 'requires explicit checkpoint recovery proof'):
                recovery.validate_import_proof(bucket, {**previous, 'state': state, 'downloading': 0}, config)

    def test_finalized_proof_is_not_rewritten_on_repeat(self):
        bucket, bq, session, args, *_ = fixture()
        report = recovery.recover(bucket, bq, session, args)
        events_before = list(bucket.events)
        replay_calls = bq.load_table_from_file.call_count
        second = recovery.recover(bucket, bq, session, args)
        self.assertEqual(report, second)
        self.assertEqual(bucket.events, events_before)
        self.assertEqual(bq.load_table_from_file.call_count, replay_calls)


if __name__ == '__main__':
    unittest.main()
