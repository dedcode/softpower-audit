import copy
import json
from pathlib import Path
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import release_distributed_crawl_for_scaleover as scaleover
from test_migration_recovery import Bucket, terminal_resources, RUN, EXECUTION, WORKFLOW


def fixture():
    bucket = Bucket()
    prefix = 'runs/' + RUN + '/'
    config = {'run_id': RUN, 'country': 'KE', 'workers': 48, 'input_count': 100}
    bucket.add(prefix + 'config.json', config)
    bucket.add(prefix + 'STOP', b'scale to ten')
    bucket.add('control/worker-lease.json', {'run_id': RUN, 'execution': EXECUTION,
        'owner': EXECUTION, 'distributed': True, 'task_count': 4})
    args = SimpleNamespace(run_id=RUN, execution=EXECUTION, workflow_execution=WORKFLOW,
                           from_instances=4, to_instances=10, stop_generation=1, apply=True)
    queue = Mock()
    queue.control.return_value = {'run_id': RUN, 'queue_version': 1, 'input_count': 100, 'stop': False, 'state': 'ready'}
    queue.summary.return_value = {'run_id': RUN, 'total': 100, 'processed': 25, 'pending': 35,
        'downloading': 40, 'counts': {'saved': 25}, 'attempts': 50, 'response_bytes': 1000, 'stored_bytes': 200}
    execution, workflow = terminal_resources()
    execution['taskCount'] = 4
    execution['template']['containers'][0]['env'].append({'name': 'CRAWL_DISTRIBUTED', 'value': '1'})
    job = {'name': 'projects/citygraph/locations/us-central1/jobs/softpower-crawler', 'generation': 20,
        'terminalCondition': {'state': 'CONDITION_SUCCEEDED'}, 'template': {'taskCount': 10, 'parallelism': 10,
            'template': {'containers': [{'env': [{'name': 'CRAWL_DISTRIBUTED', 'value': '1'},
                {'name': 'CRAWL_BUCKET', 'value': 'citygraph-softpower-crawl'},
                {'name': 'CRAWL_FIRESTORE_DATABASE', 'value': 'softpower-crawl'}]}]}}}
    session = Mock()
    active = {}
    def get(url, **kwargs):
        if 'workflowexecutions' in url:
            data = workflow if url.endswith('/' + WORKFLOW) else active
        else:
            data = execution if '/executions/' in url else job
        response = Mock()
        response.json.return_value = copy.deepcopy(data)
        return response
    session.get.side_effect = get
    return bucket, queue, session, args, execution, workflow, job, active


class DistributedScaleoverTests(unittest.TestCase):
    def test_releases_only_global_lease_preserving_stop_and_every_queue_document(self):
        bucket, queue, session, args, *_ = fixture()
        proof = scaleover.release(bucket, queue, session, args)
        self.assertEqual(proof['state'], 'released')
        self.assertFalse(proof['queue_changed'])
        self.assertFalse(proof['old_claims_requeued'])
        self.assertFalse(proof['result_outbox_changed'])
        self.assertEqual(proof['queue_counts']['downloading'], 40)
        self.assertIn('runs/' + RUN + '/STOP', bucket.objects)
        self.assertNotIn('control/worker-lease.json', bucket.objects)
        self.assertTrue(all(call[0] in ('control', 'summary') for call in queue.method_calls))
        self.assertEqual([event for event in bucket.events if event[0] == 'delete'], [('delete', 'control/worker-lease.json')])

    def test_cancel_requested_but_not_terminal_cannot_release(self):
        bucket, queue, session, args, execution, *_ = fixture()
        execution.pop('completionTime')
        with self.assertRaisesRegex(RuntimeError, 'not terminal'):
            scaleover.release(bucket, queue, session, args)
        self.assertFalse(bucket.events)

    def test_other_cohort_lease_never_deleted(self):
        bucket, queue, session, args, *_ = fixture()
        bucket.add('control/worker-lease.json', {'run_id': RUN, 'execution': 'replacement',
            'owner': 'replacement', 'distributed': True, 'task_count': 10})
        with self.assertRaisesRegex(RuntimeError, 'different cohort'):
            scaleover.release(bucket, queue, session, args)
        self.assertFalse(bucket.events)

    def test_changed_stop_generation_preserves_operator_pause(self):
        bucket, queue, session, args, *_ = fixture()
        args.stop_generation = 2
        with self.assertRaisesRegex(RuntimeError, 'STOP generation changed'):
            scaleover.release(bucket, queue, session, args)
        self.assertFalse(bucket.events)

    def test_separate_queue_stop_flag_is_not_overridden(self):
        bucket, queue, session, args, *_ = fixture()
        queue.control.return_value['stop'] = True
        with self.assertRaisesRegex(RuntimeError, 'separate stop flag'):
            scaleover.release(bucket, queue, session, args)
        self.assertFalse(bucket.events)

    def test_nonresumable_queue_blocks_scaleover(self):
        bucket, queue, session, args, *_ = fixture()
        queue.control.return_value['state'] = 'failed'
        with self.assertRaisesRegex(RuntimeError, 'not in a resumable state'):
            scaleover.release(bucket, queue, session, args)
        self.assertFalse(bucket.events)

    def test_target_cannot_switch_the_crawl_bucket(self):
        bucket, queue, session, args, _, _, job, _ = fixture()
        env = job['template']['template']['containers'][0]['env']
        next(item for item in env if item['name'] == 'CRAWL_BUCKET')['value'] = 'other-bucket'
        with self.assertRaisesRegex(RuntimeError, 'existing shared crawl database'):
            scaleover.release(bucket, queue, session, args)
        self.assertFalse(bucket.events)

    def test_active_coordinator_blocks_release(self):
        bucket, queue, session, args, _, _, _, active = fixture()
        active['executions'] = [{'name': 'another', 'state': 'ACTIVE'}]
        with self.assertRaisesRegex(RuntimeError, 'still active or queued'):
            scaleover.release(bucket, queue, session, args)
        self.assertFalse(bucket.events)

    def test_target_must_already_be_ten_distributed_instances(self):
        bucket, queue, session, args, _, _, job, _ = fixture()
        job['template']['taskCount'] = 4
        with self.assertRaisesRegex(RuntimeError, 'target instance count'):
            scaleover.release(bucket, queue, session, args)
        self.assertFalse(bucket.events)

    def test_duplicate_success_is_read_only_and_does_not_rewrite_proof(self):
        bucket, queue, session, args, *_ = fixture()
        first = scaleover.release(bucket, queue, session, args)
        events = list(bucket.events)
        second = scaleover.release(bucket, queue, session, args)
        self.assertEqual(first, second)
        self.assertEqual(events, bucket.events)


if __name__ == '__main__':
    unittest.main()
