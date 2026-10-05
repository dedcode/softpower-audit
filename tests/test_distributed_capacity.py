import time
import unittest
from unittest.mock import Mock
from test_distributed_worker import bare_run, worker_record


class DistributedCapacityTests(unittest.TestCase):
    def test_reduced_worker_cannot_replace_cohort_capacity_with_its_local_limit(self):
        run = bare_run()
        run.configured_workers = 48
        run.config['workers'] = 1
        run.task_count = 10
        run.queue.summary.return_value = {'processed': 100, 'total': 1000,
            'pending': 467, 'downloading': 433, 'counts': {'saved': 100}, 'domains': []}
        run.queue.list_workers.return_value = [worker_record(i, 'running',
            article_slots=1 if i == 2 else 48, configured_article_slots=48) for i in range(10)]
        run.bucket.blob.return_value.generation = 4
        summary = run.publish()
        self.assertEqual(summary['article_slots'], 480)
        self.assertEqual(summary['active_article_slots'], 433)
        self.assertEqual(summary['limits']['workers'], 48)
        self.assertEqual(summary['downloading'], 433)

    def test_effective_capacity_excludes_stopped_and_expired_workers(self):
        run = bare_run()
        run.configured_workers = 48
        run.task_count = 3
        run.queue.summary.return_value = {'processed': 100, 'total': 1000,
            'pending': 852, 'downloading': 48, 'counts': {'saved': 100}, 'domains': []}
        run.queue.list_workers.return_value = [
            worker_record(0, 'running', article_slots=48),
            worker_record(1, 'running', article_slots=48, expires_at=time.time() - 1),
            worker_record(2, 'completed', article_slots=48),
        ]
        run.bucket.blob.return_value.generation = 4
        summary = run.publish()
        self.assertEqual(summary['article_slots'], 144)
        self.assertEqual(summary['active_article_slots'], 48)

    def test_heartbeat_reports_configured_and_effective_slots_separately(self):
        run = bare_run()
        run.configured_workers = 48
        run.config['workers'] = 1
        run.lease_update = Mock()
        run.queue.heartbeat.return_value = set()
        run.heartbeat('running')
        state = run.queue.heartbeat.call_args.kwargs['worker_state']
        self.assertEqual(state['configured_article_slots'], 48)
        self.assertEqual(state['article_slots'], 1)


if __name__ == '__main__':
    unittest.main()
