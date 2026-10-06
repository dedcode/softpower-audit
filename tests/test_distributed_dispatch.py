import time
import unittest
from unittest.mock import Mock

from test_distributed_worker import bare_run, worker_record
from shared_queue import Claim, article_key
from shared_hosts import Admission


def article(url, outlet='news.ke'):
    return {'url': url, 'outlet': outlet}


class DistributedDispatchTests(unittest.TestCase):
    def run_with_pages(self, pages, states):
        run = bare_run()
        run.verification = False
        run.config['workers'] = 48
        run.fetcher.dispatch_availability = Mock(side_effect=lambda hosts: (
            None if states is None else {host: states[host] for host in hosts}))

        def claim_page(**kwargs):
            if kwargs['phase'] != 'publisher':
                self.assertNotIn('admit', kwargs)
                return []
            claims = []
            for page in pages:
                kwargs['prepare_admission'](page)
                for item in page:
                    if kwargs['admit'](item):
                        claims.append(Claim(article_key(item['url']), 'token', item, time.time() + 600))
            return claims

        run.queue.claim.side_effect = claim_page
        return run

    def test_dispatch_uses_actual_url_host_and_skips_full_or_cooling_hosts(self):
        pages = [[article('https://blocked.ke/a', 'ready.ke'), article('https://ready.ke/a', 'alias-one.ke')],
                 [article('https://ready.ke/b', 'alias-two.ke'), article('https://other.ke/a'),
                  article('https://cooling.ke/a')]]
        states = {'blocked.ke': Admission(False, 10), 'ready.ke': Admission(True),
                  'other.ke': Admission(True), 'cooling.ke': Admission(False, 500, 500)}
        run = self.run_with_pages(pages, states)
        claims = run.claim_available()
        self.assertEqual([claim.item['url'] for claim in claims], ['https://ready.ke/a', 'https://other.ke/a'])
        # Repeated hosts and publisher aliases do not create repeated Firestore reads.
        self.assertEqual([call.args[0] for call in run.fetcher.dispatch_availability.call_args_list],
                         [['blocked.ke', 'ready.ke'], ['cooling.ke', 'other.ke']])
        self.assertEqual([call.kwargs['phase'] for call in run.queue.claim.call_args_list],
                         ['publisher', 'browser', 'archive'])
        self.assertEqual(run.dispatch_retry_seconds, 3.)

    def test_each_refill_rechecks_availability_and_invalid_urls_reach_pipeline(self):
        items = [article('https://News.KE./a'), article('not a valid url')]
        run = self.run_with_pages([items], {'news.ke': Admission(False, 4)})
        self.assertEqual([claim.item for claim in run.claim_available()], [items[1]])
        run.fetcher.dispatch_availability.side_effect = lambda hosts: {host: Admission(True) for host in hosts}
        self.assertEqual([claim.item for claim in run.claim_available()], items)
        self.assertEqual(run.fetcher.dispatch_availability.call_count, 2)

    def test_short_spacing_rechecks_soon_and_full_hosts_never_trigger_empty_queue_backoff(self):
        run = self.run_with_pages([[article('https://news.ke/a')]], {'news.ke': Admission(False, .8)})
        self.assertEqual(run.claim_available(), [])
        self.assertEqual(run.dispatch_retry_seconds, .8)
        run.fetcher.dispatch_availability.side_effect = lambda hosts: {host: Admission(False, 180) for host in hosts}
        self.assertEqual(run.claim_available(), [])
        self.assertEqual(run.dispatch_retry_seconds, 3.)

    def test_no_shared_coordinator_preserves_original_dispatch_capacity(self):
        items = [article('https://news.ke/a'), article('https://news.ke/b')]
        run = self.run_with_pages([items], None)
        self.assertEqual([claim.item for claim in run.claim_available()], items)
        self.assertIsNone(run.dispatch_retry_seconds)

    def test_readiness_error_does_not_turn_urls_into_extraction_failures(self):
        run = self.run_with_pages([[article('https://news.ke/a')]], {})
        run.fetcher.dispatch_availability.side_effect = RuntimeError('host state unavailable')
        with self.assertRaisesRegex(RuntimeError, 'host state unavailable'):
            run.claim_available()
        run.queue.complete.assert_not_called()
        run.queue.handoff.assert_not_called()
        self.assertEqual(run.claims, {})

    def test_heartbeat_reports_actual_network_activity(self):
        run = bare_run()
        run.lease_update = Mock()
        run.queue.heartbeat.return_value = set()
        metrics = {'http_in_flight': 3, 'http_started': 5, 'http_completed': 2, 'http_errors': 1,
                   'http_seconds': 4.5, 'host_waiters': 2, 'host_wait_seconds': 3.5}
        run.fetcher.network_snapshot = Mock(return_value=metrics)
        run.heartbeat('running')
        self.assertEqual(run.queue.heartbeat.call_args.kwargs['worker_state']['network'], metrics)

    def test_publish_sums_counters_but_excludes_stopped_worker_gauges(self):
        run = bare_run()
        run.config['delay_seconds'] = 1
        run.task_count = 2
        run.queue.summary.return_value = {'processed': 1, 'total': 10, 'pending': 7,
                                         'downloading': 2, 'counts': {'saved': 1}, 'domains': []}
        run.queue.list_workers.return_value = [
            worker_record(0, 'running', network={'http_in_flight': 2, 'host_waiters': 1,
                'http_started': 4, 'http_completed': 2, 'http_errors': 1, 'http_seconds': 3., 'host_wait_seconds': 2.}),
            worker_record(1, 'completed', network={'http_in_flight': 1, 'host_waiters': 1,
                'http_started': 5, 'http_completed': 5, 'http_errors': 0, 'http_seconds': 7., 'host_wait_seconds': 6.}),
        ]
        run.bucket.blob.return_value.generation = 4
        summary = run.publish()
        self.assertEqual(summary['network'], {'http_in_flight': 2, 'host_waiters': 1,
            'http_started': 9, 'http_completed': 7, 'http_errors': 1, 'http_seconds': 10., 'host_wait_seconds': 8.})
        self.assertEqual(summary['limits']['delay_seconds'], 1)


if __name__ == '__main__':
    unittest.main()
