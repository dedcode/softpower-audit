import time
import unittest
from unittest.mock import Mock, patch

from test_distributed_worker import bare_run, worker_record
from shared_queue import Claim, article_key
from shared_hosts import Admission


def article(url, outlet='news.ke'):
    return {'url': url, 'outlet': outlet}


class DistributedDispatchTests(unittest.TestCase):
    def test_archive_outage_cache_expires_but_completed_preflight_cache_remains(self):
        from collections import OrderedDict
        items = [article('https://cooling.ke/unfinished'), article('https://cooling.ke/missing')]
        states = {'cooling.ke': Admission(False, 300, 300)}
        run = self.archive_first_run(items, states)
        run.archive_first_ineligible = OrderedDict([(items[0]['url'], 1010.), (items[1]['url'], None)])
        with patch('distributed_worker.time.time', return_value=1009):
            self.assertEqual(run.claim_available(), [])
        with patch('distributed_worker.time.time', return_value=1010):
            selected = run.claim_available()
        self.assertEqual([claim.item['url'] for claim in selected], [items[0]['url']])
        self.assertTrue(selected[0].archive_first)
        self.assertIn(items[1]['url'], run.archive_first_ineligible)
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

    def archive_first_run(self, items, states, archive_backlog=()):
        """Model bounded fenced claims, including the queue's per-outlet cap."""
        run = bare_run()
        run.verification = False
        run.config['workers'] = 48
        run.fetcher.dispatch_availability = Mock(side_effect=lambda hosts: {
            host: states[host] for host in hosts})
        run.fetcher.archive_first_handoff = Mock()
        leased = set()

        def reserve(**kwargs):
            if kwargs['phase'] == 'browser':
                return []
            if kwargs['phase'] == 'archive':
                return list(archive_backlog)[:kwargs['limit']]
            kwargs['prepare_admission'](items)
            selected = []
            counts = dict(kwargs['inflight'])
            for item in items:
                outlet = item['outlet']
                if (item['url'] in leased or counts.get(outlet, 0) >= kwargs['per_outlet']
                        or not kwargs['admit'](item)):
                    continue
                selected.append(Claim(article_key(item['url']), 'token', item, time.time() + 600))
                leased.add(item['url'])
                counts[outlet] = counts.get(outlet, 0) + 1
                if len(selected) == kwargs['limit']:
                    break
            return selected

        run.queue.claim.side_effect = reserve
        return run

    def test_cooldown_fills_eight_archive_handoffs_without_waiting_publisher_threads(self):
        items = [article('https://cooling.ke/' + str(i), 'cooling.ke') for i in range(20)]
        items.append(article('https://ready.ke/a', 'ready.ke'))
        run = self.archive_first_run(items, {'cooling.ke': Admission(False, 300, 300),
                                            'ready.ke': Admission(True)})
        with patch.dict('os.environ', {'CRAWL_ARCHIVE_SLOTS': '8'}), patch('distributed_worker.time.time', return_value=1000):
            selected = run.claim_available()
        metadata = [claim for claim in selected if getattr(claim, 'archive_first', False)]
        self.assertEqual(len(metadata), 8)
        self.assertEqual(len(selected), 9)
        self.assertTrue(all(claim.publisher_retry_at == 1300 for claim in metadata))
        self.assertEqual(selected[0].item['url'], 'https://ready.ke/a')
        self.assertEqual(run.queue.claim.call_args.kwargs['per_outlet'], 8)
        run.fetcher.fetch_phase.assert_not_called()
        run.queue.complete.assert_not_called()

    def test_active_handoffs_reserve_archive_capacity_alongside_archive_downloads(self):
        items = [article('https://cooling.ke/' + str(i), 'cooling.ke') for i in range(20)]
        run = self.archive_first_run(items, {'cooling.ke': Admission(False, 300, 300)})
        for i in range(7):
            active = Claim('active-' + str(i), 'token', article('https://cooling.ke/active-' + str(i),
                           'cooling.ke'), time.time() + 600, phase='archive' if i < 6 else 'publisher')
            if i == 6:
                active.archive_first = True
            run.claims[active.article_id] = active
        with patch.dict('os.environ', {'CRAWL_ARCHIVE_SLOTS': '8'}):
            selected = run.claim_available()
        self.assertEqual(len(selected), 1)
        self.assertTrue(selected[0].archive_first)
        self.assertEqual(run.queue.claim.call_args.kwargs['limit'], 1)
        self.assertEqual(run.queue.claim.call_args.kwargs['inflight']['cooling.ke'], 7)

    def test_existing_archive_backlog_gets_pool_before_new_handoffs(self):
        items = [article('https://cooling.ke/' + str(i), 'cooling.ke') for i in range(20)]
        backlog = [Claim('archive-' + str(i), 'token', items[i], time.time() + 600, phase='archive')
                   for i in range(8)]
        run = self.archive_first_run(items, {'cooling.ke': Admission(False, 300, 300)}, backlog)
        with patch.dict('os.environ', {'CRAWL_ARCHIVE_SLOTS': '8'}):
            selected = run.claim_available()
        self.assertEqual(selected, backlog)
        self.assertFalse(any(getattr(claim, 'archive_first', False) for claim in selected))
        self.assertEqual(len(run.queue.claim.call_args_list), 3)

    def test_spacing_or_full_permits_do_not_trigger_archive_first_and_checked_urls_wait_for_publisher(self):
        items = [article('https://spacing.ke/a'), article('https://full.ke/a'),
                 article('https://cooling.ke/already-checked')]
        run = self.archive_first_run(items, {'spacing.ke': Admission(False, 2),
            'full.ke': Admission(False, 180), 'cooling.ke': Admission(False, 300, 300)})
        run.archive_first_ineligible = {'https://cooling.ke/already-checked': None}
        with patch.dict('os.environ', {'CRAWL_ARCHIVE_SLOTS': '8'}):
            self.assertEqual(run.claim_available(), [])
            run.fetcher.dispatch_availability.side_effect = lambda hosts: {
                host: Admission(True) for host in hosts}
            selected = run.claim_available()
        self.assertEqual(len(selected), 3)
        self.assertFalse(any(getattr(claim, 'archive_first', False) for claim in selected))

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
