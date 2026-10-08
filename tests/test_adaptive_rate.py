import base64
import copy
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from adaptive_rate import AdaptivePolicy, decode_policies
from crawl import Fetcher
from shared_hosts import SharedHostCoordinator, acquire_state
from test_crawler import Bucket
import test_shared_hosts as host_fixture


class AdaptiveRateTests(unittest.TestCase):
    def setUp(self):
        host_fixture.SharedHostTests.setUp(self)
        self.policy = AdaptivePolicy('test-pilot', requests=100, window=5)
        self.coordinator = SharedHostCoordinator(self.store, max_concurrency=4,
            adaptive_hosts={'example.org': self.policy}, clock=self.clock.now, sleep=self.clock.advance)
        self.other = SharedHostCoordinator(self.store, max_concurrency=4,
            adaptive_hosts={'example.org': self.policy}, clock=self.clock.now, sleep=self.clock.advance)

    def document(self):
        return host_fixture.SharedHostTests.document(self)

    def sample(self, status=200, seconds=1, transport=False, coordinator=None):
        lease = (coordinator or self.coordinator).acquire('example.org', delay=1, robots_delay=20)
        self.clock.advance(seconds)
        lease.observe(status, seconds, transport)
        lease.release()
        return copy.deepcopy(self.document())

    def test_pilot_relaxes_only_opted_in_host_and_preserves_robots_rule(self):
        state = self.sample()
        self.assertEqual(state['delay_seconds'], 5)
        self.assertEqual(state['robots_delay_seconds'], 20)
        admitted, state = acquire_state({'robots_delay_seconds': 20}, 100, 'other', 'other.org', 1, 180, 4, 0)
        self.assertEqual(state['delay_seconds'], 20)
        self.assertNotIn('adaptive', state)

    def test_healthy_window_ramps_up_and_survives_another_worker(self):
        for _ in range(5):state = self.sample()
        self.assertEqual(state['adaptive']['delay_seconds'], 3.5)
        state = self.sample(coordinator=self.other)
        self.assertEqual(state['adaptive']['started'], 6)
        self.assertEqual(state['delay_seconds'], 3.5)

    def test_advisory_and_atomic_admission_agree_on_adaptive_capacity(self):
        a = self.coordinator.acquire('example.org', delay=1, robots_delay=20)
        self.clock.advance(5)
        b = self.other.acquire('example.org', delay=1, robots_delay=20)
        self.clock.advance(5)
        self.assertFalse(self.coordinator.availability(['example.org'], delay=1)['example.org'].acquired)
        self.assertIsNone(self.coordinator.try_acquire('example.org', delay=1)[0])
        a.release();b.release()

    def test_rate_limit_immediately_slows_and_retains_longer_retry_after(self):
        lease = self.coordinator.acquire('example.org', delay=1, robots_delay=20)
        lease.defer(600)
        lease.observe(429, 1)
        lease.release()
        state = self.document()
        self.assertEqual(state['adaptive']['delay_seconds'], 10)
        self.assertEqual(state['adaptive']['max_concurrency'], 1)
        self.assertEqual(state['cooldown_until'], 700)
        self.assertEqual(self.other.availability(['example.org'])['example.org'].cooldown_seconds, 600)

    def test_repeated_timeouts_back_off_without_claiming_success(self):
        for _ in range(3):state = self.sample(None, 25, True)
        self.assertEqual(state['adaptive']['delay_seconds'], 10)
        self.assertEqual(state['adaptive']['transport_errors'], 3)
        self.assertEqual(state['adaptive']['status_counts'], {'transport_error': 3})

    def test_slow_successes_do_not_increase_the_rate(self):
        for _ in range(5):state = self.sample(200, 8)
        self.assertEqual(state['adaptive']['delay_seconds'], 10)
        self.assertEqual(state['adaptive']['slow_responses'], 5)

    def test_fast_404s_allow_capacity_learning_and_remain_missing_outcomes(self):
        for _ in range(5):state = self.sample(404)
        self.assertEqual(state['adaptive']['delay_seconds'], 3.5)
        self.assertEqual(state['adaptive']['transport_errors'], 0)
        self.assertEqual(state['adaptive']['status_counts'], {'404': 5})

    def test_isolated_denial_does_not_pause_unrelated_articles(self):
        state = self.sample(403)
        self.assertEqual(state['adaptive']['delay_seconds'], 5)
        self.assertNotIn('cooldown_until', state)
        self.sample(200)
        state = self.sample(403)
        self.assertEqual(state['adaptive']['delay_seconds'], 5)
        state = self.sample(403)
        self.assertEqual(state['adaptive']['delay_seconds'], 10)
        self.assertGreater(state['cooldown_until'], self.clock.now())

    def test_request_cap_returns_to_baseline_even_after_worker_restart(self):
        policy = AdaptivePolicy('tiny-pilot', requests=2)
        self.coordinator.adaptive_hosts['example.org'] = policy
        self.other.adaptive_hosts['example.org'] = policy
        self.sample();state = self.sample(coordinator=self.other)
        self.assertEqual(state['adaptive']['phase'], 'completed')
        lease = self.coordinator.acquire('example.org', delay=1, robots_delay=20)
        self.assertEqual(self.document()['delay_seconds'], 20)
        lease.observe(200, 1);lease.release()
        self.assertEqual(self.document()['adaptive']['started'], 2)
        self.assertEqual(self.document()['adaptive']['completed'], 2)

    def test_repeated_denial_aborts_experiment(self):
        for _ in range(3):state = self.sample(403)
        self.assertEqual(state['adaptive']['phase'], 'aborted')
        lease = self.coordinator.acquire('example.org', delay=1, robots_delay=20)
        self.assertEqual(self.document()['delay_seconds'], 20)
        lease.release()

    def test_duplicate_response_and_stale_owners_cannot_change_statistics(self):
        lease = self.coordinator.acquire('example.org', delay=1, robots_delay=20)
        lease.observe(200, 1);lease.observe(429, 1)
        self.assertEqual(self.document()['adaptive']['completed'], 1)
        lease.release()
        self.assertFalse(self.store.observe('example.org', lease.owner, self.policy, 429, 1))

    def test_policy_settings_cannot_reset_budget_under_the_same_id(self):
        self.sample()
        self.other.adaptive_hosts['example.org'] = AdaptivePolicy('test-pilot', requests=200)
        with self.assertRaisesRegex(ValueError, 'cannot change'):
            self.other.try_acquire('example.org')

    def test_fetcher_reports_transport_failure_before_releasing_lease(self):
        import requests
        fetcher = Fetcher(Bucket(), 'pilot', delay=1, host_coordinator=self.coordinator)
        with patch('crawl.public_url', side_effect=urlsplit), patch('crawl.requests.Session') as session:
            session.return_value.__enter__.return_value.get.side_effect = requests.ReadTimeout('slow')
            with self.assertRaises(requests.ReadTimeout):fetcher.one('https://example.org/story')
        self.assertEqual(self.document()['adaptive']['transport_errors'], 1)
        self.assertEqual(self.document()['leases'], {})


class PolicyValidationTests(unittest.TestCase):
    def test_config_is_explicit_and_invalid_settings_fail_closed(self):
        encoded = base64.b64encode(json.dumps({'EXAMPLE.org.': {'pilot_id': 'pilot'}}).encode()).decode()
        self.assertEqual(list(decode_policies(encoded)), ['example.org'])
        self.assertEqual(decode_policies(''), {})
        for settings in [{'requests': 0}, {'requests': 1001}, {'min_delay': 0},
                         {'max_concurrency': 99}, {'initial_delay': float('nan')}]:
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                AdaptivePolicy('pilot', **settings)


if __name__ == '__main__':unittest.main()
