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

    def sample(self, status=200, seconds=1, transport=False, coordinator=None, transport_kind=None):
        lease = (coordinator or self.coordinator).acquire('example.org', delay=1, robots_delay=20)
        self.clock.advance(seconds)
        lease.observe(status, seconds, transport, transport_kind)
        lease.release()
        return copy.deepcopy(self.document())

    def continuous(self):
        self.policy = AdaptivePolicy('continuous-recovery', requests=None, initial_delay=2,
                                     min_delay=2, max_delay=20, max_concurrency=2)
        self.coordinator.adaptive_hosts['example.org'] = self.policy
        self.other.adaptive_hosts['example.org'] = self.policy

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

    def test_denials_do_not_impute_host_overload(self):
        state = self.sample(403)
        self.assertEqual(state['adaptive']['delay_seconds'], 5)
        self.assertNotIn('cooldown_until', state)
        self.sample(200)
        state = self.sample(403)
        self.assertEqual(state['adaptive']['delay_seconds'], 5)
        state = self.sample(403)
        self.assertEqual(state['adaptive']['delay_seconds'], 5)
        self.assertNotIn('cooldown_until', state)

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

    def test_repeated_rate_limiting_aborts_experiment(self):
        for _ in range(3):state = self.sample(429)
        self.assertEqual(state['adaptive']['phase'], 'aborted')
        lease = self.coordinator.acquire('example.org', delay=1, robots_delay=20)
        self.assertEqual(self.document()['delay_seconds'], 20)
        lease.release()

    def test_continuous_mode_keeps_learning_after_one_hundred_requests(self):
        policy = AdaptivePolicy('continuous', requests=None, window=5)
        self.coordinator.adaptive_hosts['example.org'] = policy
        for _ in range(110):state = self.sample()
        self.assertEqual(state['adaptive']['phase'], 'continuous')
        self.assertEqual(state['adaptive']['started'], 110)
        self.assertEqual(state['adaptive']['completed'], 110)
        self.assertEqual(state['adaptive']['delay_seconds'], 1)
        self.assertEqual(state['robots_delay_seconds'], 20)
        self.assertEqual(state['adaptive']['sample_owners'], {})
        self.assertLessEqual(len(state['adaptive']['adjustments']), 30)

    def test_continuous_mode_pauses_rate_limits_without_resetting_its_statistics(self):
        self.coordinator.adaptive_hosts['example.org'] = AdaptivePolicy('continuous', requests=None)
        for _ in range(3):state = self.sample(429)
        self.assertEqual(state['adaptive']['phase'], 'continuous')
        self.assertEqual(state['adaptive']['delay_seconds'], 5)
        self.assertEqual(state['adaptive']['status_counts'], {'429': 3})
        self.assertEqual(state['adaptive']['max_concurrency'], 1)
        self.assertEqual(state['adaptive']['recovery_pause_seconds'], 240)
        self.assertEqual(state['cooldown_until'], self.clock.now() + 240)

    def test_continuous_retry_after_gates_single_probe_and_three_healthy_responses_restore_capacity(self):
        self.continuous()
        lease = self.coordinator.acquire('example.org', delay=1, robots_delay=20)
        lease.defer(600)
        lease.observe(429, 1)
        lease.release()
        deadline = self.document()['cooldown_until']
        denied, admission = self.other.try_acquire('example.org', delay=1, robots_delay=20)
        self.assertIsNone(denied)
        self.assertEqual(admission.cooldown_seconds, 600)
        self.clock.advance(599)
        self.assertFalse(self.other.availability(['example.org'])['example.org'].acquired)
        self.clock.advance(1)
        first = self.coordinator.acquire('example.org', delay=1, robots_delay=20)
        self.clock.advance(2)
        # Recovery has one permit even when the normal policy allows two.
        self.assertIsNone(self.other.try_acquire('example.org', delay=1, robots_delay=20)[0])
        first.observe(404, 1)
        first.release()
        state = self.sample(403, coordinator=self.other)
        self.assertEqual(state['adaptive']['recovery_probes'], 1)
        self.assertEqual(state['adaptive']['max_concurrency'], 1)
        self.sample(200, coordinator=self.other)
        state = self.sample(410)
        self.assertEqual(state['adaptive']['recovery_mode'], 'normal')
        self.assertEqual(state['adaptive']['max_concurrency'], 2)
        self.assertEqual(state['adaptive']['delay_seconds'], 2)
        self.assertEqual(state['adaptive']['status_counts'], {'429': 1, '404': 1, '403': 1, '200': 1, '410': 1})
        self.assertLess(self.clock.now() - deadline, 10)

    def test_continuous_three_timeouts_pause_by_clock_and_recover_without_twenty_more_responses(self):
        self.continuous()
        for _ in range(3):
            state = self.sample(None, 25, True)
        self.assertEqual(state['adaptive']['delay_seconds'], 2)
        self.assertEqual(state['adaptive']['recovery_pause_seconds'], 30)
        self.assertEqual(state['cooldown_until'], self.clock.now() + 30)
        self.assertEqual(state['adaptive']['status_counts'], {'transport_error': 3})
        # A peer sees the same shared pause and cannot acquire before expiry.
        self.assertEqual(self.other.availability(['example.org'])['example.org'].cooldown_seconds, 30)
        start = self.clock.now()
        for _ in range(3):
            state = self.sample(404, coordinator=self.other)
        self.assertLess(self.clock.now() - start, 40)
        self.assertEqual(state['adaptive']['recovery_mode'], 'normal')
        self.assertEqual(state['adaptive']['max_concurrency'], 2)
        self.assertEqual(state['adaptive']['completed'], 6)

    def test_continuous_server_error_window_creates_thirty_second_pause(self):
        self.continuous()
        for index in range(20):
            state = self.sample(500 if index in (0, 10) else 200)
        self.assertEqual(state['adaptive']['recovery_pause_seconds'], 30)
        self.assertEqual(state['adaptive']['delay_seconds'], 2)
        self.assertEqual(state['cooldown_until'], self.clock.now() + 30)
        self.assertEqual(state['adaptive']['status_counts'], {'500': 2, '200': 18})

    def test_continuous_expired_owner_cannot_extend_retry_after_or_reset_recovery(self):
        self.continuous()
        old = self.coordinator.acquire('example.org', delay=1, robots_delay=20)
        self.clock.advance(self.coordinator.lease_seconds + 1)
        replacement = self.other.acquire('example.org', delay=1, robots_delay=20)
        replacement.defer(600)
        replacement.observe(429, 1)
        before = copy.deepcopy(self.document())
        self.assertFalse(self.store.observe('example.org', old.owner, self.policy, 429, 1))
        self.assertEqual(self.document(), before)
        replacement.release()
        self.assertEqual(self.other.availability(['example.org'])['example.org'].cooldown_seconds, 600)

    def test_failed_recovery_probes_extend_pause_to_bounded_five_minutes(self):
        self.continuous()
        pauses = []
        for _ in range(7):
            if pauses:
                self.clock.advance(max(0, self.document()['cooldown_until'] - self.clock.now()))
            state = self.sample(429)
            pauses.append(state['adaptive']['recovery_pause_seconds'])
            self.assertEqual(state['cooldown_until'], self.clock.now() + pauses[-1])
            self.assertEqual(state['adaptive']['delay_seconds'], 2)
            self.assertEqual(state['adaptive']['max_concurrency'], 1)
        self.assertEqual(pauses, [60, 120, 240, 300, 300, 300, 300])
        self.clock.advance(state['cooldown_until'] - self.clock.now())
        state = self.sample(None, 25, True)
        self.assertEqual(state['adaptive']['recovery_pause_seconds'], 300)
        self.clock.advance(state['cooldown_until'] - self.clock.now())
        for code in (404, 403, 200, 403, 410):
            state = self.sample(code)
        self.assertEqual(state['adaptive']['max_concurrency'], 2)
        self.assertEqual(state['adaptive']['recovery_pause_seconds'], 0)

    def test_inflight_response_during_pause_does_not_pass_or_double_recovery_probe(self):
        self.continuous()
        first = self.coordinator.acquire('example.org', delay=1, robots_delay=20)
        self.clock.advance(2)
        peer = self.other.acquire('example.org', delay=1, robots_delay=20)
        first.observe(429, 1)
        first.release()
        deadline = self.document()['cooldown_until']
        peer.observe(200, 1)
        peer.release()
        self.assertEqual(self.document()['adaptive']['recovery_probes'], 0)
        self.assertEqual(self.document()['cooldown_until'], deadline)
        self.assertEqual(self.document()['adaptive']['recovery_pause_seconds'], 60)

    def test_tls_and_dns_failures_remain_diagnostics_without_imputing_server_overload(self):
        self.continuous()
        for kind in ('tls', 'dns'):
            for _ in range(3):
                state = self.sample(None, 25, True, transport_kind=kind)
        self.assertNotIn('cooldown_until', state)
        self.assertEqual(state['adaptive']['transport_errors'], 6)
        self.assertEqual(state['adaptive']['transport_kinds'], {'tls': 3, 'dns': 3})
        self.assertEqual(state['adaptive']['max_concurrency'], 2)
        self.sample(429)
        self.sample(404)
        state = self.sample(None, 25, True, transport_kind='tls')
        self.assertEqual(state['adaptive']['recovery_probes'], 1)
        self.assertEqual(state['adaptive']['recovery_pause_seconds'], 60)

    def test_finite_pilot_keeps_legacy_backoff_for_transport_diagnostics(self):
        for _ in range(3):
            state = self.sample(None, 25, True, transport_kind='tls')
        self.assertEqual(state['adaptive']['delay_seconds'], 10)
        self.assertEqual(state['adaptive']['transport_kinds'], {'tls': 3})

    def test_existing_continuous_ratchet_migrates_to_probe_without_reset_or_retry_after_bypass(self):
        self.continuous()
        self.sample(200)
        state = self.document()
        adaptive = state['adaptive']
        for field in ('recovery_mode', 'recovery_probes', 'recovery_pause_seconds', 'recovery_until',
                      'last_healthy_delay', 'transport_kinds'):
            adaptive.pop(field)
        adaptive.update(delay_seconds=120, max_concurrency=1)
        state.update(delay_seconds=120, cooldown_until=self.clock.now() + 600)
        denied, admission = self.other.try_acquire('example.org', delay=1, robots_delay=20)
        self.assertIsNone(denied)
        self.assertEqual(admission.cooldown_seconds, 600)
        self.assertEqual(self.document()['adaptive']['delay_seconds'], 2)
        self.assertEqual(self.document()['adaptive']['max_concurrency'], 1)
        self.assertEqual(self.document()['adaptive']['started'], 1)
        self.assertEqual(self.document()['adaptive']['status_counts'], {'200': 1})
        self.clock.advance(admission.cooldown_seconds)
        for _ in range(3):
            state = self.sample(404)
        self.assertEqual(state['adaptive']['max_concurrency'], 2)
        self.assertEqual(state['adaptive']['completed'], 4)

    def test_long_synthetic_trace_preserves_all_outcomes_and_never_ratchets_request_spacing(self):
        # Matches the live trace's aggregate outcomes, not its unknown event
        # order: clusters of transport errors and occasional HTTP 429s exercise
        # pauses repeatedly among old 404s and neutral denied URLs.
        import random
        self.continuous()
        outcomes = ([200] * 277 + [404] * 1840 + [403] * 754 + [429] * 7
                    + [500] * 4 + [502] + [520] * 2 + [301] + [None] * 31)
        random.Random(37).shuffle(outcomes)
        start = self.clock.now()
        observed = {}
        for code in outcomes:
            state = self.sample(code, 25 if code is None else .7, code is None)
            label = 'transport_error' if code is None else str(code)
            observed[label] = observed.get(label, 0) + 1
            self.assertEqual(state['adaptive']['delay_seconds'], 2)
            self.assertLessEqual(len(state['adaptive']['window_results']), 20)
            self.assertLessEqual(len(state['adaptive']['adjustments']), 30)
        self.assertEqual(state['adaptive']['status_counts'], observed)
        self.assertEqual(state['adaptive']['started'], 2917)
        self.assertEqual(state['adaptive']['completed'], 2917)
        self.assertEqual(state['adaptive']['transport_errors'], 31)
        self.assertLess(self.clock.now() - start, 10000)

    def test_neutral_denials_do_not_prevent_learning_from_healthy_responses(self):
        for code in [200, 403, 404, 403, 200]:state = self.sample(code)
        self.assertEqual(state['adaptive']['delay_seconds'], 3.5)
        self.assertEqual(state['adaptive']['status_counts']['403'], 2)

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
            session.return_value.get.side_effect = requests.ReadTimeout('slow')
            with self.assertRaises(requests.ReadTimeout):fetcher.one('https://example.org/story')
        self.assertEqual(self.document()['adaptive']['transport_errors'], 1)
        self.assertEqual(self.document()['leases'], {})

    def test_denial_with_explicit_retry_after_still_pauses_host(self):
        from contextlib import nullcontext
        from types import SimpleNamespace
        fetcher = Fetcher(Bucket(), 'pilot', delay=1, host_coordinator=self.coordinator)
        response = SimpleNamespace(status_code=403, headers={'Retry-After': '600'},
                                   iter_content=lambda _: [b'denied'])
        with patch('crawl.public_url', side_effect=urlsplit), patch('crawl.requests.Session') as session:
            session.return_value.get.return_value = nullcontext(response)
            fetcher.one('https://example.org/story')
        self.assertEqual(self.document()['cooldown_until'], 700)
        self.assertEqual(self.document()['adaptive']['delay_seconds'], 5)


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
