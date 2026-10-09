"""Bounded, opt-in host-rate experiments with durable response feedback."""
import base64
import copy
import json
import math
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class AdaptivePolicy:
    pilot_id: str
    requests: int | None = 100
    initial_delay: float = 5.
    min_delay: float = 1.
    max_delay: float = 120.
    max_concurrency: int = 2
    window: int = 20
    healthy_seconds: float = 5.

    def __post_init__(self):
        if not isinstance(self.pilot_id, str) or not self.pilot_id.strip() or len(self.pilot_id) > 100:
            raise ValueError('Adaptive policy needs a pilot ID of 1–100 characters')
        for name, low, high in [('requests', 1, 1000), ('max_concurrency', 1, 4), ('window', 5, 100)]:
            value = getattr(self, name)
            if name == 'requests' and value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
                raise ValueError('Invalid adaptive ' + name)
        for name in ('initial_delay', 'min_delay', 'max_delay', 'healthy_seconds'):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value <= 0:
                raise ValueError('Invalid adaptive ' + name)
        if not .5 <= self.min_delay <= self.initial_delay <= self.max_delay <= 120:
            raise ValueError('Adaptive delays must satisfy .5 <= min <= initial <= max <= 120')
        if self.requests is None and self.initial_delay > 10:
            raise ValueError('Continuous adaptive probes require initial_delay <= 10 seconds')


def decode_policies(value):
    from shared_hosts import normalize_host
    if not value:
        return {}
    parsed = json.loads(base64.b64decode(value, validate=True))
    if not isinstance(parsed, dict) or not parsed or len(parsed) > 10:
        raise ValueError('Adaptive hosts must be an explicit map of 1–10 hostnames')
    result = {}
    for host, settings in parsed.items():
        host = normalize_host(host)
        if host in result or not isinstance(settings, dict):
            raise ValueError('Duplicate hostname or invalid adaptive policy')
        result[host] = AdaptivePolicy(**settings)
    return result


def prepared(state, policy):
    state = copy.deepcopy(state)
    previous = state.get('adaptive', {})
    settings = asdict(policy)
    if previous.get('pilot_id') == policy.pilot_id:
        if previous.get('settings') != settings:
            raise ValueError('An existing adaptive pilot ID cannot change its settings')
        if policy.requests is None and 'recovery_mode' not in previous:
            # Upgrade an existing continuous controller without resetting its
            # statistics or bypassing any active publisher Retry-After. An old
            # ratcheted delay resumes with one cautious probe, not full capacity.
            probing = previous['delay_seconds'] > policy.initial_delay
            previous.update(recovery_mode='probing' if probing else 'normal',
                recovery_probes=0, recovery_pause_seconds=0,
                recovery_until=float(state.get('cooldown_until', 0)),
                last_healthy_delay=min(policy.initial_delay, previous['delay_seconds']),
                transport_kinds={})
            if probing:
                previous.update(delay_seconds=policy.initial_delay, max_concurrency=1, window_results=[])
        return state
    state['adaptive'] = {
        'pilot_id': policy.pilot_id, 'settings': settings,
        'phase': 'continuous' if policy.requests is None else 'sampling',
        'started': 0, 'completed': 0, 'expired_samples': 0, 'delay_seconds': policy.initial_delay,
        'max_concurrency': policy.max_concurrency, 'window_results': [],
        'consecutive_errors': 0, 'consecutive_blocks': 0, 'status_counts': {},
        'transport_errors': 0, 'slow_responses': 0, 'response_seconds': 0.,
        'adjustments': [], 'sample_owners': {},
        'recovery_mode': 'normal', 'recovery_probes': 0, 'recovery_pause_seconds': 0,
        'recovery_until': 0., 'last_healthy_delay': policy.initial_delay, 'transport_kinds': {},
    }
    return state


def limits(state, policy, default_delay, robots_delay, default_capacity):
    state = prepared(state, policy)
    adaptive = state['adaptive']
    if adaptive['phase'] not in ('sampling', 'continuous') or (policy.requests is not None and adaptive['started'] >= policy.requests):
        return state, default_delay, default_capacity
    # Only explicitly opted-in hosts treat crawl-delay as advisory during the
    # bounded experiment. The recorded rule is retained for automatic fallback.
    return state, adaptive['delay_seconds'], adaptive['max_concurrency']


def admitted(state, owner, now, policy):
    adaptive = state['adaptive']
    if adaptive['phase'] not in ('sampling', 'continuous') or (policy.requests is not None and adaptive['started'] >= policy.requests):
        return
    retained = {key: value for key, value in adaptive['sample_owners'].items()
                if float(state.get('leases', {}).get(key, 0)) > now}
    adaptive['expired_samples'] += len(adaptive['sample_owners']) - len(retained)
    adaptive['sample_owners'] = retained
    adaptive['sample_owners'][owner] = True
    adaptive['started'] += 1
    adaptive.setdefault('started_at', now)
    if policy.requests is not None and adaptive['started'] >= policy.requests:
        adaptive['phase'] = 'draining'
        adaptive['ended_at'] = now


def feedback(state, now, owner, policy, status, seconds, transport_error=False, transport_kind=None):
    """Apply a response at most once, while its fenced host lease is valid."""
    state = prepared(state, policy)
    adaptive = state['adaptive']
    if owner not in adaptive['sample_owners']:
        return state, False
    adaptive['sample_owners'].pop(owner)
    adaptive['completed'] += 1
    adaptive['response_seconds'] += seconds
    key = 'transport_error' if transport_error else str(status)
    adaptive['status_counts'][key] = adaptive['status_counts'].get(key, 0) + 1
    adaptive['transport_errors'] += int(transport_error)
    if transport_error and transport_kind is not None:
        kinds = adaptive.setdefault('transport_kinds', {})
        kind = transport_kind if transport_kind in ('tls', 'dns', 'connect_timeout', 'read_timeout', 'connection') else 'other'
        kinds[kind] = kinds.get(kind, 0) + 1
    slow = seconds > policy.healthy_seconds
    adaptive['slow_responses'] += int(slow)
    overload = status in (429, 503)
    non_capacity_transport = (policy.requests is None and transport_error
                              and transport_kind in ('tls', 'dns'))
    error = (transport_error and not non_capacity_transport) or (status is not None and status >= 500)
    # A fast 404/410 demonstrates serving capacity, although it contributes no
    # article text. Keep HTTP outcomes separate from extraction success counts.
    healthy = status in (200, 404, 410) and not slow and not transport_error
    adaptive['consecutive_errors'] = adaptive['consecutive_errors'] + 1 if error else 0
    adaptive['consecutive_blocks'] = adaptive['consecutive_blocks'] + 1 if status == 429 else 0
    neutral = (policy.requests is None and (status in (401, 403) or non_capacity_transport))
    adaptive['window_results'].append('neutral' if neutral else 'healthy' if healthy else
                                      'error' if error or status == 429 else 'slow' if slow else 'neutral')
    adaptive['window_results'] = adaptive['window_results'][-policy.window:]

    def change(delay, capacity, reason):
        old = adaptive['delay_seconds']
        adaptive['delay_seconds'] = min(policy.max_delay, max(policy.min_delay, delay))
        adaptive['max_concurrency'] = capacity
        adaptive['adjustments'].append({'at': now, 'after': adaptive['completed'], 'reason': reason,
                                       'from_seconds': old, 'to_seconds': adaptive['delay_seconds'],
                                       'max_concurrency': capacity})
        adaptive['adjustments'] = adaptive['adjustments'][-30:]
        adaptive['window_results'] = []

    def pause(base_seconds, reason):
        # Cooldowns expire by clock time. Keep one probe at the configured
        # starting rate rather than doubling the permanent request interval.
        previous_pause = adaptive.get('recovery_pause_seconds', 0)
        if adaptive.get('recovery_mode') == 'probing':
            # A response from a request already in flight when the pause began
            # is not a failed recovery probe. Only a request after that gate
            # can double the next pause.
            multiplier = 2 if now >= adaptive.get('recovery_until', 0) else 1
            base_seconds = max(base_seconds, previous_pause * multiplier)
        pause_seconds = min(300., base_seconds)
        state['cooldown_until'] = max(float(state.get('cooldown_until', 0)), now + pause_seconds)
        adaptive.update(recovery_mode='probing', recovery_probes=0,
                        recovery_pause_seconds=pause_seconds,
                        recovery_until=state['cooldown_until'])
        change(policy.initial_delay, 1, reason)
        adaptive['adjustments'][-1].update(cooldown_until=state['cooldown_until'],
                                          pause_seconds=pause_seconds)
        adaptive['consecutive_errors'] = 0

    if policy.requests is None and adaptive['phase'] == 'continuous':
        if overload:
            pause(60. if status == 429 else 30., 'overload_cooldown')
        elif adaptive.get('recovery_mode') == 'probing':
            if error or (slow and not neutral):
                pause(30., 'failed_probe_cooldown')
            elif healthy and now >= float(state.get('cooldown_until', 0)):
                adaptive['recovery_probes'] += 1
                if adaptive['recovery_probes'] >= 3:
                    restored_delay = adaptive['last_healthy_delay']
                    change(restored_delay, policy.max_concurrency, 'healthy_probes')
                    adaptive.update(recovery_mode='normal', recovery_probes=0, recovery_pause_seconds=0,
                                    recovery_until=0.)
        elif adaptive['consecutive_errors'] >= 3:
            pause(30., 'transport_or_server_cooldown')
        elif len(adaptive['window_results']) >= policy.window:
            window = adaptive['window_results']
            informative = len(window) - window.count('neutral')
            if informative >= math.ceil(policy.window / 2) and window.count('healthy') >= math.ceil(informative * .95):
                change(adaptive['delay_seconds'] * .7, policy.max_concurrency, 'healthy_window')
                adaptive['last_healthy_delay'] = adaptive['delay_seconds']
            elif window.count('error') >= max(2, math.ceil(policy.window * .1)) or window.count('slow') >= policy.window // 2:
                pause(30., 'unhealthy_window_cooldown')
            else:
                adaptive['window_results'] = []
        return state, True

    # Denied URLs retain their article-level outcome, without retries that bypass
    # access restrictions. A 401/403 alone is not a serving-capacity signal;
    # explicit Retry-After is handled separately by the HTTP/browser fetcher.
    if status == 429:
        state['cooldown_until'] = max(float(state.get('cooldown_until', 0)), now + 60)
    if adaptive['consecutive_blocks'] >= 3 and policy.requests is not None:
        adaptive['phase'] = 'aborted'
        adaptive['ended_at'] = now
        adaptive['reason'] = 'Repeated rate limiting'
    elif adaptive['phase'] in ('sampling', 'continuous'):
        if overload or adaptive['consecutive_errors'] >= 3:
            change(max(10., adaptive['delay_seconds'] * 2), 1, 'server_error_or_block')
            adaptive['consecutive_errors'] = 0
        elif len(adaptive['window_results']) >= policy.window:
            window = adaptive['window_results']
            informative = len(window) - window.count('neutral')
            if informative >= math.ceil(policy.window / 2) and window.count('healthy') >= math.ceil(informative * .95):
                change(adaptive['delay_seconds'] * .7, policy.max_concurrency, 'healthy_window')
            elif window.count('error') >= max(2, math.ceil(policy.window * .1)) or window.count('slow') >= policy.window // 2:
                change(max(10., adaptive['delay_seconds'] * 2), 1, 'unhealthy_window')
            else:
                adaptive['window_results'] = []
    if adaptive['phase'] == 'draining' and adaptive['completed'] + adaptive['expired_samples'] >= adaptive['started']:
        adaptive['phase'] = 'completed'
        adaptive['completed_at'] = now
    return state, True
