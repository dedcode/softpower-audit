"""Bounded, opt-in host-rate experiments with durable response feedback."""
import base64
import copy
import json
import math
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class AdaptivePolicy:
    pilot_id: str
    requests: int = 100
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
            if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
                raise ValueError('Invalid adaptive ' + name)
        for name in ('initial_delay', 'min_delay', 'max_delay', 'healthy_seconds'):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value <= 0:
                raise ValueError('Invalid adaptive ' + name)
        if not .5 <= self.min_delay <= self.initial_delay <= self.max_delay <= 120:
            raise ValueError('Adaptive delays must satisfy .5 <= min <= initial <= max <= 120')


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
        return state
    state['adaptive'] = {
        'pilot_id': policy.pilot_id, 'settings': settings, 'phase': 'sampling',
        'started': 0, 'completed': 0, 'expired_samples': 0, 'delay_seconds': policy.initial_delay,
        'max_concurrency': policy.max_concurrency, 'window_results': [],
        'consecutive_errors': 0, 'consecutive_blocks': 0, 'status_counts': {},
        'transport_errors': 0, 'slow_responses': 0, 'response_seconds': 0.,
        'adjustments': [], 'sample_owners': {},
    }
    return state


def limits(state, policy, default_delay, robots_delay, default_capacity):
    state = prepared(state, policy)
    adaptive = state['adaptive']
    if adaptive['phase'] != 'sampling' or adaptive['started'] >= policy.requests:
        return state, default_delay, default_capacity
    # Only explicitly opted-in hosts treat crawl-delay as advisory during the
    # bounded experiment. The recorded rule is retained for automatic fallback.
    return state, adaptive['delay_seconds'], adaptive['max_concurrency']


def admitted(state, owner, now, policy):
    adaptive = state['adaptive']
    if adaptive['phase'] != 'sampling' or adaptive['started'] >= policy.requests:
        return
    retained = {key: value for key, value in adaptive['sample_owners'].items()
                if float(state.get('leases', {}).get(key, 0)) > now}
    adaptive['expired_samples'] += len(adaptive['sample_owners']) - len(retained)
    adaptive['sample_owners'] = retained
    adaptive['sample_owners'][owner] = True
    adaptive['started'] += 1
    adaptive.setdefault('started_at', now)
    if adaptive['started'] >= policy.requests:
        adaptive['phase'] = 'draining'
        adaptive['ended_at'] = now


def feedback(state, now, owner, policy, status, seconds, transport_error=False):
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
    slow = seconds > policy.healthy_seconds
    adaptive['slow_responses'] += int(slow)
    denied = status in (401, 403)
    blocked = denied or status == 429
    overload = status in (429, 503)
    error = transport_error or (status is not None and status >= 500)
    # A fast 404/410 demonstrates serving capacity, although it contributes no
    # article text. Keep HTTP outcomes separate from extraction success counts.
    healthy = status in (200, 404, 410) and not slow and not transport_error
    adaptive['consecutive_errors'] = adaptive['consecutive_errors'] + 1 if error else 0
    adaptive['consecutive_blocks'] = adaptive['consecutive_blocks'] + 1 if blocked else 0
    adaptive['window_results'].append('healthy' if healthy else 'error' if error or status == 429 else 'slow' if slow else 'neutral')

    def change(delay, capacity, reason):
        old = adaptive['delay_seconds']
        adaptive['delay_seconds'] = min(policy.max_delay, max(policy.min_delay, delay))
        adaptive['max_concurrency'] = capacity
        adaptive['adjustments'].append({'at': now, 'after': adaptive['completed'], 'reason': reason,
                                       'from_seconds': old, 'to_seconds': adaptive['delay_seconds'],
                                       'max_concurrency': capacity})
        adaptive['adjustments'] = adaptive['adjustments'][-30:]
        adaptive['window_results'] = []

    # A block never triggers identity/IP changes. Stop the fast experiment after
    # repeated explicit denial; rate-limiting also pauses all peers immediately.
    # One URL-specific 403 is not evidence that the entire host is overloaded.
    # The article still retains its denied outcome and is not retried as a bypass.
    repeated_denial = denied and adaptive['consecutive_blocks'] >= 2
    if status == 429 or repeated_denial:
        state['cooldown_until'] = max(float(state.get('cooldown_until', 0)), now + 60)
    if adaptive['consecutive_blocks'] >= 3:
        adaptive['phase'] = 'aborted'
        adaptive['ended_at'] = now
        adaptive['reason'] = 'Repeated access denial or rate limiting'
    elif adaptive['phase'] == 'sampling':
        if overload or repeated_denial or adaptive['consecutive_errors'] >= 3:
            change(max(10., adaptive['delay_seconds'] * 2), 1, 'server_error_or_block')
            adaptive['consecutive_errors'] = 0
        elif len(adaptive['window_results']) >= policy.window:
            window = adaptive['window_results']
            if window.count('healthy') >= math.ceil(policy.window * .95):
                change(adaptive['delay_seconds'] * .7, policy.max_concurrency, 'healthy_window')
            elif window.count('error') >= max(2, math.ceil(policy.window * .1)) or window.count('slow') >= policy.window // 2:
                change(max(10., adaptive['delay_seconds'] * 2), 1, 'unhealthy_window')
            else:
                adaptive['window_results'] = []
    if adaptive['phase'] == 'draining' and adaptive['completed'] + adaptive['expired_samples'] >= adaptive['started']:
        adaptive['phase'] = 'completed'
        adaptive['completed_at'] = now
    return state, True
