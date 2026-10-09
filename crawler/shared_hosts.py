"""Cross-container request pacing and bounded, fenced download leases.

Request starts are spaced independently of response lifetimes. A slow body
does not prevent another request from starting when a permit and its start
time are available. Every operation must finish before its lease expires;
renewals are available for longer browser sessions.
"""
import hashlib
import math
import threading
import time
import uuid
from dataclasses import dataclass

import requests


class HostLeaseLost(RuntimeError):
    """No new network operation may use a lease after ownership is uncertain."""


class SharedHostCooldown(requests.RequestException):
    def __init__(self, host, seconds):
        self.retry_after_seconds = seconds
        super().__init__(f'{host} is cooling down; retry after {seconds:.0f} seconds')


def normalize_host(host):
    host = str(host).lower().rstrip('.')
    if not host or '/' in host or '\\' in host or any(c.isspace() for c in host):
        raise ValueError('Expected a hostname')
    return host.encode('idna').decode('ascii')


def _seconds(value, name, positive=False):
    value = float(value)
    if not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise ValueError(name + ' must be ' + ('positive' if positive else 'nonnegative') + ' and finite')
    return value


@dataclass(frozen=True)
class Admission:
    acquired: bool
    wait_seconds: float = 0.
    cooldown_seconds: float = 0.


def publisher_in_cooldown(admission):
    """Archive eligibility needs an explicit shared pause, not a full permit.

    This advisory read never authorizes a publisher request. Spacing and lease
    contention alone must not change which extraction phase runs first.
    """
    if admission is None or admission.acquired:
        return False
    seconds = float(admission.cooldown_seconds)
    return math.isfinite(seconds) and seconds > 0


def _capacity(value):
    if isinstance(value, bool) or int(value) != value or value < 1 or value > 32:
        raise ValueError('max_concurrency must be an integer between 1 and 32')
    return int(value)


def _active_leases(state, now):
    return {owner: float(deadline) for owner, deadline in state.get('leases', {}).items()
            if float(deadline) > now}


def _legacy_owner(state):
    # A late pre-upgrade client may preserve the new (empty) leases map while
    # writing its own exclusive owner. Recognize that as legacy ownership too.
    owner = state.get('owner')
    return owner if owner and owner not in state.get('leases', {}) else None


def _lease_state(state, leases, now):
    """Keep an exclusive compatibility gate for any late legacy reader.

    New readers use only the individual leases. The old implementation sees
    an occupied owner until the latest active permit expires, so it cannot
    accidentally add an uncounted download during a rolling migration.
    """
    state = {**state, 'host_policy_version': 2, 'leases': leases, 'updated_at': now}
    state['owner'] = next(iter(leases), None)
    state['lease_until'] = max(leases.values(), default=0.)
    return state


def _spacing(state, delay, robots_delay):
    known_robots = float(state.get('robots_delay_seconds', 0))
    legacy_delay = float(state.get('delay_seconds', 0))
    if 'robots_delay_seconds' not in state and legacy_delay > 3:
        # Old documents did not distinguish our default three seconds from
        # robots rules. A larger legacy delay remains conservatively bound.
        known_robots = max(known_robots, legacy_delay)
    if robots_delay is None:
        # Backward-compatible callers supplied only an effective delay. Never
        # relax it without a freshly read robots policy that separates the two.
        # Do not label an unknown old three-second default as a robots rule:
        # a request for robots.txt itself may still be using that old default.
        if delay > 3:
            known_robots = max(known_robots, delay)
        return max(delay, known_robots, legacy_delay), known_robots
    else:
        known_robots = max(known_robots, robots_delay)
    return max(delay, known_robots), known_robots


def _request_policy(state, delay, max_concurrency, robots_delay, adaptive_policy):
    spacing, known_robots = _spacing(state, delay, robots_delay)
    if adaptive_policy is not None:
        from adaptive_rate import limits
        state, spacing, max_concurrency = limits(state, adaptive_policy, spacing,
                                                 known_robots, max_concurrency)
    return state, spacing, known_robots, max_concurrency


def admission_state(state, now, delay, max_concurrency=1, robots_delay=None, adaptive_policy=None):
    """Read-only policy; eligibility is advisory until an atomic acquire."""
    state, spacing, _, max_concurrency = _request_policy(state, delay, max_concurrency,
                                                        robots_delay, adaptive_policy)
    cooldown = max(0., float(state.get('cooldown_until', 0)) - now)
    last_started = state.get('last_started_at')
    permitted = (float(last_started) + spacing if last_started is not None
                 else float(state.get('next_allowed_at', 0)))
    if _legacy_owner(state):
        # An existing legacy owner holds an exclusive permit. Do not overlap it
        # until it releases or expires, regardless of the new configured limit.
        lease_wait = max(0., float(state.get('lease_until', 0)) - now)
    elif 'leases' in state:
        expirations = sorted(_active_leases(state, now).values())
        # If the configured capacity decreases, enough old permits must expire
        # before a new start can fit under the smaller bound.
        lease_wait = (expirations[len(expirations) - max_concurrency] - now
                      if len(expirations) >= max_concurrency else 0.)
    else:
        lease_wait = 0.
    wait = max(0., cooldown, lease_wait, permitted - now)
    return Admission(wait == 0, wait, cooldown)


def acquire_state(state, now, owner, host, delay, ttl, max_concurrency=1, robots_delay=None,
                  adaptive_policy=None):
    """Pure transaction policy, shared by the Firestore adapter and tests."""
    state = dict(state)
    max_concurrency = _capacity(max_concurrency)
    state, spacing, known_robots, capacity = _request_policy(state, delay, max_concurrency,
                                                            robots_delay, adaptive_policy)
    result = admission_state(state, now, delay, max_concurrency, robots_delay, adaptive_policy)
    policy = {'host': host, 'delay_seconds': spacing,
              'robots_delay_seconds': known_robots, 'default_delay_seconds': delay,
              'max_concurrency': capacity}
    wait = result.wait_seconds
    if wait > 0:
        # Share newly learned robots policy even if the host is already full.
        changed = ({**state, **policy, 'updated_at': now}
                   if any(state.get(key) != value for key, value in policy.items()) else None)
        return result, changed
    leases = _active_leases(state, now) if 'leases' in state else {}
    leases[owner] = now + ttl
    state.update(policy, last_started_at=now, next_allowed_at=now + spacing)
    state = _lease_state(state, leases, now)
    if adaptive_policy is not None:
        from adaptive_rate import admitted
        admitted(state, owner, now, adaptive_policy)
    return Admission(True), state


class FirestoreHostStore:
    """Only this adapter imports the SDK; unit tests need no cloud account."""
    def __init__(self, client, collection='crawl_hosts'):
        self.client = client
        self.collection = client.collection(collection)

    def _transaction(self, host, operation):
        from google.cloud import firestore
        reference = self.collection.document(hashlib.sha256(host.encode()).hexdigest())

        def perform(transaction):
            snapshot = reference.get(transaction=transaction)
            # Read time is assigned by Firestore, avoiding machine clock skew.
            now = snapshot.read_time.timestamp()
            result, value = operation(snapshot.to_dict() or {}, now)
            if value is not None:
                transaction.set(reference, value)
            return result

        from transaction_retry import fresh_transaction
        return fresh_transaction(self.client, perform)

    def acquire(self, host, owner, delay, ttl, max_concurrency=1, robots_delay=None, adaptive_policy=None):
        return self._transaction(host, lambda state, now:
                                 acquire_state(state, now, owner, host, delay, ttl,
                                               max_concurrency, robots_delay, adaptive_policy))

    def availability(self, hosts, delay, max_concurrency=1, robots_delays=None, adaptive_policies=None):
        """Batch advisory reads, using server times just like atomic acquire."""
        robots_delays = robots_delays or {}
        references = {hashlib.sha256(host.encode()).hexdigest(): host for host in hosts}
        result = {}
        keys = list(references)
        for offset in range(0, len(keys), 100):
            batch = [self.collection.document(key) for key in keys[offset:offset + 100]]
            for snapshot in self.client.get_all(batch):
                host = references[snapshot.id]
                result[host] = admission_state(snapshot.to_dict() or {}, snapshot.read_time.timestamp(),
                    delay, max_concurrency, robots_delays.get(host), (adaptive_policies or {}).get(host))
        # Missing snapshots are not permission to make an uncoordinated start.
        for host in hosts:
            result.setdefault(host, Admission(False, 1.))
        return result

    def renew(self, host, owner, ttl):
        def operation(state, now):
            if 'leases' in state and _legacy_owner(state) != owner:
                leases = _active_leases(state, now)
                if owner not in leases:
                    return False, None
                leases[owner] = now + ttl
                return True, _lease_state(state, leases, now)
            if state.get('owner') != owner or float(state.get('lease_until', 0)) <= now:
                return False, None
            return True, {**state, 'lease_until': now + ttl, 'updated_at': now}
        return self._transaction(host, operation)

    def release(self, host, owner):
        def operation(state, now):
            if 'leases' in state and _legacy_owner(state) != owner:
                if owner not in state['leases']:
                    return False, None
                leases = _active_leases(state, now)
                leases.pop(owner, None)
                return True, _lease_state(state, leases, now)
            if state.get('owner') != owner:
                return False, None
            return True, {**state, 'owner': None, 'lease_until': 0., 'updated_at': now}
        return self._transaction(host, operation)

    def defer(self, host, owner, seconds):
        def operation(state, now):
            owned = (owner in _active_leases(state, now) if 'leases' in state and _legacy_owner(state) != owner else
                     state.get('owner') == owner and float(state.get('lease_until', 0)) > now)
            if not owned:
                return False, None
            return True, {**state, 'cooldown_until': max(float(state.get('cooldown_until', 0)), now + seconds),
                          'updated_at': now}
        return self._transaction(host, operation)

    def cooldown(self, host, seconds):
        # Browser responses arrive after document-start admission. Extending a
        # cooldown does not need to acquire or replace another request's lease.
        def operation(state, now):
            return True, {**state, 'host': host,
                          'cooldown_until': max(float(state.get('cooldown_until', 0)), now + seconds),
                          'updated_at': now}
        return self._transaction(host, operation)

    def observe(self, host, owner, policy, status, seconds, transport_error=False, transport_kind=None):
        from adaptive_rate import feedback
        def operation(state, now):
            if owner not in _active_leases(state, now):
                return False, None
            value, changed = feedback(state, now, owner, policy, status, seconds, transport_error, transport_kind)
            return True, value if changed else None
        return self._transaction(host, operation)


class HostLease:
    def __init__(self, coordinator, host, owner, deadline, cancelled=None, guard=None):
        self.coordinator, self.host, self.owner = coordinator, host, owner
        self.cancelled = cancelled or (lambda: False)
        self.guard = guard or (lambda: None)
        self.deadline = deadline
        self.finished = threading.Event()
        self.renewal = None
        self.failure = None
        self.closed = False

    def check(self):
        self.guard()
        if self.failure is not None or self.closed or self.cancelled() or self.coordinator.clock() >= self.deadline:
            raise HostLeaseLost(f'Host lease lost for {self.host}') from self.failure

    def renew(self):
        self.check()
        started = self.coordinator.clock()
        try:
            if not self.coordinator.store.renew(self.host, self.owner, self.coordinator.lease_seconds):
                raise HostLeaseLost(f'Host lease ownership changed for {self.host}')
            self.deadline = started + self.coordinator.lease_seconds
            self.check()
        except Exception as exc:
            self.failure = exc
            raise

    def start_renewal(self):
        """For sessions longer than one bounded HTTP request, not per request."""
        self.check()
        if self.renewal is not None:
            return self

        def maintain():
            while not self.finished.wait(self.coordinator.lease_seconds / 3):
                try:
                    self.renew()
                except Exception:
                    return  # Ownership is uncertain: check() now fails closed.

        self.renewal = threading.Thread(target=maintain, name='shared-host-lease', daemon=True)
        self.renewal.start()
        return self

    def defer(self, seconds):
        self.check()
        seconds = _seconds(seconds, 'cooldown')
        if not self.coordinator.store.defer(self.host, self.owner, seconds):
            self.failure = HostLeaseLost(f'Host lease ownership changed for {self.host}')
            raise self.failure

    def observe(self, status, seconds, transport_error=False, transport_kind=None):
        policy = self.coordinator.adaptive_hosts.get(self.host)
        if policy is None:
            return
        self.check()
        details = {'transport_kind': transport_kind} if transport_kind is not None else {}
        if not self.coordinator.store.observe(self.host, self.owner, policy, status,
                                              _seconds(seconds, 'response_seconds'), transport_error, **details):
            self.failure = HostLeaseLost(f'Host lease ownership changed for {self.host}')
            raise self.failure

    def release(self):
        if self.closed:
            return
        self.finished.set()
        if self.renewal is not None:
            self.renewal.join()
        self.closed = True
        # Fencing makes a delayed old owner's release harmless to a new owner.
        self.coordinator.store.release(self.host, self.owner)

    def __enter__(self):
        self.check()
        return self

    def __exit__(self, kind, value, traceback):
        try:
            self.release()
        except Exception:
            if kind is None:
                raise


class SharedHostCoordinator:
    def __init__(self, store, lease_seconds=180, poll_seconds=5,
                 clock=time.monotonic, sleep=time.sleep, max_concurrency=1, host_limits=None,
                 adaptive_hosts=None):
        self.store = store
        self.lease_seconds = _seconds(lease_seconds, 'lease_seconds', positive=True)
        self.poll_seconds = _seconds(poll_seconds, 'poll_seconds', positive=True)
        self.clock, self.sleep = clock, sleep
        self.max_concurrency = _capacity(max_concurrency)
        self.host_limits = {normalize_host(host): _capacity(limit)
                            for host, limit in (host_limits or {}).items()}
        self.adaptive_hosts = {normalize_host(host): policy for host, policy in (adaptive_hosts or {}).items()}

    def try_acquire(self, host, delay=3, cancelled=None, guard=None, robots_delay=None):
        """Try once without sleeping; return (lease or None, admission)."""
        host = normalize_host(host)
        delay = _seconds(delay, 'delay')
        if robots_delay is not None:
            robots_delay = _seconds(robots_delay, 'robots_delay')
        cancelled = cancelled or (lambda: False)
        guard = guard or (lambda: None)
        guard()
        if cancelled():
            raise HostLeaseLost('Crawler ownership lost while waiting for ' + host)
        owner = uuid.uuid4().hex
        started = self.clock()
        # Preserve the original adapter contract for callers using the legacy
        # single-permit policy (including in-memory stores and existing tests).
        capacity = self.host_limits.get(host, self.max_concurrency)
        adaptive = self.adaptive_hosts.get(host)
        if adaptive is not None:
            result = self.store.acquire(host, owner, delay, self.lease_seconds,
                                        capacity, robots_delay, adaptive)
        elif capacity == 1 and robots_delay is None:
            result = self.store.acquire(host, owner, delay, self.lease_seconds)
        else:
            result = self.store.acquire(host, owner, delay, self.lease_seconds,
                                        capacity, robots_delay)
        if not result.acquired:
            return None, result
        # Count Firestore request latency against our local safe lifetime.
        lease = HostLease(self, host, owner, started + self.lease_seconds, cancelled, guard)
        try:
            lease.check()
        except Exception:
            lease.release()
            raise
        return lease, result

    def acquire(self, host, delay=3, cancelled=None, guard=None, robots_delay=None):
        while True:
            lease, result = self.try_acquire(host, delay, cancelled, guard, robots_delay)
            if lease is not None:
                return lease
            if result.cooldown_seconds > 120:
                raise SharedHostCooldown(host, result.cooldown_seconds)
            self.sleep(min(self.poll_seconds, max(.05, result.wait_seconds)))

    def availability(self, hosts, delay=3, robots_delays=None):
        hosts = list(dict.fromkeys(normalize_host(host) for host in hosts))
        delay = _seconds(delay, 'delay')
        robots_delays = {normalize_host(host): _seconds(value, 'robots_delay')
                         for host, value in (robots_delays or {}).items() if value is not None}
        result = {}
        for capacity in {self.host_limits.get(host, self.max_concurrency) for host in hosts}:
            group = [host for host in hosts if self.host_limits.get(host, self.max_concurrency) == capacity]
            if any(host in self.adaptive_hosts for host in group):
                result.update(self.store.availability(group, delay, capacity, robots_delays, self.adaptive_hosts))
            else:
                result.update(self.store.availability(group, delay, capacity, robots_delays))
        return result

    def defer(self, host, seconds):
        self.store.cooldown(normalize_host(host), _seconds(seconds, 'cooldown'))
