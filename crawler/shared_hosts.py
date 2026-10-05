"""Cross-container host pacing with fenced, expiring Firestore leases.

The caller keeps its in-process host lock while acquiring this lease. This
leaves one Firestore contender per host per container, rather than one per
article. Every network operation must finish before the lease's bounded
lifetime; renewals are available for longer browser sessions.
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


def acquire_state(state, now, owner, host, delay, ttl):
    """Pure transaction policy, shared by the Firestore adapter and tests."""
    state = dict(state)
    cooldown = max(0., float(state.get('cooldown_until', 0)) - now)
    spacing = max(delay, float(state.get('delay_seconds', 0)))
    permitted = max(float(state.get('next_allowed_at', 0)),
                    float(state.get('last_started_at', 0)) + spacing if state.get('last_started_at') is not None else 0)
    lease = max(0., float(state.get('lease_until', 0)) - now) if state.get('owner') else 0.
    wait = max(cooldown, lease, permitted - now)
    if wait > 0:
        # Learning a stricter robots delay matters even while another request
        # owns the host: a third container must observe it on its next claim.
        changed = None
        if spacing > float(state.get('delay_seconds', 0)):
            changed = {**state, 'host': host, 'delay_seconds': spacing,
                       'next_allowed_at': permitted, 'updated_at': now}
        return Admission(False, wait, cooldown), changed
    state.update(host=host, owner=owner, lease_until=now + ttl,
                 last_started_at=now, next_allowed_at=now + spacing,
                 delay_seconds=spacing, updated_at=now)
    return Admission(True), state


class FirestoreHostStore:
    """Only this adapter imports the SDK; unit tests need no cloud account."""
    def __init__(self, client, collection='crawl_hosts'):
        self.client = client
        self.collection = client.collection(collection)

    def _transaction(self, host, operation):
        from google.cloud import firestore
        reference = self.collection.document(hashlib.sha256(host.encode()).hexdigest())

        @firestore.transactional
        def perform(transaction):
            snapshot = reference.get(transaction=transaction)
            # Read time is assigned by Firestore, avoiding machine clock skew.
            now = snapshot.read_time.timestamp()
            result, value = operation(snapshot.to_dict() or {}, now)
            if value is not None:
                transaction.set(reference, value)
            return result

        return perform(self.client.transaction(max_attempts=8))

    def acquire(self, host, owner, delay, ttl):
        return self._transaction(host, lambda state, now:
                                 acquire_state(state, now, owner, host, delay, ttl))

    def renew(self, host, owner, ttl):
        def operation(state, now):
            if state.get('owner') != owner or float(state.get('lease_until', 0)) <= now:
                return False, None
            return True, {**state, 'lease_until': now + ttl, 'updated_at': now}
        return self._transaction(host, operation)

    def release(self, host, owner):
        def operation(state, now):
            if state.get('owner') != owner:
                return False, None
            return True, {**state, 'owner': None, 'lease_until': 0., 'updated_at': now}
        return self._transaction(host, operation)

    def defer(self, host, owner, seconds):
        def operation(state, now):
            if state.get('owner') != owner or float(state.get('lease_until', 0)) <= now:
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
                 clock=time.monotonic, sleep=time.sleep):
        self.store = store
        self.lease_seconds = _seconds(lease_seconds, 'lease_seconds', positive=True)
        self.poll_seconds = _seconds(poll_seconds, 'poll_seconds', positive=True)
        self.clock, self.sleep = clock, sleep

    def acquire(self, host, delay=3, cancelled=None, guard=None):
        host = normalize_host(host)
        delay = _seconds(delay, 'delay')
        cancelled = cancelled or (lambda: False)
        guard = guard or (lambda: None)
        owner = uuid.uuid4().hex
        while True:
            guard()
            if cancelled():
                raise HostLeaseLost('Crawler ownership lost while waiting for ' + host)
            started = self.clock()
            result = self.store.acquire(host, owner, delay, self.lease_seconds)
            if result.acquired:
                # Count Firestore request latency against our local safe lifetime.
                lease = HostLease(self, host, owner, started + self.lease_seconds, cancelled, guard)
                try:
                    lease.check()
                except Exception:
                    lease.release()
                    raise
                return lease
            if result.cooldown_seconds > 120:
                raise SharedHostCooldown(host, result.cooldown_seconds)
            self.sleep(min(self.poll_seconds, max(.05, result.wait_seconds)))

    def defer(self, host, seconds):
        self.store.cooldown(normalize_host(host), _seconds(seconds, 'cooldown'))
