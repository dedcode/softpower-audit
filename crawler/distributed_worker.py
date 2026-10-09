"""Multiple Cloud Run tasks consuming one fenced, durable article queue."""
import gzip
import io
import json
import os
import re
import threading
import time
import uuid
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone, timedelta
from urllib.parse import urlsplit

from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import firestore, bigquery

import worker
from worker import Run
from crawl import now
from shared_queue import SharedQueue
from shared_hosts import publisher_in_cooldown
from memory_recovery import Recovery, reclaim_memory
from result_store import memory_usage, encode_jsonl, JsonlBatch

TERMINAL_WORKERS = {'completed', 'continuing', 'paused_by_operator', 'paused_limit',
                    'interrupted', 'failed', 'recovery_failed'}


def latest_workers(records, expected):
    """An old task attempt cannot outrank its replacement's heartbeat."""
    tasks = {}
    for record in records:
        task = str(record.get('task_index', ''))
        if not task.isdigit() or not 0 <= int(task) < expected:
            continue
        rank = (int(record.get('task_attempt', 0)), record.get('updated_at', 0))
        previous = tasks.get(task)
        if previous is None or rank > (int(previous.get('task_attempt', 0)), previous.get('updated_at', 0)):
            tasks[task] = record
    return list(tasks.values())


def aggregate_state(summary, workers, expected):
    workers = latest_workers(workers, expected)
    states = [record.get('state', 'starting') for record in workers]
    # A memory recycle or absent/stale active heartbeat is not a finished task.
    # Wait for its replacement/native retry, even when the URL count is complete.
    if len(workers) < expected or any(state not in TERMINAL_WORKERS for state in states):
        return 'running'
    for state in ('failed', 'recovery_failed', 'paused_by_operator', 'paused_limit', 'interrupted'):
        if state in states:
            return state
    return 'completed' if summary['processed'] >= summary['total'] else 'continuing'


class DistributedRun(Run):
    def __init__(self):
        super().__init__()
        self.configured_workers = int(self.config['workers'])
        self.task_count = int(os.environ['CLOUD_RUN_TASK_COUNT'])
        self.identity = f'{self.execution}-{self.task_index}-{self.task_attempt}-{uuid.uuid4().hex}'
        self.client = firestore.Client(project=self.config['project'], database=os.environ['CRAWL_FIRESTORE_DATABASE'])
        self.queue = SharedQueue(self.client, self.run_id, self.identity,
                                 start_partition=(int(self.task_index), self.task_count))
        self.claims = {}
        self.deadlines = {}
        self.claim_lock = threading.Lock()
        self.archive_first_ineligible = OrderedDict()
        self.worker_state = 'starting'
        self.last_heartbeat = 0
        self.last_export = time.monotonic()
        self.last_publish = 0
        self.summary_snapshot = None
        self.verification = os.environ.get('VERIFY_DISTRIBUTED') == '1'
        if self.verification:
            if not self.run_id.startswith('verify-distributed-'):
                raise ValueError('Verification requires an isolated run ID')
            self.lease = self.bucket.blob('control/' + self.run_id + '-lease.json')
        self.recovery = Recovery(self.bucket, self.prefix, self.execution + '-task-' + self.task_index,
                                 self.config.get('max_runtime_seconds'))
        self.started = time.monotonic() - self.recovery.elapsed
        if self.recovery.reduced_concurrency:
            self.config['workers'] = 1

    def is_retired(self):
        # Absence is normal. A network/permission failure is not absence and
        # propagates, preventing this execution from claiming or publishing.
        path = self.prefix + 'retired-executions/' + self.execution + '.json'
        return self.bucket.blob(path).exists(timeout=30)

    def require_not_retired(self):
        try:
            if self.is_retired():
                raise RuntimeError('This crawl execution has been retired')
        except Exception:
            self.fetcher.abort_event.set()
            raise

    def startup_state(self):
        if self.is_retired():
            return 'retired'
        if self.bucket.blob(self.prefix + 'STOP').exists(timeout=30) or self.queue.control().get('stop'):
            return 'paused_by_operator'
        return None

    def publication_guard(self):
        """Return a fresh owned guard generation, or None only if absent."""
        self.require_not_retired()
        for _ in range(4):
            guard = self.bucket.blob(self.lease.name)
            try:
                guard.reload(timeout=30)
            except NotFound:
                return None
            generation = guard.generation
            try:
                value = json.loads(guard.download_as_text(if_generation_match=generation, timeout=30))
            except (NotFound, PreconditionFailed):
                continue
            try:
                valid = (value.get('run_id') == self.run_id and value.get('execution') == self.execution
                         and value.get('owner') == self.execution and value.get('distributed') is True
                         and datetime.fromisoformat(value['expires_at']) > datetime.now(timezone.utc))
            except (KeyError, TypeError, ValueError):
                valid = False
            if not valid:
                self.fetcher.abort_event.set()
                raise RuntimeError('Collection publication ownership was lost')
            return generation
        self.fetcher.abort_event.set()
        raise RuntimeError('Could not verify current collection publication ownership')

    def publication_gate(self):
        """Allow writes only for the current cohort; terminal replay is read-only.

        A final peer may already have published and released the guard. Accept
        that exact completed cohort snapshot without publishing again or turning
        a successfully finished peer into a failed native task.
        """
        try:
            if self.publication_guard() is not None:
                return None
            snapshot = json.loads(self.bucket.blob(self.prefix + 'progress.json').download_as_text(timeout=30))
            records = latest_workers(snapshot.get('workers', []), self.task_count)
            totals = [snapshot.get(field) for field in ('total', 'processed', 'pending')]
            valid_counts = (all(type(value) is int and value >= 0 for value in totals)
                            and totals[1] + totals[2] == totals[0]
                            and (snapshot.get('state') != 'completed' or totals[2] == 0))
            valid = (valid_counts and snapshot.get('run_id') == self.run_id and snapshot.get('execution') == self.execution
                     and snapshot.get('state') in TERMINAL_WORKERS and snapshot.get('downloading') == 0
                     and len(records) == self.task_count
                     and all(record.get('execution') == self.execution and record.get('state') in TERMINAL_WORKERS for record in records)
                     and aggregate_state(snapshot, records, self.task_count) == snapshot.get('state'))
            if not valid or self.publication_guard() is not None:
                raise RuntimeError('Collection publication guard is absent without a verified terminal snapshot')
            self.summary_snapshot = snapshot
            self.last_publish = time.monotonic()
            return snapshot
        except Exception:
            self.fetcher.abort_event.set()
            raise

    def lease_update(self, initial=False):
        reservation_deadline = time.monotonic() + 180
        # One cohort guard, shared only by tasks of the same Cloud execution.
        conflicts = 0
        while conflicts < 12:
            self.require_not_retired()
            if initial and (self.bucket.blob(self.prefix + 'STOP').exists(timeout=30) or self.queue.control().get('stop')):
                self.fetcher.abort_event.set()
                raise RuntimeError('The collection is paused before startup')
            # Blob.reload() retains a previously loaded generation. A peer may
            # replace that generation, so every read must start from a fresh
            # handle to the current object, never a cached generation handle.
            self.lease = self.bucket.blob(self.lease.name)
            generation = 0
            try:
                self.lease.reload()
                generation = self.lease.generation
                try:
                    old = json.loads(self.lease.download_as_text(if_generation_match=generation))
                except (NotFound, PreconditionFailed):
                    # With object versioning disabled, a peer replacing the
                    # generation between reload and download can return 404.
                    conflicts += 1
                    time.sleep(.1)
                    continue
                same = (old.get('execution') == self.execution and old.get('run_id') == self.run_id
                        and old.get('owner') == self.execution and old.get('distributed') is True)
                expires_at = datetime.fromisoformat(old['expires_at'])
                token = old.get('handover_id')
                reservation = (initial and old.get('run_id') == self.run_id
                               and isinstance(token, str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', token)
                               and old.get('owner') == old.get('execution') == 'handover-' + token
                               and old.get('distributed') is True
                               and type(old.get('task_count')) is int and old['task_count'] > 0)
                if reservation:
                    remaining = reservation_deadline - time.monotonic()
                    if expires_at <= datetime.now(timezone.utc) or remaining <= 0:
                        self.fetcher.abort_event.set()
                        raise RuntimeError('Handover reservation was not transferred within 180 seconds or expired')
                    # The orchestrator can identify this execution before it
                    # starts requests. Waiting is not a failed CAS attempt and
                    # must not consume native retries or reduce worker capacity.
                    time.sleep(min(2., remaining))
                    continue
                if not same and expires_at > datetime.now(timezone.utc):
                    raise RuntimeError('Another crawl execution holds the collection lease')
                if not initial and not same:
                    raise RuntimeError('Collection lease ownership was lost')
                # Every task renews its own article claims, but the cohort has
                # one guard. Rewriting a fresh shared guard from all ten tasks
                # causes needless CAS contention at startup and each heartbeat.
                if same and expires_at > datetime.now(timezone.utc) + timedelta(minutes=5):
                    return
            except NotFound:
                # Other tasks may be starting together. A missing initial lease
                # is expected; losing an established lease must stop requests.
                if not initial and self.last_heartbeat:
                    raise RuntimeError('Collection lease disappeared') from None
            except PreconditionFailed:
                conflicts += 1
                time.sleep(.1)
                continue
            self.require_not_retired()
            payload = {'owner': self.execution, 'execution': self.execution, 'run_id': self.run_id,
                       'task_count': self.task_count, 'distributed': True,
                       'expires_at': (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()}
            try:
                self.lease.upload_from_string(json.dumps(payload), content_type='application/json',
                                              if_generation_match=generation, timeout=30)
                return
            except PreconditionFailed:
                conflicts += 1
                time.sleep(.1)
        raise RuntimeError('Could not renew collection execution lease')

    def request_guard(self, article_id):
        if self.fetcher.abort_event.is_set():
            raise RuntimeError('Worker lost queue ownership')
        with self.claim_lock:
            deadline = self.deadlines.get(article_id, 0)
        if time.monotonic() >= deadline:
            raise RuntimeError('Article lease is no longer safe for a new request')

    def fetch_claim(self, claim):
        self.fetcher.request_context.guard = lambda: self.request_guard(claim.article_id)
        self.fetcher.request_context.output_scope = claim.article_id + '/' + claim.token
        try:
            self.request_guard(claim.article_id)
            if self.verification:
                # Exercise real cloud claims/heartbeats/exports without requesting publishers.
                time.sleep(2)
                result = {**claim.item, 'article_id': claim.article_id, 'run_id': self.run_id,
                          'country': self.country, 'updated_at': now(), 'status': 'saved',
                          'attempts': [], 'response_bytes': 0, 'stored_bytes': 0, 'error': None,
                          'reused': False, 'raw_uri': None, 'text_uri': None}
            else:
                checkpoint = None
                if claim.checkpoint_uri:
                    prefix = 'gs://' + self.bucket.name + '/' + self.prefix + 'distributed/checkpoints/' + claim.article_id + '/'
                    if not claim.checkpoint_uri.startswith(prefix):
                        raise RuntimeError('Unexpected phase checkpoint location')
                    path = claim.checkpoint_uri.split('gs://' + self.bucket.name + '/', 1)[1]
                    envelope = json.loads(gzip.decompress(self.bucket.blob(path).download_as_bytes(timeout=30)))
                    if (envelope.get('article_id') != claim.article_id or envelope.get('run_id') != self.run_id
                            or envelope.get('phase') != claim.phase):
                        raise RuntimeError('Phase checkpoint does not match its queued article')
                    checkpoint = envelope['checkpoint']
                if getattr(claim, 'archive_first', False):
                    result = self.fetcher.archive_first_handoff(
                        claim.item, self.run_id, self.country, checkpoint=checkpoint,
                        publisher_retry_at=claim.publisher_retry_at)
                else:
                    result = self.fetcher.fetch_phase(claim.item, self.run_id, self.country,
                                                      phase=claim.phase, checkpoint=checkpoint)
                if result is None and getattr(claim, 'archive_first', False):
                    # A durable checkpoint says archives have already been
                    # tried. Leave publisher work unfinished without entering
                    # its network path during the shared pause. Remember this
                    # bounded eligibility result until the worker restarts.
                    with self.claim_lock:
                        cache = getattr(self, 'archive_first_ineligible', None)
                        if cache is None:
                            cache = self.archive_first_ineligible = OrderedDict()
                        cache[claim.item['url']] = None
                        cache.move_to_end(claim.item['url'])
                        while len(cache) > 4096:
                            cache.popitem(last=False)
                    result = {'status': 'publisher_pending', '_release_for_cooldown': True}
            # Infrastructure cancellation is never a terminal extraction failure.
            # A successfully preserved article may still commit if its fence holds.
            if result.get('status') != 'saved':
                self.request_guard(claim.article_id)
            return result
        finally:
            self.fetcher.request_context.guard = None
            self.fetcher.request_context.output_scope = None

    def claim_available(self):
        """Slow recovery stages get separate pools, never all article slots.

        All phases remain in the same durable queue. A handoff frees a publisher
        slot immediately; it does not finish the article or consume a retry.
        """
        from collections import Counter
        self.dispatch_retry_seconds = None
        with self.claim_lock:
            active = list(self.claims.values())
            archive_first_ineligible = set(getattr(self, 'archive_first_ineligible', ()))
        capacity = self.config['workers'] - len(active)
        if capacity <= 0:
            return []
        recovery_limit = min(2, max(1, self.config['workers'] // 8))
        publisher_limit = max(1, self.config['workers'] - 2 * recovery_limit)
        # Publisher claims come first. Recovery may borrow unused capacity,
        # bounded independently so a slow archive cannot fill every thread.
        archive_limit = min(self.config['workers'], max(recovery_limit,
                            int(os.environ.get('CRAWL_ARCHIVE_SLOTS', '2'))))
        selected = []
        # Admission here is advisory: a peer may start a request between this
        # read and execution, so the actual request still takes an atomic host
        # lease. Cache each hostname once per refill and dispatch at most one
        # article for it, instead of filling every task with waiting URLs.
        availability = {}
        cooldown_deadlines = {}
        considered = set()
        readiness_disabled = False
        dispatch_availability = getattr(self.fetcher, 'dispatch_availability', None)
        archive_first_handoff = getattr(self.fetcher, 'archive_first_handoff', None)

        def hostname(item):
            try:
                host = urlsplit(item['url']).hostname
                return host.lower().rstrip('.').encode('idna').decode('ascii') if host else None
            except (KeyError, ValueError, UnicodeError):
                # The extraction pipeline owns invalid-URL classification.
                return None

        def prepare_admission(items):
            nonlocal readiness_disabled
            if readiness_disabled:
                return
            hosts = {host for item in items if (host := hostname(item)) and host not in availability}
            if hosts:
                states = dispatch_availability(sorted(hosts))
                if states is None:
                    readiness_disabled = True
                else:
                    availability.update(states)
                    checked_at = time.time()
                    cooldown_deadlines.update({host: checked_at + state.cooldown_seconds
                                               for host, state in states.items()
                                               if publisher_in_cooldown(state)})
                    # A full host may free a permit before its crash-recovery
                    # lease expires. Recheck it within three seconds instead of
                    # treating blocked URLs like an empty queue and backing off
                    # for thirty seconds with otherwise-idle article capacity.
                    waits = [state.wait_seconds for state in states.values() if state.wait_seconds > 0]
                    hint = max(.25, min([3., *waits]))
                    previous = self.dispatch_retry_seconds
                    self.dispatch_retry_seconds = hint if previous is None else min(previous, hint)

        def admit(item):
            if readiness_disabled:
                return True
            host = hostname(item)
            if host is None:
                return True
            state = availability.get(host)
            if host in considered or state is None or not state.acquired:
                return False
            considered.add(host)
            return True

        for phase, limit in (('publisher', publisher_limit), ('browser', recovery_limit), ('archive', archive_limit)):
            phase_claims = [claim for claim in active
                            if (claim.phase == phase and not getattr(claim, 'archive_first', False))
                            or (phase == 'archive' and getattr(claim, 'archive_first', False))]
            available = min(capacity, max(0, limit - len(phase_claims)))
            if not available:
                continue
            admission = ({'admit': admit, 'prepare_admission': prepare_admission}
                         if phase == 'publisher' and not self.verification and callable(dispatch_availability) else {})
            # Archive retrieval contacts archive services, not the original
            # publisher. Its global hostname permits still limit requests; the
            # publisher's per-outlet article cap must not halve an archive pool
            # simply because the remaining URLs belong to the same newspaper.
            per_outlet = archive_limit if phase == 'archive' else self.config.get('per_outlet_workers', 4)
            claims = self.queue.claim(limit=available, per_outlet=per_outlet,
                                      inflight=Counter(claim.item['outlet'] for claim in phase_claims), phase=phase,
                                      **admission)
            selected.extend(claims)
            capacity -= len(claims)
        # A paused publisher must not leave otherwise-idle archive slots empty.
        # These are ordinary fenced publisher claims running only a checkpoint
        # handoff; the archive download starts after its separate durable claim.
        # Reserve the same pool budget for handoffs and active archive work.
        archive_active = sum(claim.phase == 'archive' or getattr(claim, 'archive_first', False)
                             for claim in [*active, *selected])
        archive_available = min(capacity, max(0, archive_limit - archive_active))
        if (archive_available and not self.verification and callable(dispatch_availability)
                and callable(archive_first_handoff) and not readiness_disabled):
            def admit_archive_first(item):
                if readiness_disabled or item.get('url') in archive_first_ineligible:
                    return False
                host = hostname(item)
                return host is not None and publisher_in_cooldown(availability.get(host))

            recovery_claims = [claim for claim in [*active, *selected]
                               if claim.phase == 'archive' or getattr(claim, 'archive_first', False)]
            claims = self.queue.claim(limit=archive_available, per_outlet=archive_limit,
                                      inflight=Counter(claim.item['outlet'] for claim in recovery_claims),
                                      phase='publisher', admit=admit_archive_first,
                                      prepare_admission=prepare_admission)
            for claim in claims:
                claim.archive_first = True
                claim.publisher_retry_at = cooldown_deadlines[hostname(claim.item)]
            selected.extend(claims)
        return selected

    def update_deadlines(self, claims):
        with self.claim_lock:
            for claim in claims:
                remaining = claim.lease_until - time.time() - 120
                self.deadlines[claim.article_id] = time.monotonic() + max(0, remaining)

    def heartbeat(self, state):
        self.worker_state = state
        try:
            with self.claim_lock:
                claims = list(self.claims.values())
            with self.fetcher.lock:
                stages = list(self.fetcher.live.values())
            elapsed = time.monotonic() - self.started
            data = {'execution': self.execution, 'task_index': self.task_index,
                    'task_attempt': self.task_attempt, 'task_count': self.task_count, 'state': state,
                    'article_slots': self.config['workers'],
                    'configured_article_slots': self.configured_workers,
                    'active_stages': stages, 'elapsed_seconds': round(elapsed),
                    'estimated_compute_usd': round(elapsed * (float(os.environ.get('CRAWL_CPU', '4')) * .000018
                                                  + float(os.environ.get('CRAWL_MEMORY_GIB', '4')) * .000002), 4)}
            network_snapshot = getattr(self.fetcher, 'network_snapshot', None)
            if callable(network_snapshot):
                data['network'] = network_snapshot()
            self.lease_update()
            lost = self.queue.heartbeat(claims, worker_state=data)
            if lost:
                raise RuntimeError('Article ownership lost; stopping requests: ' + ','.join(sorted(lost)[:3]))
            self.update_deadlines(claims)
            self.last_heartbeat = time.monotonic()
        except Exception:
            self.fetcher.abort_event.set()
            raise

    def publish(self, error=None, _attempt=0):
        previous_terminal = self.publication_gate()
        if previous_terminal is not None:
            return previous_terminal
        # Reading the generation first prevents a delayed writer replacing a newer snapshot.
        target = self.bucket.blob(self.prefix + 'progress.json')
        generation = 0
        try:
            target.reload()
            generation = target.generation
        except NotFound:
            pass
        summary = self.queue.summary()
        current = latest_workers(self.queue.list_workers(execution=self.execution), self.task_count)
        state = aggregate_state(summary, current, self.task_count)
        live = [record for record in current if record.get('expires_at', 0) > time.time()
                and record.get('state') not in TERMINAL_WORKERS]
        # Queue summary already reconciles live claim IDs against remaining counts,
        # including per-outlet pending values. Do not recompute them from absent totals.
        summary.update(country=self.country, run_id=self.run_id, phase=self.config['phase'],
                       execution=self.execution, state=state, updated_at=now(),
                       active_stages=[stage for record in live for stage in record.get('active_stages', [])],
                       instances=self.task_count, active_instances=len(live),
                       article_slots=self.task_count * self.configured_workers,
                       active_article_slots=sum(record.get('article_slots', self.configured_workers) for record in live),
                       workers=current, error=error,
                       estimated_compute_usd=sum(record.get('estimated_compute_usd', 0) for record in current),
                       result_table=self.config['dataset'] + '.crawl_results',
                       source_table=self.config['source_table'],
                       limits={key: self.config.get(key) for key in ('workers', 'per_outlet_workers', 'delay_seconds',
                               'max_runtime_seconds', 'max_response_bytes', 'max_total_attempts')})
        summary['limits']['workers'] = self.configured_workers
        if any(isinstance(record.get('network'), dict) for record in current):
            # Gauges count only live workers; cumulative counters include the
            # latest attempt of workers that have finished this execution.
            gauges = {'http_in_flight', 'host_waiters'}
            fields = (*gauges, 'http_started', 'http_completed', 'http_errors',
                      'http_seconds', 'host_wait_seconds')
            summary['network'] = {field: sum(record.get('network', {}).get(field, 0)
                                            for record in (live if field in gauges else current))
                                  for field in fields}
        previous_terminal = self.publication_gate()
        if previous_terminal is not None:
            return previous_terminal
        self.summary_snapshot = summary
        self.last_publish = time.monotonic()
        try:
            target.upload_from_string(json.dumps(summary, separators=(',', ':')), content_type='application/json',
                                      if_generation_match=generation, timeout=30)
        except PreconditionFailed:
            if state in TERMINAL_WORKERS:
                # The continuation workflow reads this object after all tasks
                # exit. A racing older "running" snapshot must not survive the
                # final publisher merely because its generation check lost.
                # Re-read counters/heartbeats as well as the object generation:
                # a native task retry may have started in the meantime.
                if _attempt >= 4:
                    raise RuntimeError('Could not persist terminal collection status after concurrent updates')
                return self.publish(error, _attempt=_attempt + 1)
            return summary
        if not self.verification:
            public = self.bucket.blob('progress/' + self.country + '.json')
            public_generation = 0
            try:
                public.reload()
                public_generation = public.generation
            except NotFound:
                pass
            # upload_from_string pins target.generation. A peer may already
            # have replaced it, so read the latest object through a fresh handle.
            latest = json.loads(self.bucket.blob(self.prefix + 'progress.json').download_as_text())
            previous_terminal = self.publication_gate()
            if previous_terminal is not None:
                return previous_terminal
            if latest.get('run_id') != self.run_id or latest.get('execution') != self.execution:
                self.fetcher.abort_event.set()
                raise RuntimeError('Progress snapshot belongs to a different collection execution')
            try:
                public.upload_from_string(json.dumps(latest, separators=(',', ':')), content_type='application/json',
                                          if_generation_match=public_generation, timeout=30)
            except PreconditionFailed:
                pass
        return summary

    def forget_claim(self, claim):
        with self.claim_lock:
            self.claims.pop(claim.article_id, None)
            self.deadlines.pop(claim.article_id, None)

    def release_claim(self, claim):
        try:
            self.queue.release(claim)
        finally:
            self.forget_claim(claim)

    def persist_result(self, claim, result):
        if result.get('_release_for_cooldown'):
            self.request_guard(claim.article_id)
            self.release_claim(claim)
            return
        if result.get('status') == 'queued':
            self.request_guard(claim.article_id)
            phase = result['next_phase']
            if phase not in ('publisher', 'browser', 'archive'):
                raise ValueError('Invalid next extraction phase')
            path = self.prefix + 'distributed/checkpoints/' + claim.article_id + '/' + claim.token + '.json.gz'
            envelope = {'article_id': claim.article_id, 'run_id': self.run_id,
                        'phase': phase, 'checkpoint': result['_checkpoint']}
            self.bucket.blob(path).upload_from_string(gzip.compress(encode_jsonl(envelope), mtime=0),
                                                     content_type='application/gzip', if_generation_match=0, timeout=60)
            if not self.queue.handoff(claim, 'gs://' + self.bucket.name + '/' + path,
                                      next_phase=phase, retry_at=result['retry_at'], result=result):
                self.fetcher.abort_event.set()
                raise RuntimeError('Phase handoff rejected because claim ownership expired or changed')
            self.forget_claim(claim)
            return
        # Evidence precedes the fenced done transition. A stale task can never
        # replace accepted results or count the same URL twice.
        path = self.prefix + 'distributed/results/' + claim.article_id + '/' + claim.token + '.json.gz'
        self.bucket.blob(path).upload_from_string(gzip.compress(encode_jsonl(result), mtime=0),
                                                 content_type='application/gzip', timeout=60)
        if not self.queue.complete(claim, result, 'gs://' + self.bucket.name + '/' + path):
            self.fetcher.abort_event.set()
            raise RuntimeError('Article completion rejected because claim ownership expired or changed')
        self.forget_claim(claim)

    def export_results(self, *, renew_claims=True):
        records = self.queue.export_pending(limit=500)
        if not records:
            self.last_export = time.monotonic()
            return
        batch = JsonlBatch(500, 8 * 1024 * 1024)
        accepted = []

        def read_record(record):
            uri = record['result_uri']
            prefix = 'gs://' + self.bucket.name + '/'
            if not uri.startswith(prefix + self.prefix + 'distributed/results/'):
                raise RuntimeError('Unexpected result evidence location')
            result = json.loads(gzip.decompress(self.bucket.blob(uri[len(prefix):]).download_as_bytes(timeout=30)))
            if result.get('article_id') != record['article_id'] or result.get('run_id') != self.run_id:
                raise RuntimeError('Outbox evidence does not match its queued article and run')
            result['attempts_json'] = json.dumps(result.pop('attempts', []), separators=(',', ':'))
            return encode_jsonl(result)

        # Prefetch only eight immutable result objects at a time. Never retain
        # all 500 decoded records while waiting on GCS, and never acknowledge a
        # prefetched record that did not fit in the byte-bounded load batch.
        with ThreadPoolExecutor(max_workers=8) as pool:
            full = False
            for offset in range(0, len(records), 8):
                if renew_claims and time.monotonic() - self.last_heartbeat >= 20:
                    self.heartbeat(self.worker_state)
                chunk = records[offset:offset + 8]
                for record, encoded in zip(chunk, pool.map(read_record, chunk)):
                    if not batch.fits(encoded):
                        full = True
                        break
                    batch.append(encoded)
                    accepted.append(record)
                if full:
                    break
        if accepted:
            if renew_claims:
                self.heartbeat(self.worker_state)
            elif self.publication_guard() is None:
                raise RuntimeError('Cannot export without collection publication ownership')
            config = bigquery.LoadJobConfig(ignore_unknown_values=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
            self.bq.load_table_from_file(io.BytesIO(batch.data()), self.config['dataset'] + '.crawl_result_events',
                                        job_config=config).result(timeout=90)
            # A background load can outlast a handover. Its immutable BQ events
            # are harmless duplicates, but only the current cohort acknowledges
            # the durable outbox. The main loop remains the sole claim renewer.
            if not renew_claims and self.publication_guard() is None:
                raise RuntimeError('Cannot acknowledge exports without collection publication ownership')
            self.queue.mark_exported(accepted)
            if renew_claims:
                self.heartbeat(self.worker_state)
        self.last_export = time.monotonic()

    def release_cohort_lease(self, summary):
        releasable = TERMINAL_WORKERS - {'failed', 'recovery_failed'}
        if summary['state'] not in releasable:
            return
        tasks = latest_workers(summary['workers'], self.task_count)
        if len(tasks) != self.task_count or any(record.get('state') not in releasable for record in tasks):
            return
        for _ in range(12):
            # A newer native attempt may have started since this summary was
            # published. Never delete its guard based on stale terminal peers.
            current = latest_workers(self.queue.list_workers(execution=self.execution), self.task_count)
            if len(current) != self.task_count or any(record.get('state') not in releasable for record in current):
                return
            self.lease = self.bucket.blob(self.lease.name)
            try:
                self.lease.reload()
                generation = self.lease.generation
                try:
                    old = json.loads(self.lease.download_as_text(if_generation_match=generation))
                except (NotFound, PreconditionFailed):
                    time.sleep(.1)
                    continue
                if (old.get('execution') == self.execution and old.get('run_id') == self.run_id
                        and old.get('owner') == self.execution and old.get('distributed') is True):
                    self.lease.delete(if_generation_match=generation)
                return
            except NotFound:
                return
            except PreconditionFailed:
                # A final peer heartbeat changed the generation between read
                # and delete. Retry against the latest object and owner.
                time.sleep(.1)
        raise RuntimeError('Could not release the final collection lease after concurrent updates')

    def run(self):
        state = 'running'
        failure = None
        next_refill = 0
        refill_delay = 3
        export_future = None
        rotation = float(os.environ.get('CRAWL_ROTATE_SECONDS', '518400'))
        if not 0 < rotation < float('inf'):
            raise ValueError('CRAWL_ROTATE_SECONDS must be positive and finite')
        try:
            startup_state = self.startup_state()
            if startup_state is not None:
                print(json.dumps({'run_id': self.run_id, 'task_index': self.task_index, 'state': startup_state}), flush=True)
                return startup_state
            self.lease_update(initial=True)
            control = self.queue.control()
            if control.get('state') not in ('ready', 'running', 'continuing'):
                raise RuntimeError('Shared crawl queue is not ready')
            self.heartbeat('starting')
            self.publish()
            if self.verification:
                deadline = time.monotonic() + 300
                while len(latest_workers(self.queue.list_workers(execution=self.execution), self.task_count)) < self.task_count:
                    if time.monotonic() > deadline:
                        raise TimeoutError('Verification peers did not start')
                    self.heartbeat('starting')
                    time.sleep(5)
            with ThreadPoolExecutor(max_workers=self.config['workers']) as pool, ThreadPoolExecutor(max_workers=1) as exporter:
                # Catch while still inside the executor: abort request admissions
                # before __exit__ waits for outstanding article futures to drain.
                try:
                    while True:
                        if export_future is not None and export_future.done():
                            # Surface failed loads immediately: evidence/outbox
                            # stay durable and the native retry can export them.
                            export_future.result()
                            export_future = None
                        if time.monotonic() - self.last_heartbeat >= 20:
                            self.heartbeat(state)
                        if time.monotonic() - self.last_publish >= 30:
                            summary = self.publish(str(failure)[:600] if failure else None)
                            if state == 'running' and self.budget_reached(summary):
                                state = 'paused_limit'
                            if self.bucket.blob(self.prefix + 'STOP').exists() or self.queue.control().get('stop'):
                                state = 'paused_by_operator'
                        if worker.STOP:
                            state = 'interrupted'
                        if state == 'running' and self.recovery.attempt_elapsed >= rotation:
                            state = 'continuing'
                        memory = memory_usage()
                        pressure = bool(memory.get('limit_bytes') and memory.get('current_bytes', 0) >= .75 * memory['limit_bytes'])
                        if state == 'running' and not pressure:
                            capacity = self.config['workers'] - len(self.active)
                            if capacity and time.monotonic() >= next_refill:
                                claims = self.claim_available()
                                refill_delay = 3 if claims else min(30, max(5, refill_delay * 2))
                                if not claims and self.dispatch_retry_seconds is not None:
                                    refill_delay = min(refill_delay, self.dispatch_retry_seconds)
                                next_refill = time.monotonic() + refill_delay
                                for claim in claims:
                                    with self.claim_lock:
                                        self.claims[claim.article_id] = claim
                                    self.update_deadlines([claim])
                                    future = pool.submit(self.fetch_claim, claim)
                                    self.active[future] = claim
                        if not self.active:
                            if state != 'running':
                                break
                            if pressure:
                                reclaim_memory()
                                memory = memory_usage()
                                if memory.get('current_bytes', 0) >= .75 * memory.get('limit_bytes', 2**62):
                                    state = 'recovering_memory' if self.recovery.reserve_restart() else 'recovery_failed'
                                    break
                            summary = self.summary_snapshot
                            if summary is None or time.monotonic() - self.last_publish >= 30:
                                summary = self.publish()
                            if summary['processed'] >= summary['total']:
                                state = 'completed'
                                break
                            time.sleep(2)
                            continue
                        finished, _ = wait(self.active, timeout=1, return_when=FIRST_COMPLETED)
                        for future in finished:
                            if time.monotonic() - self.last_heartbeat >= 20:
                                self.heartbeat(state)
                            claim = self.active.pop(future)
                            try:
                                result = future.result()
                                if self.fetcher.abort_event.is_set() and result.get('status') != 'saved':
                                    self.release_claim(claim)
                                    continue
                                self.persist_result(claim, result)
                            except Exception as exc:
                                failure = failure or exc
                                state = 'failed'
                                self.fetcher.abort_event.set()
                                self.release_claim(claim)
                        if finished:
                            refill_delay = 3
                            next_refill = min(next_refill, time.monotonic() + 3)
                        if (self.task_index == '0' and export_future is None
                                and time.monotonic() - self.last_export >= 60):
                            # BQ may take ninety seconds, plus GCS evidence reads.
                            # It must not stop publisher/recovery dispatch, claim
                            # persistence, or the twenty-second ownership renewal.
                            export_future = exporter.submit(self.export_results, renew_claims=False)
                    while export_future is not None and not export_future.done():
                        if time.monotonic() - self.last_heartbeat >= 20:
                            self.heartbeat('exporting')
                        wait([export_future], timeout=1)
                    if export_future is not None:
                        export_future.result()
                except BaseException:
                    self.fetcher.abort_event.set()
                    raise
            if failure:
                raise failure
            # Recheck operator/budget state after the final active article drains.
            if state == 'continuing':
                if worker.STOP:
                    state = 'interrupted'
                elif self.bucket.blob(self.prefix + 'STOP').exists() or self.queue.control().get('stop'):
                    state = 'paused_by_operator'
                elif self.budget_reached(self.queue.summary()):
                    state = 'paused_limit'
            self.heartbeat('exporting')
            # A failed BQ load leaves evidence and outbox entries for the retry.
            while self.queue.export_pending(limit=1):
                self.export_results()
            self.heartbeat(state)
            summary = self.publish()
            if summary['state'] in TERMINAL_WORKERS:
                self.bq.load_table_from_json([{'run_id': self.run_id, 'country': self.country,
                    'updated_at': now(), 'state': summary['state'], 'config_json': json.dumps(self.config),
                    'summary_json': json.dumps(summary)}],
                    self.config['dataset'] + '.crawl_run_events').result(timeout=90)
            self.release_cohort_lease(summary)
            print(json.dumps({'run_id': self.run_id, 'task_index': self.task_index, 'state': state}), flush=True)
            return state
        except Exception as exc:
            self.fetcher.abort_event.set()
            # The executor has now drained, so retained successful results may
            # commit; cancelled/failed work is released and remains unfinished.
            for future, claim in list(self.active.items()):
                try:
                    if future.done() and not future.cancelled():
                        result = future.result()
                        if result.get('status') == 'saved':
                            self.persist_result(claim, result)
                            continue
                except Exception:
                    pass
                try:
                    self.release_claim(claim)
                except Exception:
                    pass
            self.active.clear()
            for claim in list(self.claims.values()):
                try:
                    self.release_claim(claim)
                except Exception:
                    pass
            # Update the worker document without trying to reclaim a lost cohort
            # lease. A failed native task can then be replaced safely by Cloud Run.
            try:
                if self.publication_gate() is None:
                    self.queue.heartbeat_worker(worker_state={'state': 'failed'})
                    self.publish(type(exc).__name__ + ': ' + str(exc)[:500])
            except Exception:
                pass
            raise
        finally:
            # Both executors have drained before this closes their thread-local
            # connection pools, including failure, handover and memory recycle.
            close=getattr(self.fetcher,'close',None)
            if callable(close):close()
