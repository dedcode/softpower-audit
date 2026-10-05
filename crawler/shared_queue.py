"""Durable article claims and exactly-once accounting shared by Cloud Run tasks.

Firestore stores only coordination and compact metadata. The caller must upload
immutable result evidence to GCS before ``complete`` and must stop making new
HTTP requests if a lease cannot be renewed. A fencing token prevents a stale
process from committing a result after its claim has been reassigned.
"""
import hashlib
import math
import os
import random
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlsplit

from google.cloud import firestore

SHARDS = 32
MAX_CLAIMS = 48
WORKER_HEARTBEAT_SECONDS = 90
ACTIVE_RUN_STATES = frozenset(('ready', 'running', 'continuing'))
TERMINAL_EXCLUSIONS = frozenset(('deferred', 'retrying', 'queued'))
PHASE_DUE_FIELDS = {'publisher': 'due_at', 'archive': 'archive_due_at'}
METRICS = ('attempts', 'retries', 'response_bytes', 'stored_bytes')


def article_key(url):
    return hashlib.sha256(url.encode()).hexdigest()


def _empty_stats():
    return {'total': 0, 'processed': 0, 'counts': {}, 'domains': {}, 'archive_remaining': 0,
            **{field: 0 for field in METRICS}}


def _metrics(result):
    attempts = [len(event.get('http_attempts', ())) for event in result.get('attempts', ())]
    return {'attempts': sum(attempts),
            'retries': sum(max(0, count - 1) for count in attempts),
            'response_bytes': int(result.get('response_bytes', 0)),
            'stored_bytes': int(result.get('stored_bytes', 0))}


def _increment_stats(stats, outlet, status=None, metrics=None, add_total=False):
    domain = stats.setdefault('domains', {}).setdefault(outlet, {'total': 0, 'processed': 0, 'counts': {}})
    if add_total:
        stats['total'] += 1
        domain['total'] += 1
    if status is not None:
        stats['processed'] += 1
        stats['counts'][status] = stats['counts'].get(status, 0) + 1
        domain['processed'] += 1
        domain['counts'][status] = domain['counts'].get(status, 0) + 1
    if metrics is not None:
        for field in METRICS:
            stats[field] = stats.get(field, 0) + metrics.get(field, 0)


def _due_field(phase):
    if phase not in PHASE_DUE_FIELDS:
        raise ValueError('Queue phase must be publisher or archive')
    return PHASE_DUE_FIELDS[phase]


def _metric_delta(row, metrics):
    accounted = row.get('accounted_metrics', {})
    delta = {field: metrics[field] - accounted.get(field, 0) for field in METRICS}
    if any(value < 0 for value in delta.values()):
        raise ValueError('Cumulative article metrics cannot decrease across phases')
    return delta


def _validate_evidence(uri):
    parsed = urlsplit(uri or '')
    if parsed.scheme != 'gs' or not parsed.netloc or not parsed.path.strip('/'):
        raise ValueError('A durable gs:// evidence URI is required')


@dataclass
class Claim:
    article_id: str
    token: str
    item: dict
    lease_until: float
    phase: str = 'publisher'
    checkpoint_uri: str | None = None

    @property
    def expires_at(self):
        return self.lease_until


class SharedQueue:
    def __init__(self, client, run_id, owner, *, lease_seconds=600, clock=time.time):
        if not run_id or '/' in run_id or not owner or '/' in owner:
            raise ValueError('run_id and owner must be nonempty document IDs')
        if lease_seconds <= WORKER_HEARTBEAT_SECONDS:
            raise ValueError('Article lease must outlast the worker heartbeat')
        self.client = client
        self.run_id = run_id
        self.owner = owner
        self.lease_seconds = lease_seconds
        self.clock = clock
        self.run_ref = client.collection('crawl_runs').document(run_id)
        self.articles = self.run_ref.collection('articles')
        self.stats = self.run_ref.collection('stats')
        self.workers = self.run_ref.collection('workers')
        self._cursors = {phase: None for phase in PHASE_DUE_FIELDS}
        self._random = random.Random(owner)

    def _transaction(self, operation):
        return firestore.transactional(operation)(self.client.transaction())

    def control(self):
        snapshot = self.run_ref.get(timeout=30)
        if not snapshot.exists:
            raise RuntimeError('Shared crawl queue has not been initialized')
        return snapshot.to_dict()

    def set_control(self, **fields):
        """Update operational state without changing immutable input metadata."""
        allowed = {'state', 'stop', 'stop_reason', 'error', 'execution', 'updated_at'}
        if set(fields) - allowed:
            raise ValueError('Unsupported operational control field')
        fields['updated_at'] = self.clock()
        self.run_ref.update(fields, timeout=30)

    def initialize(self, rows, done, config, result_refs=None):
        """Seed an inactive queue from the existing manifest and ResultIndex.

        Run this only after draining the old crawler. Initialization is resumable
        by the same owner, uses conditional batches, and is invisible to claimers
        until all article documents and all counter shards have been committed.
        A ready queue is never reseeded or reset. ``result_refs`` optionally maps
        imported article IDs to their existing GCS checkpoint object.
        """
        token = uuid.uuid4().hex
        now = self.clock()
        expected = config.get('input_count')
        if expected is None:
            raise ValueError('config.input_count is required for queue initialization')
        rows = rows if isinstance(rows, list) else list(rows)
        identities = [(article_key(item['url']), item['outlet']) for item in rows]
        if len({aid for aid, _ in identities}) != len(identities):
            raise ValueError('Input manifest contains duplicate article URLs')
        if len(rows) != expected:
            raise ValueError(f'Input count mismatch: expected {expected}, received {len(rows)}')
        fingerprint = hashlib.sha256()
        for aid, outlet in sorted(identities):
            fingerprint.update((aid + '\0' + outlet + '\n').encode())
        manifest_sha256 = fingerprint.hexdigest()
        def reserve(transaction):
            snapshot = self.run_ref.get(transaction=transaction)
            previous = snapshot.to_dict() if snapshot.exists else None
            if previous and (previous.get('input_count') != expected
                    or previous.get('country') != config['country']
                    or previous.get('config', {}).get('source_table') != config.get('source_table')
                    or previous.get('manifest_sha256') != manifest_sha256):
                raise ValueError('Existing queue does not match the requested manifest')
            if previous and previous.get('state') != 'initializing':
                return False
            if (previous and previous.get('initialization_owner') != self.owner
                    and previous.get('initialization_expires_at', 0) > now):
                raise RuntimeError('Another initializer owns this queue')
            transaction.set(self.run_ref, {
                'queue_version': 1, 'run_id': self.run_id, 'country': config['country'],
                'phase': config.get('phase', 'full'), 'input_count': expected,
                'config': dict(config), 'state': 'initializing', 'stop': False,
                'manifest_sha256': manifest_sha256,
                'initialization_owner': self.owner, 'initialization_token': token,
                'initialization_expires_at': now + 900, 'updated_at': now,
            })
            return True
        if not self._transaction(reserve):
            return self.control()
        shards = [_empty_stats() for _ in range(SHARDS)]
        pending_batch = []
        result_refs = result_refs or {}
        for item in rows:
            aid = article_key(item['url'])
            entry = done.entries.get(aid)
            terminal = entry is not None and entry.terminal
            doc = {'article_id': aid, 'item': dict(item), 'outlet': item['outlet'],
                   'state': 'done' if terminal else 'ready', 'updated_at': now}
            stats = shards[int(aid[:8], 16) % SHARDS]
            if terminal:
                metrics = {field: getattr(entry, field) for field in METRICS}
                doc.update(status=entry.status, result_updated_at=entry.updated_at,
                           result_uri=result_refs.get(aid), imported_checkpoint=True, needs_export=False,
                           metrics=metrics)
                _increment_stats(stats, item['outlet'], entry.status, metrics, add_total=True)
            else:
                doc['due_at'] = 0.0
                _increment_stats(stats, item['outlet'], add_total=True)
            pending_batch.append((self.articles.document(aid), doc))
            if len(pending_batch) >= 300:
                self._seed_batch(pending_batch, token)
                pending_batch.clear()
        if pending_batch:
            self._seed_batch(pending_batch, token)
        self._seed_batch([(self.stats.document(f'{i:02d}'), stats) for i, stats in enumerate(shards)], token)
        def finish(transaction):
            snapshot = self.run_ref.get(transaction=transaction)
            control = snapshot.to_dict()
            if control.get('initialization_token') != token or control.get('state') != 'initializing':
                raise RuntimeError('Initialization ownership was lost')
            transaction.update(self.run_ref, {'state': 'ready', 'manifest_sha256': manifest_sha256,
                'updated_at': self.clock(), 'initialization_expires_at': 0})
        self._transaction(finish)
        return self.control()

    def _seed_batch(self, documents, token):
        # The control-document precondition makes all writes fail atomically if
        # another initializer takes ownership between this read and the commit.
        snapshot = self.run_ref.get(timeout=30)
        control = snapshot.to_dict()
        if control.get('initialization_token') != token or control.get('state') != 'initializing':
            raise RuntimeError('Initialization ownership was lost')
        batch = self.client.batch()
        for reference, document in documents:
            batch.set(reference, document)
        batch.update(self.run_ref, {'updated_at': self.clock(),
                     'initialization_expires_at': self.clock() + 900},
                     option=self.client.write_option(last_update_time=snapshot.update_time))
        batch.commit(timeout=60)

    def claim(self, limit=MAX_CLAIMS, per_outlet=4, excluded_outlets=(), inflight=None, phase='publisher'):
        """Claim up to ``limit`` URLs using a bounded indexed due_at query.

        ``per_outlet`` is per instance; shared HTTP pacing is enforced separately.
        Pagination rotates locally so a few currently saturated outlets cannot
        indefinitely hide other work. No corpus-wide scan occurs on refill.
        """
        due_field = _due_field(phase)
        if not 0 <= limit <= MAX_CLAIMS or per_outlet < 1:
            raise ValueError('Invalid claim or per-outlet limit')
        if not limit:
            return []
        control = self.control()
        if control.get('stop') or control.get('state') not in ACTIVE_RUN_STATES:
            return []
        now = self.clock()
        active = Counter(inflight or {})
        excluded = set(excluded_outlets)
        claims = []
        wrapped = False
        page_size = max(48, min(192, limit * 4))
        for _ in range(2):
            query = self.articles.where(filter=firestore.FieldFilter(due_field, '<=', now)).order_by(due_field).limit(page_size)
            if self._cursors[phase] is not None:
                query = query.start_after(self._cursors[phase])
            candidates = list(query.stream(timeout=30))
            if not candidates:
                if self._cursors[phase] is None or wrapped:
                    break
                self._cursors[phase] = None
                wrapped = True
                continue
            self._cursors[phase] = candidates[-1]
            self._random.shuffle(candidates)
            for candidate in candidates:
                doc = candidate.to_dict()
                outlet = doc.get('outlet')
                if outlet in excluded or active[outlet] >= per_outlet:
                    continue
                token = uuid.uuid4().hex
                def reserve(transaction):
                    live_control = self.run_ref.get(transaction=transaction).to_dict()
                    live = candidate.reference.get(transaction=transaction)
                    if live_control.get('stop') or live_control.get('state') not in ACTIVE_RUN_STATES:
                        return None
                    if not live.exists:
                        return None
                    row = live.to_dict()
                    admitted_at = self.clock()
                    if (row.get('state') == 'done' or row.get('phase', 'publisher') != phase
                            or row.get(due_field, float('inf')) > admitted_at):
                        return None
                    if row.get('state') not in ('ready', 'leased'):
                        raise RuntimeError('Unexpected article queue state')
                    expires = admitted_at + self.lease_seconds
                    transaction.update(candidate.reference, {'state': 'leased', 'owner': self.owner,
                        'claim_token': token, due_field: expires, 'phase': phase, 'updated_at': admitted_at})
                    return Claim(row['article_id'], token, row['item'], expires, phase, row.get('checkpoint_uri'))
                claimed = self._transaction(reserve)
                if claimed is not None:
                    claims.append(claimed)
                    active[outlet] += 1
                    if len(claims) == limit:
                        return claims
        return claims

    def heartbeat(self, claims, worker_state=None):
        """Renew owned, still-live claims atomically; return lost article IDs."""
        claims = list(claims)
        if len(claims) > MAX_CLAIMS:
            raise ValueError('A worker may heartbeat at most 48 active claims')
        refs = [self.articles.document(claim.article_id) for claim in claims]
        def renew(transaction):
            snapshots = list(self.client.get_all(refs, transaction=transaction)) if refs else []
            documents = {snapshot.id: snapshot for snapshot in snapshots}
            now = self.clock()
            lost = set()
            expires = now + self.lease_seconds
            for claim, reference in zip(claims, refs):
                snapshot = documents.get(claim.article_id)
                row = snapshot.to_dict() if snapshot and snapshot.exists else {}
                due_field = _due_field(claim.phase)
                if (row.get('state') != 'leased' or row.get('owner') != self.owner
                        or row.get('phase', 'publisher') != claim.phase
                        or row.get('claim_token') != claim.token or row.get(due_field, 0) <= now):
                    lost.add(claim.article_id)
                    continue
                transaction.update(reference, {due_field: expires, 'updated_at': now})
            return lost, expires
        lost, expires = self._transaction(renew)
        for claim in claims:
            claim.lease_until = 0 if claim.article_id in lost else expires
        self.heartbeat_worker([claim for claim in claims if claim.article_id not in lost], worker_state)
        return lost

    def heartbeat_worker(self, claims=(), worker_state=None):
        now = self.clock()
        details = dict(worker_state or {})
        # Never allow caller data to replace the worker identity/expiry/claims.
        details.update(owner=self.owner, run_id=self.run_id, updated_at=now,
            expires_at=now + WORKER_HEARTBEAT_SECONDS,
            execution=os.environ.get('CLOUD_RUN_EXECUTION', 'local'),
            task_index=os.environ.get('CLOUD_RUN_TASK_INDEX', '0'),
            task_attempt=os.environ.get('CLOUD_RUN_TASK_ATTEMPT', '0'),
            task_count=os.environ.get('CLOUD_RUN_TASK_COUNT', '1'),
            active=[{'article_id': claim.article_id, 'outlet': claim.item['outlet'],
                     'lease_until': claim.lease_until, 'phase': claim.phase} for claim in claims])
        details.setdefault('state', 'running')
        self.workers.document(self.owner).set(details, timeout=30)

    def owns(self, claim):
        """Local admission check, updated only after successful heartbeat commits."""
        return claim.lease_until > self.clock()

    def complete(self, claim, result, result_uri):
        """Commit compact metadata once, after caller durably writes GCS evidence.

        Returns False if ownership expired or moved to another process. Repeating
        the same completion is successful without incrementing counters again.
        """
        _validate_evidence(result_uri)
        due_field = _due_field(claim.phase)
        if result.get('article_id') != claim.article_id or result.get('outlet') != claim.item['outlet']:
            raise ValueError('Result does not belong to the claimed article')
        status = result.get('status')
        if not status or status in TERMINAL_EXCLUSIONS:
            raise ValueError('Only terminal results can complete an article claim')
        metrics = _metrics(result)
        if any(value < 0 for value in metrics.values()):
            raise ValueError('Article metrics must be nonnegative')
        reference = self.articles.document(claim.article_id)
        stats_ref = self.stats.document(f'{int(claim.article_id[:8], 16) % SHARDS:02d}')
        def finish(transaction):
            snapshot = reference.get(transaction=transaction)
            row = snapshot.to_dict() if snapshot.exists else {}
            if row.get('state') == 'done':
                return (row.get('owner') == self.owner and row.get('claim_token') == claim.token
                        and row.get('phase', 'publisher') == claim.phase and row.get('result_uri') == result_uri)
            if (row.get('state') != 'leased' or row.get('owner') != self.owner
                    or row.get('phase', 'publisher') != claim.phase
                    or row.get('claim_token') != claim.token or row.get(due_field, 0) <= self.clock()):
                return False
            stats_snapshot = stats_ref.get(transaction=transaction)
            if not stats_snapshot.exists:
                raise RuntimeError('Queue counter shard is missing; refusing unaccounted completion')
            stats = stats_snapshot.to_dict()
            _increment_stats(stats, row['outlet'], status, _metric_delta(row, metrics))
            if claim.phase == 'archive':
                if stats.get('archive_remaining', 0) < 1:
                    raise RuntimeError('Archive counter is inconsistent')
                stats['archive_remaining'] -= 1
            row.update(state='done', status=status, result_uri=result_uri, metrics=metrics, needs_export=True,
                       result_updated_at=result.get('updated_at'), updated_at=self.clock())
            row.pop('due_at', None)
            row.pop('archive_due_at', None)
            transaction.set(reference, row)
            transaction.set(stats_ref, stats)
            return True
        return self._transaction(finish)

    def handoff(self, claim, checkpoint_uri, *, next_phase, retry_at, result):
        """Durably queue another phase without claiming an article is finished.

        The caller uploads an immutable cumulative-result checkpoint first. The
        transition and newly incurred metrics commit together; retries of the
        same handoff do not account the same attempts or bytes twice.
        """
        _validate_evidence(checkpoint_uri)
        due_field = _due_field(claim.phase)
        next_due = _due_field(next_phase)
        retry_at = float(retry_at)
        if not math.isfinite(retry_at) or retry_at < 0:
            raise ValueError('retry_at must be a finite nonnegative epoch time')
        if result.get('article_id') != claim.article_id or result.get('outlet') != claim.item['outlet']:
            raise ValueError('Checkpoint result does not belong to the claimed article')
        metrics = _metrics(result)
        if any(value < 0 for value in metrics.values()):
            raise ValueError('Article metrics must be nonnegative')
        reference = self.articles.document(claim.article_id)
        stats_ref = self.stats.document(f'{int(claim.article_id[:8], 16) % SHARDS:02d}')
        handoff_identity = {'owner': self.owner, 'token': claim.token,
                            'checkpoint_uri': checkpoint_uri, 'next_phase': next_phase,
                            'retry_at': retry_at}
        def transition(transaction):
            snapshot = reference.get(transaction=transaction)
            row = snapshot.to_dict() if snapshot.exists else {}
            if row.get('last_handoff') == handoff_identity:
                return True
            if (row.get('state') != 'leased' or row.get('owner') != self.owner
                    or row.get('phase', 'publisher') != claim.phase
                    or row.get('claim_token') != claim.token or row.get(due_field, 0) <= self.clock()):
                return False
            stats_snapshot = stats_ref.get(transaction=transaction)
            if not stats_snapshot.exists:
                raise RuntimeError('Queue counter shard is missing; refusing unaccounted handoff')
            stats = stats_snapshot.to_dict()
            _increment_stats(stats, row['outlet'], metrics=_metric_delta(row, metrics))
            archive_remaining = stats.get('archive_remaining', 0) + int(next_phase == 'archive') - int(claim.phase == 'archive')
            if archive_remaining < 0:
                raise RuntimeError('Archive counter is inconsistent')
            stats['archive_remaining'] = archive_remaining
            row.update(state='ready', phase=next_phase, checkpoint_uri=checkpoint_uri,
                       accounted_metrics=metrics, last_handoff=handoff_identity,
                       status='queued', needs_export=False, updated_at=self.clock())
            for field in ('due_at', 'archive_due_at', 'owner', 'claim_token'):
                row.pop(field, None)
            row[next_due] = retry_at
            transaction.set(reference, row)
            transaction.set(stats_ref, stats)
            return True
        return self._transaction(transition)

    def release(self, claim):
        """Requeue only this claim; never release another worker's replacement."""
        reference = self.articles.document(claim.article_id)
        due_field = _due_field(claim.phase)
        def release_owned(transaction):
            snapshot = reference.get(transaction=transaction)
            row = snapshot.to_dict() if snapshot.exists else {}
            if (row.get('state') != 'leased' or row.get('owner') != self.owner
                    or row.get('phase', 'publisher') != claim.phase or row.get('claim_token') != claim.token):
                return False
            row.update(state='ready', updated_at=self.clock())
            row[due_field] = 0.0
            row.pop('owner', None)
            row.pop('claim_token', None)
            transaction.set(reference, row)
            return True
        return self._transaction(release_owned)

    def export_pending(self, limit=1000):
        """Return a bounded durable outbox; evidence can be replayed after a crash."""
        if not 1 <= limit <= 1000:
            raise ValueError('Export batch must contain 1 to 1000 records')
        query = self.articles.where(filter=firestore.FieldFilter('needs_export', '==', True)).limit(limit)
        records = []
        for snapshot in query.stream(timeout=30):
            row = snapshot.to_dict()
            if row.get('state') != 'done' or not row.get('status') or row['status'] in TERMINAL_EXCLUSIONS:
                raise RuntimeError('Nonterminal article found in the result export outbox')
            records.append({'article_id': snapshot.id, 'claim_token': row.get('claim_token'),
                            'result_uri': row['result_uri']})
        return records

    def mark_exported(self, records):
        """Acknowledge only matching immutable results after a successful BQ load.

        Concurrent exporters may harmlessly load the same events; the existing
        BigQuery latest-result view deduplicates them. A failed export leaves its
        outbox records available to any replacement worker.
        """
        records = list(records)
        acknowledged = set()
        for offset in range(0, len(records), 300):
            chunk = records[offset:offset + 300]
            references = [self.articles.document(record['article_id']) for record in chunk]
            def acknowledge(transaction):
                snapshots = {snapshot.id: snapshot for snapshot in self.client.get_all(references, transaction=transaction)}
                changed = set()
                for record, reference in zip(chunk, references):
                    snapshot = snapshots.get(record['article_id'])
                    row = snapshot.to_dict() if snapshot and snapshot.exists else {}
                    if (row.get('state') != 'done' or row.get('status') in TERMINAL_EXCLUSIONS
                            or row.get('result_uri') != record['result_uri']
                            or row.get('claim_token') != record.get('claim_token')):
                        continue
                    if row.get('needs_export'):
                        transaction.update(reference, {'needs_export': False})
                    changed.add(record['article_id'])
                return changed
            acknowledged.update(self._transaction(acknowledge))
        return acknowledged

    def list_workers(self, execution=None, include_expired=True):
        """Read task heartbeats, including finished tasks for cohort completion."""
        query = self.workers
        if execution is not None:
            query = query.where(filter=firestore.FieldFilter('execution', '==', execution))
        elif not include_expired:
            query = query.where(filter=firestore.FieldFilter('expires_at', '>', self.clock()))
        records = [snapshot.to_dict() for snapshot in query.stream(timeout=30)]
        if not include_expired:
            records = [record for record in records if record.get('expires_at', 0) > self.clock()]
        return records

    def summary(self):
        """Read 32 compact counters plus live worker heartbeats, never all URLs.

        Completed totals are authoritative. Active counts are a recent heartbeat
        view and expire after 90 seconds; pending includes stale worker claims so
        a dead worker cannot make the dashboard imply a completed collection.
        """
        control = self.control()
        totals = Counter()
        counts = Counter()
        domains = defaultdict(lambda: {'total': 0, 'processed': 0, 'counts': Counter()})
        snapshots = list(self.client.get_all([self.stats.document(f'{i:02d}') for i in range(SHARDS)]))
        if len(snapshots) != SHARDS or any(not snapshot.exists for snapshot in snapshots):
            raise RuntimeError('Shared queue counters are incomplete')
        for snapshot in snapshots:
            shard = snapshot.to_dict()
            for field in ('total', 'processed', 'archive_remaining') + METRICS:
                totals[field] += shard.get(field, 0)
            counts.update(shard.get('counts', {}))
            for outlet, values in shard.get('domains', {}).items():
                domain = domains[outlet]
                domain['total'] += values.get('total', 0)
                domain['processed'] += values.get('processed', 0)
                domain['counts'].update(values.get('counts', {}))
        now = self.clock()
        workers = []
        active_ids = set()
        active_outlets = Counter()
        query = self.workers.where(filter=firestore.FieldFilter('expires_at', '>', now))
        for snapshot in query.stream(timeout=30):
            worker = snapshot.to_dict()
            workers.append(worker)
            for article in worker.get('active', ()):
                aid = article['article_id']
                if aid not in active_ids and article.get('lease_until', 0) > now:
                    active_ids.add(aid)
                    active_outlets[article['outlet']] += 1
        domain_rows = []
        downloading = 0
        for outlet, values in sorted(domains.items()):
            remaining = max(0, values['total'] - values['processed'])
            active = min(remaining, active_outlets[outlet])
            downloading += active
            domain_rows.append({'outlet': outlet, **dict(values['counts']),
                                'pending': remaining - active, 'downloading': active})
        pending = max(0, totals['total'] - totals['processed'] - downloading)
        return {'run_id': self.run_id, 'country': control['country'],
                'phase': control.get('phase', 'full'), 'state': control['state'],
                'updated_at': datetime.fromtimestamp(now, timezone.utc).isoformat(),
                **dict(totals), 'counts': dict(counts), 'domains': domain_rows,
                'pending': pending, 'downloading': downloading,
                'publisher_remaining': max(0, totals['total'] - totals['processed'] - totals['archive_remaining']),
                'workers': workers,
                'worker_count': len(workers), 'stop': control.get('stop', False),
                'error': control.get('error')}
