"""Bounded queue repair after a fenced crawler handover has stopped execution.

The caller first verifies its GCS STOP/guard reservation and termination of the
old execution, prepares new immutable checkpoints with
``migrate_legacy_archive_retry``, then supplies their exact evidence URIs here.
This module never retrieves articles, changes accounting, or resumes a crawl.
"""
import math
import re
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from retrying import migrate_legacy_archive_retry
from shared_queue import PHASE_DUE_FIELDS, SHARDS

MAX_BATCH = 48


def _checkpoint_uri(uri, run_id, article_id):
    if not isinstance(uri, str):
        raise ValueError('A checkpoint evidence URI is required')
    parsed = urlsplit(uri)
    prefix = '/runs/' + run_id + '/distributed/checkpoints/' + article_id + '/'
    name = parsed.path[len(prefix):] if parsed.path.startswith(prefix) else ''
    if (parsed.scheme != 'gs' or not re.fullmatch(r'[a-z0-9][a-z0-9._-]*', parsed.netloc)
            or parsed.query or parsed.fragment
            or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*\.json\.gz', name)):
        raise ValueError('Checkpoint URI must identify this run and article')
    return parsed.netloc


def migrate_paused_publisher_retries(queue, expected_stop_reason, old_execution, replacements, now=None):
    """Move at most 48 prepared legacy retries to archive in one transaction.

    ``replacements`` contain article_id, expected_checkpoint_uri, and
    new_checkpoint_uri. Their new artifacts must already be durably written.
    The old execution must have been independently verified terminal. Changed
    rows and unrelated leases are left untouched; repeated calls are harmless.
    """
    if not isinstance(expected_stop_reason, str) or not expected_stop_reason.strip():
        raise ValueError('An exact owned STOP reason is required')
    if (not isinstance(old_execution, str)
            or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', old_execution)
            or old_execution.startswith('handover-')):
        raise ValueError('A terminated crawler execution ID is required')
    replacements = list(replacements)
    if not 1 <= len(replacements) <= MAX_BATCH:
        raise ValueError('Migration batches must contain 1 to 48 replacements')
    at = float(queue.clock() if now is None else now)
    if not math.isfinite(at) or at < 0:
        raise ValueError('Migration time must be a finite nonnegative epoch')
    prepared = {}
    for replacement in replacements:
        aid = replacement.get('article_id')
        if not isinstance(aid, str) or not re.fullmatch(r'[0-9a-f]{64}', aid) or aid in prepared:
            raise ValueError('Article IDs must be unique SHA256 keys')
        previous = replacement.get('expected_checkpoint_uri')
        following = replacement.get('new_checkpoint_uri')
        if (_checkpoint_uri(previous, queue.run_id, aid) != _checkpoint_uri(following, queue.run_id, aid)
                or previous == following):
            raise ValueError('Replacement must be a new checkpoint in the same bucket')
        prepared[aid] = dict(replacement)
    article_refs = {aid: queue.articles.document(aid) for aid in prepared}
    shard_refs = {f'{int(aid[:8], 16) % SHARDS:02d}':
                  queue.stats.document(f'{int(aid[:8], 16) % SHARDS:02d}') for aid in prepared}

    def transition(transaction):
        # Firestore requires every read before writes. One bulk read also
        # avoids a serial RPC for each article and its counter shard.
        references = [queue.run_ref, *article_refs.values(), *shard_refs.values()]
        snapshots = {snapshot.reference.path: snapshot for snapshot in
                     queue.client.get_all(references, transaction=transaction, timeout=30)}
        control_snapshot = snapshots.get(queue.run_ref.path)
        control = control_snapshot.to_dict() if control_snapshot and control_snapshot.exists else {}
        if (control.get('run_id') != queue.run_id or control.get('stop') is not True
                or control.get('stop_reason') != expected_stop_reason
                or control.get('execution') not in (None, old_execution)):
            raise RuntimeError('The exact owned stopped queue control is required')
        report = {'run_id': queue.run_id, 'at': at, 'migrated': [], 'already_migrated': [], 'skipped': {}}
        rows = {}
        increments = Counter()
        for aid, replacement in prepared.items():
            snapshot = snapshots.get(article_refs[aid].path)
            row = snapshot.to_dict() if snapshot and snapshot.exists else None
            reason = None
            if row is None:
                reason = 'missing'
            elif row.get('state') == 'done':
                reason = 'done'
            elif row.get('phase', 'publisher') == 'archive' and row.get('checkpoint_uri') == replacement['new_checkpoint_uri']:
                report['already_migrated'].append(aid)
                continue
            elif row.get('phase', 'publisher') != 'publisher':
                reason = 'phase_changed'
            elif row.get('checkpoint_uri') != replacement['expected_checkpoint_uri']:
                reason = 'checkpoint_changed'
            elif row.get('state') not in ('ready', 'leased'):
                reason = 'state_changed'
            elif row.get('owner') and not str(row['owner']).startswith(old_execution + '-'):
                reason = 'foreign_owner'
            elif row.get('state') == 'leased' and not str(row.get('owner', '')).startswith(old_execution + '-'):
                reason = 'foreign_owner'
            if reason:
                report['skipped'][aid] = reason
                continue
            rows[aid] = row
            increments[f'{int(aid[:8], 16) % SHARDS:02d}'] += 1
        counters = {}
        for shard, count in increments.items():
            snapshot = snapshots.get(shard_refs[shard].path)
            if not snapshot or not snapshot.exists:
                raise RuntimeError('Counter shard is missing; refusing unaccounted migration')
            stats = snapshot.to_dict()
            remaining = stats.get('archive_remaining', 0)
            if type(remaining) is not int or remaining < 0:
                raise RuntimeError('Archive counter is inconsistent')
            stats['archive_remaining'] = remaining + count
            counters[shard] = stats
        for aid, row in rows.items():
            # Ready rows may carry a deliberate server/service retry pause.
            # A retired lease expiry is ownership metadata, not such a pause.
            due = max(at, float(row.get('due_at', at))) if row['state'] == 'ready' else at
            if not math.isfinite(due):
                raise RuntimeError('Article retry time is inconsistent')
            row.update(state='ready', phase='archive', checkpoint_uri=prepared[aid]['new_checkpoint_uri'],
                       archive_due_at=due, updated_at=at, status='queued', needs_export=False,
                       last_phase_migration={'old_checkpoint_uri': prepared[aid]['expected_checkpoint_uri'],
                                             'new_checkpoint_uri': prepared[aid]['new_checkpoint_uri'],
                                             'old_execution': old_execution, 'reason': expected_stop_reason,
                                             'at': at})
            for field in (*PHASE_DUE_FIELDS.values(), 'owner', 'claim_token', 'lease_until', 'last_handoff'):
                if field != 'archive_due_at':
                    row.pop(field, None)
            transaction.set(article_refs[aid], row)
            report['migrated'].append(aid)
        for shard, stats in counters.items():
            transaction.set(shard_refs[shard], stats)
        return report

    return queue._transaction(transition)
