"""Atomic crawler guard handover; callers verify execution termination/identity.

The guard is replaced in place, never deleted. No article queue state changes.
"""
import json
import re
from datetime import datetime, timedelta, timezone

from google.api_core.exceptions import NotFound, PreconditionFailed

LEASE_PATH = 'control/worker-lease.json'


def _identifier(value, label):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', value):
        raise ValueError('Invalid ' + label)


def _read(bucket):
    blob = bucket.blob(LEASE_PATH)
    try:
        blob.reload()
        generation = blob.generation
        lease = json.loads(blob.download_as_text(if_generation_match=generation))
    except (NotFound, PreconditionFailed) as exc:
        raise RuntimeError('Crawler guard disappeared or changed; inspect before retrying handover') from exc
    if not isinstance(lease, dict):
        raise RuntimeError('Crawler guard is malformed')
    return blob, lease, generation


def _future(lease):
    try:
        expiry = datetime.fromisoformat(lease['expires_at'])
        return expiry.tzinfo is not None and expiry > datetime.now(timezone.utc)
    except (KeyError, TypeError, ValueError):
        return False


def _cohort(lease, run_id, execution):
    return (lease.get('run_id') == run_id and lease.get('execution') == execution
            and lease.get('owner') == execution and lease.get('distributed') is True
            and type(lease.get('task_count')) is int and lease['task_count'] > 0)


def _reservation(lease, run_id, token):
    return (_cohort(lease, run_id, 'handover-' + token)
            and lease.get('handover_id') == token and _future(lease))


def validate_reservation(bucket, run_id, token):
    """Read and validate a live reservation without changing its generation."""
    _identifier(run_id, 'run ID'); _identifier(token, 'handover ID')
    _, lease, generation = _read(bucket)
    if not _reservation(lease, run_id, token):
        raise RuntimeError('The crawler guard is not this live handover reservation')
    return {'lease': lease, 'generation': generation, 'reused': True}


def _write(blob, lease, generation):
    try:
        blob.upload_from_string(json.dumps(lease), content_type='application/json',
                                if_generation_match=generation, timeout=30)
    except PreconditionFailed as exc:
        raise RuntimeError('Crawler guard changed during handover; no replacement was made') from exc
    return {'lease': lease, 'generation': blob.generation, 'reused': False}


def reserve(bucket, run_id, expected_execution, token):
    """Replace only the specified old cohort with a 30-minute reservation."""
    for value, label in ((run_id, 'run ID'), (expected_execution, 'old execution'), (token, 'handover ID')):
        _identifier(value, label)
    if expected_execution.startswith('handover-'):
        raise ValueError('The old execution must be a real crawler cohort')
    blob, old, generation = _read(bucket)
    if _reservation(old, run_id, token) and old.get('previous_execution') == expected_execution:
        return {'lease': old, 'generation': generation, 'reused': True}
    if not _cohort(old, run_id, expected_execution):
        raise RuntimeError('Crawler guard belongs to a different cohort; refusing reservation')
    now = datetime.now(timezone.utc)
    lease = {'owner': 'handover-' + token, 'execution': 'handover-' + token,
             'run_id': run_id, 'distributed': True, 'task_count': old['task_count'],
             'handover_id': token, 'previous_execution': expected_execution,
             'created_at': now.isoformat(), 'expires_at': (now + timedelta(minutes=30)).isoformat()}
    return _write(blob, lease, generation)


def transfer(bucket, run_id, token, new_execution, task_count):
    """Replace this reservation with the verified new cohort's 10-minute guard."""
    for value, label in ((run_id, 'run ID'), (token, 'handover ID'), (new_execution, 'new execution')):
        _identifier(value, label)
    if new_execution.startswith('handover-') or type(task_count) is not int or task_count < 1:
        raise ValueError('A real new cohort and positive task count are required')
    blob, old, generation = _read(bucket)
    # Worker renewals intentionally discard handover metadata. Exact cohort,
    # task count, and future expiry still prove an already successful transfer.
    if _cohort(old, run_id, new_execution) and old['task_count'] == task_count and _future(old):
        if old.get('handover_id') not in (None, token):
            raise RuntimeError('The new cohort was assigned by a different handover')
        return {'lease': old, 'generation': generation, 'reused': True}
    if not _reservation(old, run_id, token):
        raise RuntimeError('The crawler guard is not this live handover reservation')
    if old.get('previous_execution') == new_execution:
        raise RuntimeError('A retired cohort cannot receive its own handover')
    now = datetime.now(timezone.utc)
    lease = {'owner': new_execution, 'execution': new_execution, 'run_id': run_id,
             'distributed': True, 'task_count': task_count, 'handover_id': token,
             'previous_execution': old.get('previous_execution'), 'transferred_at': now.isoformat(),
             'expires_at': (now + timedelta(minutes=10)).isoformat()}
    return _write(blob, lease, generation)
