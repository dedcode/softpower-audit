"""Recover a cancelled legacy execution from durable checkpoints for migration.

This is an explicit operator action, not a crawler retry. It never describes the
cancelled execution as having completed cleanly and never marks active URLs done.
"""
import argparse
import gzip
import hashlib
import io
import json
import re
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.api_core.exceptions import NotFound
from google.cloud import storage, bigquery

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from crawl import key, now
from result_store import ResultIndex, JsonlBatch, encode_jsonl, iter_input_rows

PROJECT = 'citygraph'
REGION = 'us-central1'
BUCKET = 'citygraph-softpower-crawl'
WORKFLOW = 'softpower-crawl-continuation'


def checked(response):
    response.raise_for_status()
    return response.json()


def validate_terminal(execution, workflow, run_id, execution_id):
    name = execution.get('name', '')
    if not name.endswith('/jobs/softpower-crawler/executions/' + execution_id):
        raise RuntimeError('Cloud Run returned a different execution')
    completed = next((condition for condition in execution.get('conditions', []) if condition.get('type') == 'Completed'), {})
    if (not execution.get('completionTime') or execution.get('runningCount', 0)
            or completed.get('state') not in ('CONDITION_SUCCEEDED', 'CONDITION_FAILED')):
        raise RuntimeError('The exact old Cloud Run execution is not terminal')
    if workflow.get('state') not in ('SUCCEEDED', 'FAILED', 'CANCELLED') or not workflow.get('endTime'):
        raise RuntimeError('The old continuation workflow is not terminal')
    try:
        argument = json.loads(workflow['argument'])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError('Cannot verify the old workflow run') from exc
    if argument.get('run_id') != run_id:
        raise RuntimeError('The old workflow belongs to a different collection')
    env = execution.get('template', {}).get('containers', [{}])[0].get('env', [])
    if next((item.get('value') for item in env if item.get('name') == 'RUN_ID'), None) != run_id:
        raise RuntimeError('The old Cloud Run execution belongs to a different collection')


def validate_lease(lease, run_id, execution_id, owner):
    if lease is None:
        return
    if (lease.get('run_id') != run_id or lease.get('execution') != execution_id
            or lease.get('owner') != owner or lease.get('distributed')):
        raise RuntimeError('The collection lease is not owned by the exact cancelled legacy worker')


def snapshot_from_checkpoints(previous, config, manifest, done, execution_id, proof_uri):
    if any(article_id not in manifest for article_id in done.entries):
        raise RuntimeError('A durable result is outside the original input manifest')
    totals = done.snapshot()
    domains = defaultdict(Counter, totals['domains'])
    for article_id, item in manifest.items():
        if article_id not in done:
            domains[item['outlet']]['pending'] += 1
    result = {**previous, **totals,
              'run_id': config['run_id'], 'country': config['country'], 'phase': config['phase'],
              'execution': execution_id, 'state': 'stopped_for_migration', 'updated_at': now(),
              'total': len(manifest), 'processed': len(done), 'pending': len(manifest) - len(done),
              'downloading': 0, 'active_stages': [], 'error': None,
              'domains': [{'outlet': outlet, **dict(counts)} for outlet, counts in sorted(domains.items())],
              'migration_reason': 'Old execution was cancelled after STOP; unfinished URLs remain pending. Counts were rebuilt only from durable checkpoints.',
              'migration_proof': proof_uri}
    # These belonged to a running process and must not imply a live worker.
    for field in ('workers', 'active_instances', 'worker_count'):
        result.pop(field, None)
    return result


def read_json(bucket, path, optional=False):
    blob = bucket.blob(path)
    try:
        blob.reload()
        generation = blob.generation
        return json.loads(blob.download_as_text(if_generation_match=generation, timeout=30)), generation
    except NotFound:
        if optional:
            return None, 0
        raise


def write_json(bucket, path, value, generation):
    blob = bucket.blob(path)
    blob.upload_from_string(json.dumps(value, separators=(',', ':')), content_type='application/json',
                            if_generation_match=generation, timeout=60)
    return blob.generation


def terminal_evidence(session, args):
    base = f'projects/{PROJECT}/locations/{REGION}'
    execution = checked(session.get('https://run.googleapis.com/v2/' + base + '/jobs/softpower-crawler/executions/' + args.execution, timeout=30))
    workflow = checked(session.get('https://workflowexecutions.googleapis.com/v1/' + base + '/workflows/' + WORKFLOW + '/executions/' + args.workflow_execution,
                                   params={'view': 'FULL'}, timeout=30))
    validate_terminal(execution, workflow, args.run_id, args.execution)
    return execution, workflow


def checkpoint_inventory(bucket, prefix):
    return sorted((blob.name, int(blob.generation)) for blob in bucket.list_blobs(prefix=prefix + 'checkpoints/'))


def recover(bucket, bq, session, args):
    prefix = 'runs/' + args.run_id + '/'
    if not bucket.blob(prefix + 'STOP').exists():
        raise RuntimeError('STOP must remain present throughout migration recovery')
    execution, workflow = terminal_evidence(session, args)
    lease, lease_generation = read_json(bucket, 'control/worker-lease.json', optional=True)
    validate_lease(lease, args.run_id, args.execution, args.lease_owner)
    config, config_generation = read_json(bucket, prefix + 'config.json')
    if config.get('run_id') != args.run_id or config.get('input_count') != args.expected_count:
        raise RuntimeError('Run configuration does not match the expected input count')
    previous, progress_generation = read_json(bucket, prefix + 'progress.json')
    if previous.get('run_id') != args.run_id or previous.get('execution') != args.execution:
        raise RuntimeError('Existing progress belongs to a different execution')
    manifest = {}
    input_blob = bucket.blob(prefix + 'inputs.json.gz')
    input_blob.reload()
    input_generation = input_blob.generation
    with input_blob.open('rb') as raw, gzip.GzipFile(fileobj=raw) as decoded, io.TextIOWrapper(decoded, encoding='utf-8') as stream:
        for row in iter_input_rows(stream):
            article_id = key(row['url'])
            if article_id in manifest:
                raise RuntimeError('Duplicate URL in input manifest')
            manifest[article_id] = row
    if len(manifest) != args.expected_count:
        raise RuntimeError(f'Expected {args.expected_count} manifest URLs, found {len(manifest)}')
    inventory = checkpoint_inventory(bucket, prefix)
    proof_path = prefix + 'distributed/migrations/' + args.execution + '.json'
    proof_uri = 'gs://' + bucket.name + '/' + proof_path
    prior_proof, proof_generation = read_json(bucket, proof_path, optional=True)
    if prior_proof and (prior_proof.get('run_id') != args.run_id or prior_proof.get('execution') != args.execution
                        or prior_proof.get('lease_owner') != args.lease_owner):
        raise RuntimeError('Existing migration proof belongs to another worker')
    if prior_proof and prior_proof.get('state') == 'ready_for_import':
        validate_import_proof(bucket, previous, config)
        if (lease is not None or prior_proof.get('checkpoint_inventory') != [list(item) for item in inventory]
                or prior_proof.get('input_generation') != input_generation):
            raise RuntimeError('A finalized migration proof no longer matches the unchanged durable source')
        return {'run_id': args.run_id, 'state': previous['state'], 'processed': previous['processed'],
                'pending': previous['pending'], 'replayed_events': prior_proof['replayed_events'], 'proof': proof_uri}
    proof = {**(prior_proof or {}), 'run_id': args.run_id, 'execution': args.execution,
             'workflow_execution': args.workflow_execution, 'lease_owner': args.lease_owner,
             'state': 'replaying_checkpoints', 'updated_at': now(), 'input_count': len(manifest),
             'input_generation': input_generation, 'config_generation': config_generation,
             'manifest_sha256': hashlib.sha256(''.join(sorted(manifest)).encode()).hexdigest(),
             'checkpoint_inventory': inventory, 'terminal_execution': execution, 'terminal_workflow': workflow}
    proof.setdefault('before', {'progress': previous, 'progress_generation': progress_generation,
                                'lease': lease, 'lease_generation': lease_generation})
    proof_generation = write_json(bucket, proof_path, proof, proof_generation)
    done = ResultIndex()
    batch = JsonlBatch(1000, 8 * 1024 * 1024)
    load_jobs = []
    event_count = 0

    def flush():
        if not batch:
            return
        job = bq.load_table_from_file(io.BytesIO(batch.data()), config['dataset'] + '.crawl_result_events',
            job_config=bigquery.LoadJobConfig(ignore_unknown_values=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON))
        job.result(timeout=180)
        load_jobs.append(job.job_id)
        batch.clear()

    def read_checkpoint(entry):
        path, generation = entry
        blob = bucket.blob(path, generation=generation)
        return gzip.decompress(blob.download_as_bytes(if_generation_match=generation, timeout=60))

    with ThreadPoolExecutor(max_workers=8) as pool:
        # Limit prefetched checkpoint bodies to eight objects, not the whole run.
        for offset in range(0, len(inventory), 8):
            for body in pool.map(read_checkpoint, inventory[offset:offset + 8]):
                for line in body.splitlines():
                    if not line.strip():
                        continue
                    result = json.loads(line)
                    article_id = result.get('article_id')
                    item = manifest.get(article_id)
                    if (not item or result.get('run_id') != args.run_id or result.get('url') != item['url']
                            or result.get('outlet') != item['outlet']):
                        raise RuntimeError('Checkpoint result does not match its manifest URL, outlet, and run')
                    done.record(result)
                    event = dict(result)
                    event['attempts_json'] = json.dumps(event.pop('attempts', []), separators=(',', ':'))
                    encoded = encode_jsonl(event)
                    if not batch.fits(encoded):
                        flush()
                    batch.append(encoded)
                    event_count += 1
    flush()
    # No state/lease mutation until every durable checkpoint event is replayed.
    terminal_evidence(session, args)
    if not bucket.blob(prefix + 'STOP').exists():
        raise RuntimeError('STOP disappeared during recovery')
    if checkpoint_inventory(bucket, prefix) != inventory:
        raise RuntimeError('Checkpoint inventory changed during recovery')
    current_config, current_config_generation = read_json(bucket, prefix + 'config.json')
    if current_config_generation != config_generation:
        raise RuntimeError('Run configuration changed during recovery')
    current_input = bucket.blob(prefix + 'inputs.json.gz')
    current_input.reload()
    if current_input.generation != input_generation:
        raise RuntimeError('Input manifest changed during recovery')
    snapshot = snapshot_from_checkpoints(previous, config, manifest, done, args.execution, proof_uri)
    snapshot_generation = write_json(bucket, prefix + 'progress.json', snapshot, progress_generation)
    public, public_generation = read_json(bucket, 'progress/' + config['country'] + '.json', optional=True)
    if public is not None and (public.get('run_id') != args.run_id or public.get('execution') != args.execution):
        raise RuntimeError('Public progress belongs to another execution; refusing to overwrite it')
    write_json(bucket, 'progress/' + config['country'] + '.json', snapshot, public_generation)
    bq.load_table_from_json([{'run_id': args.run_id, 'country': config['country'], 'updated_at': snapshot['updated_at'],
        'state': snapshot['state'], 'config_json': json.dumps(config), 'summary_json': json.dumps(snapshot)}],
        config['dataset'] + '.crawl_run_events').result(timeout=180)
    # Re-read the exact owner and generation immediately before releasing only
    # this terminal execution's orphaned lease. Never remove a replacement's.
    final_lease, final_generation = read_json(bucket, 'control/worker-lease.json', optional=True)
    validate_lease(final_lease, args.run_id, args.execution, args.lease_owner)
    if final_lease is not None:
        bucket.blob('control/worker-lease.json').delete(if_generation_match=final_generation, timeout=30)
    proof.update(state='ready_for_import', updated_at=now(), replayed_events=event_count, bq_load_jobs=load_jobs,
                 after={'state': snapshot['state'], 'processed': len(done), 'pending': snapshot['pending'],
                        'downloading': 0, 'counts': dict(snapshot['counts']), 'progress_generation': snapshot_generation},
                 released_lease_generation=final_generation if final_lease else None)
    write_json(bucket, proof_path, proof, proof_generation)
    return {'run_id': args.run_id, 'state': snapshot['state'], 'processed': len(done), 'pending': snapshot['pending'],
            'replayed_events': event_count, 'proof': proof_uri}


def validate_import_proof(bucket, previous, config):
    """The queue importer accepts interrupted recovery only with explicit proof."""
    if previous.get('state') != 'stopped_for_migration':
        if previous.get('state') not in ('completed', 'paused_by_operator', 'paused_limit') or previous.get('error'):
            raise RuntimeError('Interrupted or failed execution requires explicit checkpoint recovery proof before import')
        return
    prefix = 'runs/' + config['run_id'] + '/'
    if not bucket.blob(prefix + 'STOP').exists():
        raise RuntimeError('STOP must remain present until migration import finishes')
    expected_prefix = 'gs://' + bucket.name + '/runs/' + config['run_id'] + '/distributed/migrations/'
    uri = previous.get('migration_proof', '')
    if not uri.startswith(expected_prefix) or not uri.endswith('.json'):
        raise RuntimeError('Stopped-for-migration snapshot lacks a valid recovery proof')
    proof, _ = read_json(bucket, uri[len('gs://' + bucket.name + '/'):])
    after = proof.get('after', {})
    if (proof.get('state') != 'ready_for_import' or proof.get('run_id') != config['run_id']
            or proof.get('execution') != previous.get('execution') or proof.get('input_count') != config['input_count']
            or after.get('processed') != previous.get('processed') or after.get('pending') != previous.get('pending')
            or after.get('counts') != previous.get('counts') or after.get('downloading') != 0
            or previous.get('downloading') != 0 or previous.get('total') != config['input_count']
            or previous.get('processed', 0) + previous.get('pending', 0) != config['input_count']):
        raise RuntimeError('Recovery proof does not match the stopped-for-migration snapshot')
    live_progress, progress_generation = read_json(bucket, prefix + 'progress.json')
    live_config, config_generation = read_json(bucket, prefix + 'config.json')
    inputs = bucket.blob(prefix + 'inputs.json.gz')
    inputs.reload()
    if (live_progress != previous or progress_generation != after.get('progress_generation')
            or live_config != config or config_generation != proof.get('config_generation')
            or inputs.generation != proof.get('input_generation')
            or [list(item) for item in checkpoint_inventory(bucket, prefix)] != proof.get('checkpoint_inventory')):
        raise RuntimeError('Recovery proof source generations changed before import')


def arguments(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--execution', required=True)
    parser.add_argument('--workflow-execution', required=True)
    parser.add_argument('--lease-owner', required=True)
    parser.add_argument('--expected-count', required=True, type=int)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args(argv)
    for field in ('run_id', 'execution', 'workflow_execution', 'lease_owner'):
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', getattr(args, field)):
            parser.error('Invalid ' + field)
    if args.expected_count < 1:
        parser.error('--expected-count must be positive')
    return args


def main(argv=None):
    args = arguments(argv)
    if not args.apply:
        print(json.dumps({**vars(args), 'note': 'No changes. Requires STOP and the exact old execution/workflow to be terminal.'}))
        return
    credentials, _ = google.auth.default()
    bucket = storage.Client(project=PROJECT, credentials=credentials).bucket(BUCKET)
    bq = bigquery.Client(project=PROJECT, credentials=credentials)
    result = recover(bucket, bq, AuthorizedSession(credentials), args)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
