"""Release only a terminated distributed cohort's global guard before scaling.

The Firestore queue, completed results, outbox, article claims, and shared host
cooldowns are never changed. Old claims/heartbeats expire normally. The caller
keeps STOP in place until ready to launch the new task count.
"""
import argparse
import json
import re

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage, firestore

from recover_crawl_for_migration import checked, read_json, write_json, validate_terminal
from shared_queue import SharedQueue
from crawl import now

PROJECT = 'citygraph'
REGION = 'us-central1'
BUCKET = 'citygraph-softpower-crawl'
WORKFLOW = 'softpower-crawl-continuation'


def environment(container):
    return {item['name']: item.get('value') for item in container.get('env', [])}


def validate_distributed_execution(execution, args):
    containers = execution.get('template', {}).get('containers', [])
    if len(containers) != 1 or environment(containers[0]).get('CRAWL_DISTRIBUTED') != '1':
        raise RuntimeError('The old execution is not the expected distributed crawler')
    if execution.get('taskCount') != args.from_instances:
        raise RuntimeError('The old execution task count does not match --from-instances')


def validate_target_job(job, args):
    template = job.get('template', {})
    containers = template.get('template', {}).get('containers', [])
    if template.get('taskCount') != args.to_instances or template.get('parallelism') != args.to_instances:
        raise RuntimeError('Deploy the target instance count before releasing the old cohort')
    if len(containers) != 1:
        raise RuntimeError('Unexpected target crawler container configuration')
    env = environment(containers[0])
    if (env.get('CRAWL_DISTRIBUTED') != '1' or env.get('CRAWL_FIRESTORE_DATABASE') != 'softpower-crawl'
            or env.get('CRAWL_BUCKET') != BUCKET):
        raise RuntimeError('Target job must use the existing shared crawl database')
    condition = job.get('terminalCondition', {})
    if condition.get('state') != 'CONDITION_SUCCEEDED' or job.get('reconciling'):
        raise RuntimeError('Target job template is not ready')


def validate_lease(lease, args):
    if lease is not None and (lease.get('run_id') != args.run_id or lease.get('execution') != args.execution
            or lease.get('owner') != args.execution or lease.get('distributed') is not True
            or lease.get('task_count') != args.from_instances):
        raise RuntimeError('Global lease belongs to a different cohort; refusing release')


def check_stop(bucket, args):
    blob = bucket.blob('runs/' + args.run_id + '/STOP')
    blob.reload()
    if int(blob.generation) != args.stop_generation:
        raise RuntimeError('STOP generation changed; another operator may own the pause')


def inspect_cloud(session, args):
    base = f'projects/{PROJECT}/locations/{REGION}'
    job_url = 'https://run.googleapis.com/v2/' + base + '/jobs/softpower-crawler'
    workflow_url = 'https://workflowexecutions.googleapis.com/v1/' + base + '/workflows/' + WORKFLOW + '/executions'
    execution = checked(session.get(job_url + '/executions/' + args.execution, timeout=30))
    workflow = checked(session.get(workflow_url + '/' + args.workflow_execution, params={'view': 'FULL'}, timeout=30))
    validate_terminal(execution, workflow, args.run_id, args.execution)
    validate_distributed_execution(execution, args)
    job = checked(session.get(job_url, timeout=30))
    validate_target_job(job, args)
    active = checked(session.get(workflow_url, params={'filter': 'state="ACTIVE" OR state="QUEUED"',
                                                     'pageSize': 1, 'view': 'BASIC'}, timeout=30))
    if active.get('executions') or active.get('nextPageToken'):
        raise RuntimeError('A continuation coordinator is still active or queued')
    return execution, workflow, job


def release(bucket, queue, session, args):
    check_stop(bucket, args)
    execution, workflow, job = inspect_cloud(session, args)
    prefix = 'runs/' + args.run_id + '/'
    config, config_generation = read_json(bucket, prefix + 'config.json')
    control = queue.control()
    summary = queue.summary()
    if (config.get('run_id') != args.run_id or config.get('workers') != 48
            or control.get('run_id') != args.run_id or control.get('queue_version') != 1
            or control.get('input_count') != config.get('input_count')
            or summary.get('run_id') != args.run_id or summary.get('total') != config.get('input_count')):
        raise RuntimeError('The existing shared queue does not match the 48-slot collection')
    if control.get('stop'):
        raise RuntimeError('The shared queue has a separate stop flag; preserve it for explicit operator review')
    if control.get('state') not in ('ready', 'running', 'continuing'):
        raise RuntimeError('The shared queue is not in a resumable state')
    lease, generation = read_json(bucket, 'control/worker-lease.json', optional=True)
    validate_lease(lease, args)
    proof_path = prefix + 'distributed/scaleovers/' + args.execution + '-to-' + str(args.to_instances) + '.json'
    old_proof, proof_generation = read_json(bucket, proof_path, optional=True)
    if old_proof and any(old_proof.get(field) != getattr(args, field)
                         for field in ('run_id', 'execution', 'workflow_execution', 'from_instances', 'to_instances', 'stop_generation')):
        raise RuntimeError('A different scaleover operation owns the existing proof')
    proof = {**vars(args), 'state': 'verified_terminal', 'updated_at': now(),
             'config_generation': config_generation, 'config': config,
             'terminal_execution': execution, 'terminal_workflow': workflow,
             'target_job': {'name': job['name'], 'generation': job.get('generation'), 'template': job['template']},
             'queue_control': control, 'queue_counts': {key: summary.get(key) for key in
                ('total', 'processed', 'pending', 'downloading', 'counts', 'attempts', 'response_bytes', 'stored_bytes')},
             'lease_before': lease, 'lease_generation': generation,
             'queue_changed': False, 'article_claims_changed': False, 'result_outbox_changed': False,
             'old_claims_requeued': False, 'stop_removed': False}
    proof['before'] = (old_proof or {}).get('before', {'lease': lease, 'lease_generation': generation,
                                                    'queue_counts': proof['queue_counts']})
    if old_proof and old_proof.get('state') == 'released':
        if lease is not None:
            raise RuntimeError('A lease appeared after the recorded release; inspect before retrying')
        return old_proof
    proof_generation = write_json(bucket, proof_path, proof, proof_generation)
    # Repeat terminal and stop checks after the evidence is durable. Read a fresh
    # lease generation immediately before deletion; never delete a replacement.
    check_stop(bucket, args)
    inspect_cloud(session, args)
    current_config, current_config_generation = read_json(bucket, prefix + 'config.json')
    if current_config_generation != config_generation or current_config != config:
        raise RuntimeError('Collection configuration changed during scaleover')
    lease, generation = read_json(bucket, 'control/worker-lease.json', optional=True)
    validate_lease(lease, args)
    if lease is not None:
        bucket.blob('control/worker-lease.json').delete(if_generation_match=generation, timeout=30)
    proof.update(state='released', updated_at=now(), released_lease_generation=generation if lease else None,
                 proof_uri='gs://' + bucket.name + '/' + proof_path)
    write_json(bucket, proof_path, proof, proof_generation)
    return proof


def arguments(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--execution', required=True)
    parser.add_argument('--workflow-execution', required=True)
    parser.add_argument('--from-instances', required=True, type=int)
    parser.add_argument('--to-instances', required=True, type=int)
    parser.add_argument('--stop-generation', required=True, type=int)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args(argv)
    for field in ('run_id', 'execution', 'workflow_execution'):
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', getattr(args, field)):
            parser.error('Invalid ' + field)
    if min(args.from_instances, args.to_instances, args.stop_generation) < 1:
        parser.error('Instance counts and STOP generation must be positive')
    return args


def main(argv=None):
    args = arguments(argv)
    if not args.apply:
        print(json.dumps({**vars(args), 'note': 'No changes. Only releases the verified terminated cohort lease; STOP and the shared queue remain unchanged.'}))
        return
    credentials, _ = google.auth.default()
    bucket = storage.Client(project=PROJECT, credentials=credentials).bucket(BUCKET)
    client = firestore.Client(project=PROJECT, credentials=credentials, database='softpower-crawl')
    queue = SharedQueue(client, args.run_id, 'scaleover-audit')
    proof = release(bucket, queue, AuthorizedSession(credentials), args)
    print(json.dumps({key: proof[key] for key in ('run_id', 'execution', 'state', 'from_instances', 'to_instances',
                     'queue_counts', 'queue_changed', 'stop_removed', 'proof_uri')}))


if __name__ == '__main__':
    main()
