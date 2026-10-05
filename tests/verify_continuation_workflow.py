"""Read-only cloud E2E assertion for verify_continuation.py's two executions.

Run after the verification workflow finishes:
  python tests/verify_continuation_workflow.py WORKFLOW_EXECUTION_NAME BUCKET
The workflow execution name is the full projects/.../executions/... resource.
No real article requests, deployments, or writes are performed by this check.
"""
import argparse
import json
import re

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage


def verify(workflow, summary, executions):
    if workflow.get('state') != 'SUCCEEDED':
        raise AssertionError('Verification workflow did not succeed')
    result = json.loads(workflow['result'])
    run_id = result['run_id']
    if not run_id.startswith('verify-continuation-'):
        raise AssertionError('Expected an isolated continuation verification run')
    if result.get('state') != 'completed' or result.get('executions_finished') != 2:
        raise AssertionError('Coordinator must finish after exactly two executions')
    if len(executions) != 2 or len(set(executions)) != 2:
        raise AssertionError('Fixture must record two distinct Cloud Run executions')
    if result.get('execution') != executions[-1]:
        raise AssertionError('Workflow result must refer to the second execution')
    for field in ('run_id', 'execution', 'state', 'pending', 'processed'):
        if summary.get(field) != result.get(field):
            raise AssertionError(f'Final summary and workflow disagree about {field}')
    if summary.get('downloading') != 0 or summary.get('pending') != 0:
        raise AssertionError('Fixture did not finish all work')
    return {'run_id': run_id, 'executions': executions, 'state': result['state']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('workflow_execution')
    parser.add_argument('bucket')
    args = parser.parse_args()
    if not re.fullmatch(r'projects/[^/]+/locations/[^/]+/workflows/[^/]+/executions/[^/]+', args.workflow_execution):
        parser.error('workflow_execution must be a full Workflows execution resource')
    credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    with AuthorizedSession(credentials) as session:
        response = session.get('https://workflowexecutions.googleapis.com/v1/' + args.workflow_execution, timeout=60)
        response.raise_for_status()
        workflow = response.json()
    if workflow.get('state') != 'SUCCEEDED':
        raise AssertionError('Verification workflow did not succeed: ' + workflow.get('state', 'unknown'))
    run_id = json.loads(workflow['result'])['run_id']
    if not run_id.startswith('verify-continuation-'):
        raise AssertionError('Refusing to inspect a non-verification run')
    project = args.workflow_execution.split('/')[1]
    bucket = storage.Client(project=project, credentials=credentials).bucket(args.bucket)
    prefix = 'runs/' + run_id + '/'
    summary = json.loads(bucket.blob(prefix + 'progress.json').download_as_text())
    executions = json.loads(bucket.blob(prefix + 'verification.json').download_as_text())
    print(json.dumps(verify(workflow, summary, executions), indent=2))


if __name__ == '__main__':
    main()
