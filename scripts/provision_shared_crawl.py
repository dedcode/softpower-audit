"""Provision the dedicated shared-queue database; no changes without --apply.

Firestore scopes server-client IAM access to a database through a conditional
project binding, rather than a database setIamPolicy endpoint. The condition
below grants data access only to this dedicated queue, never every database.
"""
import argparse
import json
import subprocess
import tempfile
from pathlib import Path

import google.auth
from google.auth.transport.requests import AuthorizedSession, Request

PROJECT = 'citygraph'
REGION = 'us-central1'
DATABASE = 'softpower-crawl'
WORKER = 'serviceAccount:softpower-crawler@' + PROJECT + '.iam.gserviceaccount.com'
RESOURCE = f'projects/{PROJECT}/databases/{DATABASE}'
CONDITION = {
    'title': 'Shared crawl queue database only',
    'description': 'Worker data access is restricted to the dedicated crawl queue.',
    'expression': f'resource.name == "{RESOURCE}"',
}


def checked(response):
    response.raise_for_status()
    return response.json()


def validate_database(data):
    expected = {'name': RESOURCE, 'locationId': REGION, 'type': 'FIRESTORE_NATIVE'}
    mismatch = {key: data.get(key) for key, value in expected.items() if data.get(key) != value}
    if mismatch:
        raise RuntimeError('Existing queue database does not match the required resource, region, or mode: ' + json.dumps(mismatch))
    return {key: data[key] for key in expected}


def grant_worker_database_access(session):
    project_url = 'https://cloudresourcemanager.googleapis.com/v1/projects/' + PROJECT
    policy = checked(session.post(project_url + ':getIamPolicy', json={'options': {'requestedPolicyVersion': 3}}, timeout=30))
    policy['version'] = 3
    bindings = policy.setdefault('bindings', [])
    binding = next((item for item in bindings if item['role'] == 'roles/datastore.user' and item.get('condition') == CONDITION), None)
    if binding is not None and WORKER in binding.get('members', []):
        return False
    if binding is None:
        binding = {'role': 'roles/datastore.user', 'members': [], 'condition': dict(CONDITION)}
        bindings.append(binding)
    binding['members'].append(WORKER)
    # Preserve the etag: a concurrent IAM edit fails rather than being overwritten.
    checked(session.post(project_url + ':setIamPolicy', json={'policy': policy}, timeout=30))
    return True


def provision(session, command):
    command(['services', 'enable', 'firestore.googleapis.com', '--quiet'])
    url = 'https://firestore.googleapis.com/v1/' + RESOURCE
    response = session.get(url, timeout=30)
    if response.status_code == 404:
        command(['firestore', 'databases', 'create', '--database=' + DATABASE,
                 '--location=' + REGION, '--type=firestore-native', '--delete-protection', '--quiet'])
        response = session.get(url, timeout=30)
    data = validate_database(checked(response))
    changed = grant_worker_database_access(session)
    return {'database': data, 'worker': WORKER, 'iam_condition': CONDITION['expression'], 'iam_updated': changed}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true', help='Enable Firestore, create the queue database if absent, and grant database-scoped worker access')
    args = parser.parse_args(argv)
    if not args.apply:
        print(json.dumps({'apply': False, 'database': RESOURCE, 'region': REGION, 'worker': WORKER,
                          'iam_condition': CONDITION['expression'], 'note': 'No cloud changes; pass --apply to provision.'}))
        return
    credentials, _ = google.auth.default()
    credentials.refresh(Request())
    session = AuthorizedSession(credentials)
    with tempfile.TemporaryDirectory(prefix='shared-crawl-provision-') as directory:
        token = Path(directory) / 'token'
        token.write_text(credentials.token)
        token.chmod(0o600)
        base = ['gcloud', '--access-token-file=' + str(token), '--project=' + PROJECT]

        def command(parts):
            subprocess.run(base + parts, check=True)

        print(json.dumps(provision(session, command)))


if __name__ == '__main__':
    main()
