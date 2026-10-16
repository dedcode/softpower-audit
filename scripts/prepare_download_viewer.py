"""Prepare scoped API access and bootstrap download indexes for the audit page."""
import argparse
import json
import sys
from pathlib import Path
import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'backend'))
import downloads


def provision(countries):
    runs = []
    for country in countries:
        value = downloads.progress(country)
        if value.get("run_id"):
            runs.append(value["run_id"])
    if not runs:
        raise ValueError("No selected collections have started")
    session = AuthorizedSession(google.auth.default()[0])
    resource = 'https://cloudresourcemanager.googleapis.com/v1/projects/citygraph'
    response = session.post(resource + ':getIamPolicy', json={'options': {'requestedPolicyVersion': 3}}, timeout=30)
    response.raise_for_status()
    policy = response.json()
    policy['version'] = 3
    member = 'serviceAccount:softpower-audit-reader@citygraph.iam.gserviceaccount.com'
    condition = {'title': 'Read crawl metadata database',
                 'expression': 'resource.name == "projects/citygraph/databases/softpower-crawl"'}
    binding = {'role': 'roles/datastore.viewer', 'members': [member], 'condition': condition}
    if not any(b['role'] == binding['role'] and b.get('condition') == condition and member in b['members']
               for b in policy['bindings']):
        policy['bindings'].append(binding)
        response = session.post(resource + ':setIamPolicy', json={'policy': policy}, timeout=30)
        response.raise_for_status()
    bucket = storage.Client(project='citygraph').bucket(downloads.BUCKET)
    policy = bucket.get_iam_policy(requested_policy_version=3)
    policy.version = 3
    prefix = 'projects/_/buckets/' + downloads.BUCKET + '/objects/'
    # IAM supports extract/startsWith/endsWith, not contains/regex. Require the
    # extracted-text directory explicitly, including inside fenced claim paths.
    evidence_access = []
    for run in runs:
        root = prefix + 'runs/' + run + '/'
        evidence_access.extend([
            f'resource.name.startsWith("{root}distributed/results/")',
            f'resource.name.startsWith("{root}checkpoints/")',
            f'resource.name.startsWith("{root}extracted/")',
            f'(resource.name.startsWith("{root}claims/") && '
            'resource.name.extract("/extracted/{article}.txt.gz") != "" && '
            'resource.name.endsWith(".txt.gz"))'])
    index_access = ' || '.join(f'resource.name == "{prefix}downloads/{country}.json.gz"' for country in countries)
    grants = [
        {'role': 'roles/storage.objectViewer', 'members': {member}, 'condition': {
            'title': 'Accepted evidence and extracted text for selected runs',
            'expression': ' || '.join(evidence_access)}},
        {'role': 'roles/storage.objectUser', 'members': {member}, 'condition': {
            'title': 'Selected country download availability indexes only',
            'expression': index_access}}]
    changed = False
    for binding in grants:
        if not any(b['role'] == binding['role'] and b.get('condition') == binding['condition']
                   and member in b['members'] for b in policy.bindings):
            policy.bindings.append(binding)
            changed = True
    if changed:
        bucket.set_iam_policy(policy)
    print('API permissions ready: read-only crawl metadata/text; writes limited to derived download indexes.', flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--country', action='append', choices=sorted(downloads.COUNTRIES))
    args = parser.parse_args()
    if not args.apply:
        print('Use --apply to provision the viewer and build availability indexes. Crawl data is read-only.')
        return
    countries = args.country or ['KE', 'CI']
    provision(countries)
    for country in countries:
        result = downloads.index.get(country, reconcile=True)
        counts = {status: sum(row[1] == status for row in result['entries']) for status in ['saved', 'partial']}
        print(json.dumps({'country': country, 'run_id': result['run_id'], 'entries': len(result['entries']), **counts}), flush=True)


if __name__ == '__main__':
    main()
