"""Provision scoped identities for the explicitly requested public dashboard."""
import json
import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery

PROJECT = 'citygraph'
TABLE = 'citygraph.softpower.outlet_daily_2015_2025_20260919_192426'
credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
session = AuthorizedSession(credentials)

for account, title in [('softpower-audit-reader', 'Read-only China news audit API'),
                       ('softpower-audit-builder', 'Build the China news audit API')]:
    email = f'{account}@{PROJECT}.iam.gserviceaccount.com'
    response = session.get(f'https://iam.googleapis.com/v1/projects/{PROJECT}/serviceAccounts/{email}', timeout=60)
    if response.status_code == 404:
        response = session.post(f'https://iam.googleapis.com/v1/projects/{PROJECT}/serviceAccounts',
            json={'accountId': account, 'serviceAccount': {'displayName': title}}, timeout=60)
    response.raise_for_status()
    print('Service account ready:', email, flush=True)

url = f'https://cloudresourcemanager.googleapis.com/v1/projects/{PROJECT}'
response = session.post(url+':getIamPolicy', json={'options': {'requestedPolicyVersion': 3}}, timeout=60)
response.raise_for_status()
policy = response.json()
grants = [('roles/bigquery.jobUser', 'softpower-audit-reader'),
          ('roles/run.builder', 'softpower-audit-builder')]
for role, account in grants:
    member = f'serviceAccount:{account}@{PROJECT}.iam.gserviceaccount.com'
    binding = next((b for b in policy['bindings'] if b['role'] == role and 'condition' not in b), None)
    if binding is None:
        binding = {'role': role, 'members': []}
        policy['bindings'].append(binding)
    if member not in binding['members']:
        binding['members'].append(member)
response = session.post(url+':setIamPolicy', json={'policy': policy}, timeout=60)
response.raise_for_status()
print('Scoped project roles configured; existing IAM preserved.', flush=True)

client = bigquery.Client(project=PROJECT)
policy = client.get_iam_policy(TABLE)
member = 'serviceAccount:softpower-audit-reader@citygraph.iam.gserviceaccount.com'
if not any(b['role'] == 'roles/bigquery.dataViewer' and member in b['members'] for b in policy.bindings):
    policy.bindings.append({'role': 'roles/bigquery.dataViewer', 'members': {member}})
    client.set_iam_policy(TABLE, policy)
print('Runtime read access granted only on the extracted daily table.', flush=True)
