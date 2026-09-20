"""Deploy using this Mac's ADC, without storing or displaying long-lived keys."""
import os
import subprocess
import tempfile
from pathlib import Path
import google.auth
from google.auth.transport.requests import Request

ROOT = Path(__file__).resolve().parents[1]
credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
credentials.refresh(Request())
with tempfile.TemporaryDirectory(prefix='softpower-deploy-') as tmp:
    token = Path(tmp)/'access-token'
    token.write_text(credentials.token)
    token.chmod(0o600)
    command = ['gcloud', '--access-token-file='+str(token), '--project=citygraph', 'run', 'deploy',
        'softpower-audit-api', '--source='+str(ROOT/'backend'), '--region=us-central1',
        '--service-account=softpower-audit-reader@citygraph.iam.gserviceaccount.com',
        '--build-service-account=projects/citygraph/serviceAccounts/softpower-audit-builder@citygraph.iam.gserviceaccount.com',
        '--allow-unauthenticated', '--max=1', '--max-instances=1', '--min=0', '--min-instances=0',
        '--concurrency=8', '--cpu=1', '--memory=512Mi', '--timeout=180',
        '--set-env-vars=^|^GOOGLE_CLOUD_PROJECT=citygraph|ALLOWED_ORIGINS=https://dedcode.github.io,https://djelleldifallah.com,https://www.djelleldifallah.com',
        '--quiet', '--format=json']
    # gcloud may print deployment status but is never asked to print credentials.
    result = subprocess.run(command, text=True, stdout=subprocess.PIPE)
    if result.returncode:
        raise SystemExit(result.returncode)
    private = ROOT/'deployment-private'
    private.mkdir(exist_ok=True)
    (private/'cloud-run.json').write_text(result.stdout)
    print('Cloud Run deployment completed. Service metadata saved locally.', flush=True)
