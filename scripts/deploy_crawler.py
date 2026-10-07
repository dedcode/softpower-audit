"""Deploy the approved Cloud Run Job. Execution requires a separate --execute flag."""
import argparse
import json
import math
import re
import subprocess
import tempfile
from pathlib import Path

import google.auth
from google.auth.transport.requests import Request

ROOT = Path(__file__).resolve().parents[1]
PROJECT = 'citygraph'
REGION = 'us-central1'


def arguments(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--deploy', action='store_true')
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--direct', action='store_true', help='One execution only, for explicit pilot/verification runs')
    parser.add_argument('--cpu', type=int, default=1)
    parser.add_argument('--memory-gib', type=int, default=1)
    parser.add_argument('--heavy-slots', type=int, default=1)
    parser.add_argument('--browser-slots', type=int, default=1)
    parser.add_argument('--instances', type=int, default=1, help='Cloud Run tasks sharing one crawl queue; each uses the run configuration workers count')
    parser.add_argument('--host-concurrency', type=int, default=4, help='Fleet-wide simultaneous HTTP downloads per hostname')
    parser.add_argument('--archive-concurrency', type=int, default=4, help='Fleet-wide simultaneous downloads per archive hostname')
    parser.add_argument('--archive-slots', type=int, default=2, help='Maximum archive article slots per task, borrowing idle publisher capacity')
    parser.add_argument('--request-spacing', type=float, default=1., help='Minimum seconds between request starts; stricter robots rules take precedence')
    parser.add_argument('--firestore-database', default='softpower-crawl')
    args = parser.parse_args(argv)
    if min(args.cpu, args.memory_gib, args.heavy_slots, args.browser_slots, args.instances) < 1:
        parser.error('Resource and concurrency values must be positive')
    if args.browser_slots > args.heavy_slots:
        parser.error('Browser slots cannot exceed total heavy slots')
    if not 1 <= args.host_concurrency <= 16:
        parser.error('Host concurrency must be between 1 and 16')
    if not 1 <= args.archive_concurrency <= 16 or not 1 <= args.archive_slots <= 48:
        parser.error('Archive concurrency must be 1–16 and article slots 1–48')
    if not math.isfinite(args.request_spacing) or args.request_spacing < 0:
        parser.error('Request spacing must be nonnegative and finite')
    if not re.fullmatch(r'[a-z][a-z0-9-]{2,61}[a-z0-9]', args.firestore_database):
        parser.error('Use a named Firestore database ID of 4–63 lowercase letters, numbers, or hyphens')
    return args


def job_deploy_arguments(args, image):
    env = {
        'CRAWL_BUCKET': PROJECT + '-softpower-crawl',
        'RUN_ID': args.run_id,
        'CRAWL_CPU': args.cpu,
        'CRAWL_MEMORY_GIB': args.memory_gib,
        'CRAWL_HEAVY_SLOTS': args.heavy_slots,
        'CRAWL_BROWSER_SLOTS': args.browser_slots,
        'CRAWL_HOST_CONCURRENCY': args.host_concurrency,
        'CRAWL_ARCHIVE_CONCURRENCY': args.archive_concurrency,
        'CRAWL_ARCHIVE_SLOTS': args.archive_slots,
        'CRAWL_REQUEST_SPACING': args.request_spacing,
        'CRAWL_ROTATE_SECONDS': 518400,
        'CRAWL_DISTRIBUTED': int(args.instances > 1),
    }
    if args.instances > 1:
        env['CRAWL_FIRESTORE_DATABASE'] = args.firestore_database
    return [
        'run', 'jobs', 'deploy', 'softpower-crawler', '--image=' + image,
        '--region=' + REGION,
        '--service-account=softpower-crawler@' + PROJECT + '.iam.gserviceaccount.com',
        '--tasks=' + str(args.instances), '--parallelism=' + str(args.instances),
        '--max-retries=3', '--task-timeout=604800', '--cpu=' + str(args.cpu),
        '--memory=' + str(args.memory_gib) + 'Gi',
        '--set-env-vars=' + ','.join(f'{key}={value}' for key, value in env.items()), '--quiet',
    ]


def main(argv=None):
    args = arguments(argv)
    if not args.deploy and not args.execute:
        print('No changes requested. Use --deploy and/or --execute after approving cloud setup.')
        return
    credentials, _ = google.auth.default()
    credentials.refresh(Request())
    with tempfile.TemporaryDirectory(prefix='crawler-deploy-') as tmp:
        token = Path(tmp) / 'token'
        token.write_text(credentials.token)
        token.chmod(0o600)
        base = ['gcloud', '--access-token-file=' + str(token), '--project=' + PROJECT]

        def command(parts):
            subprocess.run(base + parts, check=True)

        if args.deploy:
            image = f'{REGION}-docker.pkg.dev/{PROJECT}/cloud-run-source-deploy/softpower-crawler:' + args.run_id
            build = Path(tmp) / 'build.json'
            build.write_text(json.dumps({'steps': [{'name': 'gcr.io/cloud-builders/docker', 'args': ['build', '-t', image, '.']}], 'images': [image], 'options': {'logging': 'CLOUD_LOGGING_ONLY'}}))
            command(['builds', 'submit', str(ROOT / 'crawler'), '--config=' + str(build), '--region=' + REGION, '--service-account=projects/' + PROJECT + '/serviceAccounts/softpower-audit-builder@' + PROJECT + '.iam.gserviceaccount.com', '--quiet'])
            command(job_deploy_arguments(args, image))
        if args.execute:
            if args.direct:
                command(['run', 'jobs', 'execute', 'softpower-crawler', '--region=' + REGION, '--update-env-vars=RUN_ID=' + args.run_id, '--async', '--format=json'])
            else:
                from deploy_crawl_coordinator import start
                from google.auth.transport.requests import AuthorizedSession
                start(AuthorizedSession(credentials), 'softpower-crawl-continuation', args.run_id)


if __name__ == '__main__':
    main()
