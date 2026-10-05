"""Read-only cloud validation of the two-task, 96-article distributed pilot.

Default execution performs no writes and no BigQuery query. Pass --save to keep
this verification report in the existing private crawl bucket.
"""
import argparse
import gzip
import json
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import firestore, storage

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from shared_queue import SharedQueue, article_key

PROJECT = 'citygraph'
REGION = 'us-central1'
DATABASE = 'softpower-crawl'
BUCKET = 'citygraph-softpower-crawl'
EXPECTED_ARTICLES = 96
EXPECTED_TASKS = 2


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def newest_tasks(workers, execution):
    latest = {}
    for worker in workers:
        if worker.get('execution') != execution:
            continue
        index = str(worker.get('task_index'))
        require(index in {'0', '1'}, 'Unexpected task index in verification cohort: ' + index)
        rank = (int(worker.get('task_attempt', 0)), worker.get('updated_at', 0))
        previous = latest.get(index)
        if previous is None or rank > (int(previous.get('task_attempt', 0)), previous.get('updated_at', 0)):
            latest[index] = worker
    require(set(latest) == {'0', '1'}, 'The pilot must have exactly task indexes 0 and 1')
    require(all(worker.get('state') == 'completed' for worker in latest.values()),
            'Both latest task attempts must be completed')
    return latest


def inspect_article_documents(documents, workers, run_id):
    require(len(documents) == EXPECTED_ARTICLES, f'Expected 96 article documents, found {len(documents)}')
    identities = [aid for aid, _ in documents]
    require(len(set(identities)) == EXPECTED_ARTICLES, 'Article document IDs are not unique')
    owner_to_task = {worker['owner']: str(worker['task_index']) for worker in workers}
    task_counts = Counter()
    refs = []
    for aid, article in documents:
        require(article.get('article_id') == aid, 'Article ID differs from its document ID: ' + aid)
        require(article_key(article['item']['url']) == aid, 'Article URL hash does not match: ' + aid)
        require(article.get('state') == 'done' and article.get('status') == 'saved',
                'Every pilot article must be terminal and saved: ' + aid)
        require('due_at' not in article, 'Terminal article still appears in the due queue: ' + aid)
        require(article.get('needs_export') is False, 'Article still needs BigQuery export: ' + aid)
        require(article.get('owner') in owner_to_task, 'Article owner is not in this cloud execution: ' + aid)
        task_counts[owner_to_task[article['owner']]] += 1
        token = article.get('claim_token')
        require(isinstance(token, str) and bool(token) and '/' not in token,
                'Article has no valid fenced claim token: ' + aid)
        name = f'runs/{run_id}/distributed/results/{aid}/{token}.json.gz'
        require(article.get('result_uri') == f'gs://{BUCKET}/{name}',
                'Article result URI is not its expected immutable evidence object: ' + aid)
        refs.append((aid, name))
    require(set(task_counts) == {'0', '1'} and all(count > 0 for count in task_counts.values()),
            'Both instances must have completed at least one article')
    return dict(task_counts), refs


def validate(run_id, execution=None):
    require(run_id.startswith('verify-distributed-'), 'Only an isolated verification run may be inspected')
    require('/' not in run_id, 'Run ID may not contain slashes')
    client = firestore.Client(project=PROJECT, database=DATABASE)
    bucket = storage.Client(project=PROJECT).bucket(BUCKET)
    queue = SharedQueue(client, run_id, 'verification-reader')
    progress_name = f'runs/{run_id}/progress.json'
    progress = json.loads(bucket.blob(progress_name).download_as_text(timeout=30))
    execution = execution.rsplit('/', 1)[-1] if execution else progress.get('execution')
    require(isinstance(execution, str) and bool(execution), 'No cloud execution identity was provided or published')
    require(progress.get('run_id') == run_id and progress.get('execution') == execution,
            'Final progress belongs to another run or execution')
    expected_progress = {'state': 'completed', 'total': 96, 'processed': 96,
                         'pending': 0, 'downloading': 0, 'instances': 2}
    for field, expected in expected_progress.items():
        require(progress.get(field) == expected,
                f'Final progress {field}: expected {expected!r}, got {progress.get(field)!r}')
    require(progress.get('counts') == {'saved': 96}, 'Final progress must contain exactly 96 saved articles')
    require(not progress.get('error'), 'Final progress contains an error')
    workers = queue.list_workers(execution=execution, include_expired=True)
    latest = newest_tasks(workers, execution)
    require(all(int(worker.get('task_count', 0)) == EXPECTED_TASKS for worker in latest.values()),
            'Both workers must report a two-task execution')
    documents = [(snapshot.id, snapshot.to_dict()) for snapshot in queue.articles.stream(timeout=30)]
    participation, refs = inspect_article_documents(documents, workers, run_id)
    require(queue.export_pending(limit=1) == [], 'The durable BigQuery export outbox is not empty')
    summary = queue.summary()
    for field, expected in {'total': 96, 'processed': 96, 'pending': 0, 'downloading': 0}.items():
        require(summary.get(field) == expected, f'Authoritative queue counters disagree for {field}')
    require(summary.get('counts') == {'saved': 96}, 'Queue counters do not equal 96 saved articles')

    def inspect_evidence(reference):
        aid, name = reference
        # Downloading confirms existence, gzip integrity and identity, not only a
        # stored string that happens to look like the correct GCS path.
        encoded = bucket.blob(name).download_as_bytes(timeout=30)
        result = json.loads(gzip.decompress(encoded))
        require(result.get('article_id') == aid and result.get('run_id') == run_id,
                'GCS evidence belongs to another article or run: ' + aid)
        require(result.get('status') == 'saved', 'GCS evidence has a non-saved result: ' + aid)
        return len(encoded)

    with ThreadPoolExecutor(max_workers=12) as pool:
        sizes = list(pool.map(inspect_evidence, refs))
    return {'verified': True, 'checked_at': datetime.now(timezone.utc).isoformat(),
            'run_id': run_id, 'execution': execution, 'database': DATABASE,
            'articles': len(documents), 'unique_articles': len(set(aid for aid, _ in documents)),
            'saved': summary['counts']['saved'], 'instances': len(latest),
            'completed_by_task': participation, 'pending': 0, 'downloading': 0,
            'evidence_objects_verified': len(sizes), 'evidence_compressed_bytes': sum(sizes),
            'outbox_empty': True, 'progress_state': progress['state'],
            'progress_uri': f'gs://{BUCKET}/{progress_name}',
            'worker_owners': [latest[index]['owner'] for index in sorted(latest)],
            'bigquery_query_executed': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', default='verify-distributed-20261005-r1')
    parser.add_argument('--execution', help='Optional Cloud Run execution ID or full resource name')
    parser.add_argument('--save', action='store_true', help='Save proof to the existing private crawl bucket')
    args = parser.parse_args()
    try:
        proof = validate(args.run_id, args.execution)
        if args.save:
            name = 'verification/' + args.run_id.removeprefix('verify-') + '/result.json'
            proof['verification_uri'] = f'gs://{BUCKET}/{name}'
            storage.Client(project=PROJECT).bucket(BUCKET).blob(name).upload_from_string(
                json.dumps(proof, indent=2), content_type='application/json', timeout=30)
        print(json.dumps(proof, indent=2))
    except Exception as exc:
        print(json.dumps({'verified': False, 'run_id': args.run_id,
                          'error': type(exc).__name__ + ': ' + str(exc)}, indent=2))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
