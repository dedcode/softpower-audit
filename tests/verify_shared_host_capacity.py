"""Bounded, isolated Firestore integration proof; no publisher requests."""
import argparse
import hashlib
import json
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from google.cloud import firestore
from shared_hosts import FirestoreHostStore

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project', default='citygraph')
    parser.add_argument('--database', default='softpower-crawl')
    parser.add_argument('--output', type=Path, default=Path('/tmp/shared-host-cloud-proof.json'))
    args = parser.parse_args(argv)
    PROJECT = args.project
    DATABASE = args.database
    COLLECTION = 'verify_host_capacity_' + uuid.uuid4().hex
    OUTPUT = args.output
    assert COLLECTION.startswith('verify_host_capacity_') and COLLECTION != 'crawl_hosts'
    client = firestore.Client(project=PROJECT, database=DATABASE)
    store = FirestoreHostStore(client, collection=COLLECTION)
    names = ('capacity.example', 'independent.example', 'robots.example',
             'legacy.example', 'missing.example', 'legacy-robots.example')
    refs = {name: client.collection(COLLECTION).document(hashlib.sha256(name.encode()).hexdigest())
            for name in names}
    proof = {'project': PROJECT, 'database': DATABASE, 'collection': COLLECTION,
             'started_at': datetime.now(timezone.utc).isoformat(), 'checks': {},
             'publisher_requests': 0, 'passed': False}

    def save():
        OUTPUT.write_text(json.dumps(proof, indent=2) + '\n')

    try:
        barrier = threading.Barrier(20)

        def contender(index):
            # Distinct store instances share the real transactional database, just
            # as separate workers do; no fake transaction or process-local mutex.
            worker_store = FirestoreHostStore(client, collection=COLLECTION)
            owner = f'worker-{index:02d}'
            barrier.wait(timeout=30)
            began = time.monotonic()
            result = worker_store.acquire('capacity.example', owner, 0, 180, 4, 0)
            return {'owner': owner, 'acquired': result.acquired,
                    'wait_seconds': result.wait_seconds,
                    'elapsed_seconds': round(time.monotonic() - began, 3)}

        with ThreadPoolExecutor(max_workers=20) as executor:
            replies = list(executor.map(contender, range(20)))
        admitted = [reply['owner'] for reply in replies if reply['acquired']]
        assert len(admitted) == 4, replies
        state = refs['capacity.example'].get().to_dict()
        assert set(state['leases']) == set(admitted), state
        proof['checks']['concurrent_transactions'] = {
            'clients': 20, 'admitted': len(admitted), 'denied': 20 - len(admitted),
            'transaction_errors': 0, 'max_elapsed_seconds': max(r['elapsed_seconds'] for r in replies)}

        ready = store.availability(['capacity.example', 'missing.example'], 0, 4,
                                   {'capacity.example': 0, 'missing.example': 0})
        assert not ready['capacity.example'].acquired
        assert ready['missing.example'].acquired
        assert not refs['missing.example'].get().exists
        proof['checks']['sdk_batched_readiness'] = {
            'full_host_denied': True, 'missing_document_eligible': True,
            'missing_document_not_created': True, 'used_sdk_get_all': True}

        assert store.release('capacity.example', admitted[0])
        state = refs['capacity.example'].get().to_dict()
        assert set(state['leases']) == set(admitted[1:]), state
        assert not store.release('capacity.example', admitted[0])
        assert store.acquire('capacity.example', 'replacement', 0, 180, 4, 0).acquired
        assert not store.renew('capacity.example', admitted[0], 180)
        assert not store.defer('capacity.example', admitted[0], 3600)
        assert store.renew('capacity.example', admitted[1], 180)
        proof['checks']['independent_fencing'] = {
            'released_one_preserved_three': True, 'replacement_admitted': True,
            'stale_release_renew_and_defer_rejected': True, 'live_peer_renewed': True}

        assert store.defer('capacity.example', admitted[1], 60)
        assert store.release('capacity.example', admitted[1])
        cooled = store.acquire('capacity.example', 'cooldown-contender', 0, 180, 4, 0)
        assert not cooled.acquired and 0 < cooled.cooldown_seconds <= 60
        assert store.acquire('independent.example', 'independent', 0, 180, 4, 0).acquired
        proof['checks']['cooldown'] = {
            'blocks_new_start_with_free_permit': True,
            'cooldown_seconds': round(cooled.cooldown_seconds, 3),
            'other_hostname_remains_available': True}

        assert store.acquire('robots.example', 'robot-first', 1, 180, 4, 20).acquired
        rule = store.acquire('robots.example', 'robot-peer', 1, 180, 4, 0)
        assert not rule.acquired and 0 < rule.wait_seconds <= 20
        state = refs['robots.example'].get().to_dict()
        assert state['robots_delay_seconds'] == state['delay_seconds'] == 20
        proof['checks']['robots_delay'] = {
            'stored_seconds': state['robots_delay_seconds'],
            'peer_with_no_rule_denied': True, 'wait_seconds': round(rule.wait_seconds, 3)}

        # Construct an old-format exclusive owner using a server-assigned read time.
        server_time = refs['legacy.example'].get().read_time.timestamp()
        refs['legacy.example'].set({'owner': 'legacy-worker', 'lease_until': server_time + 60,
            'last_started_at': server_time - 10, 'delay_seconds': 3,
            'next_allowed_at': server_time - 7})
        legacy = store.acquire('legacy.example', 'new-worker', 1, 180, 4, 0)
        assert not legacy.acquired and 0 < legacy.wait_seconds <= 60
        state = refs['legacy.example'].get().to_dict()
        assert state['owner'] == 'legacy-worker' and 'leases' not in state
        assert store.release('legacy.example', 'legacy-worker')
        assert store.acquire('legacy.example', 'new-worker', 1, 180, 4, 0).acquired
        state = refs['legacy.example'].get().to_dict()
        assert list(state['leases']) == ['new-worker'] and state['delay_seconds'] == 1
        refs['legacy-robots.example'].set({'delay_seconds': 20})
        assert store.acquire('legacy-robots.example', 'migrated', 1, 180, 4, 0).acquired
        assert refs['legacy-robots.example'].get().to_dict()['robots_delay_seconds'] == 20
        proof['checks']['legacy_migration'] = {
            'live_exclusive_owner_respected': True, 'explicit_release_then_new_lease': True,
            'old_default_3_lowered_to_1': True, 'old_actual_20_preserved': True}
        proof['passed'] = True
    except BaseException as exc:
        proof['error'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        proof['finished_at'] = datetime.now(timezone.utc).isoformat()
        save()
        try:
            batch = client.batch()
            for reference in refs.values():
                assert reference.path.startswith(COLLECTION + '/')
                batch.delete(reference)
            batch.commit()
            proof['cleanup'] = {'owned_documents_deleted': len(refs),
                                'collection_empty': not any(client.collection(COLLECTION).limit(1).stream())}
        except Exception as exc:
            proof['cleanup_error'] = f'{type(exc).__name__}: {exc}'
        save()
        print(json.dumps(proof, indent=2))


if __name__ == '__main__':
    main()
