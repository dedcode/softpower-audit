"""Opt-in local HTTP benchmark for distributed host admission.

Run with the crawler's Python environment::

    python tests/verify_host_scaling.py --output /tmp/host-scaling.json

Every simulated instance owns 48 article threads, as production does. The
host-concentrated case should plateau even with more instances; the independent
host case exposes unused per-instance capacity. All HTTP goes to one local
fixture. Logical host names in URL paths let it emulate independent publishers
without DNS changes or any requests to external sites. This does not measure
Firestore RPC latency, extraction, storage, browser work, or production speed.
"""
import argparse
import asyncio
import copy
import json
import statistics
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from crawl import Fetcher
from shared_hosts import FirestoreHostStore, SharedHostCoordinator


class AtomicHostStore(FirestoreHostStore):
    """Use production transaction operations with an atomic in-memory adapter."""
    def __init__(self, clock=time.monotonic):
        self.clock = clock
        self.documents = {}
        self.lock = threading.Lock()
        self.starts = defaultdict(list)

    def _transaction(self, host, operation):
        with self.lock:
            now = self.clock()
            result, value = operation(copy.deepcopy(self.documents.get(host, {})), now)
            if value is not None:
                self.documents[host] = copy.deepcopy(value)
            if getattr(result, 'acquired', False):
                self.starts[host].append(now)
            return result


class LocalArticles:
    """One async server can emulate hundreds of slow publishers without threads."""
    def __init__(self, latency):
        self.latency = latency
        self.events = []
        self.active = defaultdict(int)
        self.maximum = defaultdict(int)
        self.total_active = 0
        self.maximum_total = 0
        self.ready = threading.Event()

    async def handle(self, reader, writer):
        counted = False
        try:
            request = await reader.readuntil(b'\r\n\r\n')
            path = request.split(b' ', 2)[1].decode()
            host, kind, identifier = path.strip('/').split('/')
            self.active[host] += 1
            self.total_active += 1
            counted = True
            self.maximum[host] = max(self.maximum[host], self.active[host])
            self.maximum_total = max(self.maximum_total, self.total_active)
            self.events.append({'host': host, 'kind': kind, 'id': identifier,
                                'started': time.monotonic()})
            status = '429 Too Many Requests' if kind == 'limited' else '200 OK'
            extra = 'Retry-After: 5\r\n' if kind == 'limited' else ''
            body = b'<html><article>Local benchmark article.</article></html>'
            writer.write((f'HTTP/1.1 {status}\r\nContent-Type: text/html\r\n'
                          f'Content-Length: {len(body)}\r\nConnection: close\r\n'
                          f'{extra}\r\n').encode())
            await writer.drain()
            await asyncio.sleep(self.latency)
            # Once the full body is readable, a caller may release its permit.
            self.active[host] -= 1
            self.total_active -= 1
            counted = False
            writer.write(body)
            await writer.drain()
        finally:
            if counted:
                self.active[host] -= 1
                self.total_active -= 1
            writer.close()
            await writer.wait_closed()

    def __enter__(self):
        def run():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.server = self.loop.run_until_complete(asyncio.start_server(
                self.handle, '127.0.0.1', 0, backlog=1024))
            self.port = self.server.sockets[0].getsockname()[1]
            self.ready.set()
            self.loop.run_forever()
            self.server.close()
            self.loop.run_until_complete(self.server.wait_closed())
            self.loop.close()

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()
        if not self.ready.wait(5):
            raise RuntimeError('Local HTTP fixture did not start')
        return self

    def __exit__(self, *_):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)
        if self.thread.is_alive():
            raise RuntimeError('Local HTTP fixture did not stop')

    def url(self, host, identifier, kind='article'):
        return f'http://127.0.0.1:{self.port}/{host}/{kind}/{identifier}'

    def fixture_url(self, value):
        parsed = urlsplit(value)
        if (parsed.scheme != 'http' or parsed.hostname != '127.0.0.1'
                or parsed.port != self.port):
            raise AssertionError('Benchmark attempted a non-fixture request')
        return SimpleNamespace(hostname=parsed.path.strip('/').split('/')[0])


@contextmanager
def fixture_fetchers(fixture, instances, cap, spacing, store):
    with patch('crawl.public_url', side_effect=fixture.fixture_url):
        coordinators = [SharedHostCoordinator(store, max_concurrency=cap,
                                              poll_seconds=.01)
                        for _ in range(instances)]
        fetchers = [Fetcher(None, 'local-scaling-check', delay=spacing,
                            host_coordinator=coordinator)
                    for coordinator in coordinators]
        yield fetchers, coordinators


def run_case(instances, hosts, count, cap, spacing, latency, label):
    store = AtomicHostStore()
    with LocalArticles(latency) as fixture, fixture_fetchers(
            fixture, instances, cap, spacing, store) as (fetchers, _):
        pools = [ThreadPoolExecutor(max_workers=48) for _ in range(instances)]
        gate = threading.Event()
        def retrieve(index):
            fetcher = fetchers[index % instances]
            gate.wait()
            before = fetcher.host_queue_wait_seconds()
            status, _, body, large = fetcher.one(fixture.url(f'h{index % hosts}', index))
            assert status == 200 and body and not large
            return fetcher.host_queue_wait_seconds() - before

        try:
            futures = [pools[index % instances].submit(retrieve, index) for index in range(count)]
            started = time.monotonic()
            gate.set()
            queue_wait = [future.result(timeout=180) for future in futures]
            elapsed = time.monotonic() - started
        finally:
            gate.set()
            for pool in pools:
                pool.shutdown(wait=True)

        assert len(fixture.events) == count, (len(fixture.events), count)
        assert max(fixture.maximum.values()) <= cap, dict(fixture.maximum)
        gaps = [second - first for values in store.starts.values()
                for first, second in zip(values, values[1:])]
        minimum_gap = min(gaps) if gaps else None
        assert minimum_gap is None or minimum_gap + 1e-6 >= spacing, minimum_gap
        return {
            'workload': label, 'instances': instances, 'article_slots_per_instance': 48,
            'total_article_slots': instances * 48, 'logical_hosts': hosts,
            'global_http_limit_per_host': cap, 'request_spacing_seconds': spacing,
            'response_body_latency_seconds': latency, 'completed_http_requests': count,
            'elapsed_seconds': round(elapsed, 3),
            'completed_http_per_second': round(count / elapsed, 2),
            'maximum_actual_http_inflight': fixture.maximum_total,
            'maximum_actual_http_inflight_per_host': max(fixture.maximum.values()),
            'minimum_admission_start_gap_seconds': round(minimum_gap, 6) if gaps else None,
            'mean_host_wait_seconds': round(statistics.mean(queue_wait), 3),
        }


def verify_fencing_and_races():
    now = [100.]
    store = AtomicHostStore(clock=lambda: now[0])
    barrier = threading.Barrier(32)
    def race(index):
        barrier.wait()
        return index, store.acquire('same-host', str(index), 0, 10, max_concurrency=4)
    with ThreadPoolExecutor(max_workers=32) as pool:
        replies = list(pool.map(race, range(32)))
    owners = [str(index) for index, result in replies if result.acquired]
    assert len(owners) == 4, owners
    assert store.release('same-host', owners[0])
    assert store.acquire('same-host', 'replacement', 0, 10, max_concurrency=4).acquired
    assert not store.release('same-host', owners[0]), 'stale release unexpectedly succeeded'
    assert not store.acquire('same-host', 'fifth', 0, 10, max_concurrency=4).acquired
    now[0] += 11
    assert not store.renew('same-host', 'replacement', 10), 'expired owner renewed'
    assert store.acquire('same-host', 'fresh', 0, 10, max_concurrency=4).acquired
    assert not store.defer('same-host', 'replacement', 600), 'stale owner changed cooldown'
    assert store.renew('same-host', 'fresh', 10), 'stale operation displaced fresh owner'
    return {'simultaneous_contenders': 32, 'admitted': len(owners),
            'release_fencing': 'passed', 'expiry_fencing': 'passed'}


def verify_http_cooldown(latency):
    store = AtomicHostStore()
    with LocalArticles(latency) as fixture, fixture_fetchers(
            fixture, 2, 4, 0., store) as (fetchers, coordinators):
        status, _, _, _ = fetchers[0].one(fixture.url('limited-host', 1, 'limited'))
        assert status == 429
        lease, admission = coordinators[1].try_acquire('limited-host', delay=0.)
        assert lease is None and not admission.acquired and admission.cooldown_seconds > 4
        status, _, _, _ = fetchers[1].one(fixture.url('other-host', 2))
        assert status == 200
        assert len([event for event in fixture.events if event['host'] == 'limited-host']) == 1
        return {'http_429_retry_after_seconds': 5,
                'other_instance_blocked_for_seconds': round(admission.cooldown_seconds, 3),
                'unrelated_host_completed': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--latency', type=float, default=.2)
    parser.add_argument('--spacing', type=float, default=.02)
    parser.add_argument('--instances', default='1,2,5,10')
    parser.add_argument('--concentrated-requests', type=int, default=96)
    parser.add_argument('--independent-requests', type=int, default=480)
    arguments = parser.parse_args()
    instances = [int(value) for value in arguments.instances.split(',')]
    assert all(value > 0 for value in instances)
    report = {
        'scope': 'Local HTTP fixture; real Fetcher and host policy; in-memory atomic store.',
        'not_measured': ['Firestore RPC contention', 'extraction and storage',
                         'browser requests', 'production throughput'],
        'fencing': verify_fencing_and_races(),
        'retry_after': verify_http_cooldown(arguments.latency),
        'measurements': [],
    }
    for label, hosts, count in [('concentrated', 3, arguments.concentrated_requests),
                                ('independent', 120, arguments.independent_requests)]:
        for cap in (1, 4):
            for workers in instances:
                row = run_case(workers, hosts, count, cap, arguments.spacing,
                               arguments.latency, label)
                report['measurements'].append(row)
                print(json.dumps(row), flush=True)
    if arguments.output:
        arguments.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'fencing': report['fencing'], 'retry_after': report['retry_after']}), flush=True)


if __name__ == '__main__':
    main()
