import gzip
import io
import json
import sys
import time
import threading
import tracemalloc
import unittest
from collections import defaultdict, deque
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from result_store import JsonlBatch, ResultIndex, encode_jsonl, iter_input_rows
from worker import Run
from crawl import key


def result(article_id='a', updated_at='2026-10-04T01:00:00+00:00', status='saved', outlet='news.ke'):
    return {'article_id': article_id, 'updated_at': updated_at, 'status': status,
            'outlet': outlet, 'attempts': [{'stage': 'http', 'http_attempts': [{}, {}, {}]}],
            'response_bytes': 200, 'stored_bytes': 80}


class Bucket:
    def __init__(self, data=None):
        self.data = data or {}

    def blob(self, name):
        data = self.data

        class Blob:
            def download_as_text(self, **kwargs):
                from google.api_core.exceptions import NotFound
                if name not in data:raise NotFound(name)
                return data[name]

            def open(self, mode, **kwargs):
                return io.BytesIO(data[name])

            def exists(self):
                return name in data

            def upload_from_string(self, value, **kwargs):
                data[name] = value
        return Blob()

    def list_blobs(self, prefix):
        return [self.blob(name) for name in list(self.data) if name.startswith(prefix)]


class ResultStoreTests(unittest.TestCase):
    def test_latest_checkpoint_wins_without_counting_pending_as_done(self):
        index = ResultIndex()
        index.record(result())
        index.record(result(updated_at='2026-10-04T03:00:00+00:00', status='deferred'))
        index.record(result(updated_at='2026-10-04T02:00:00+00:00', status='failed'))
        self.assertNotIn('a', index)
        self.assertEqual(len(index), 0)
        self.assertEqual(index.snapshot()['counts'], {})
        index.record(result(updated_at='2026-10-04T04:00:00+00:00', status='partial', outlet='other.ke'))
        index.record(result(updated_at='2026-10-04T04:00:00+00:00', status='partial', outlet='other.ke'))
        self.assertIn('a', index)
        self.assertEqual(len(index), 1)
        totals = index.snapshot()
        self.assertEqual(totals['counts'], {'partial': 1})
        self.assertEqual(totals['domains']['other.ke'], {'partial': 1})
        self.assertEqual((totals['attempts'], totals['retries'], totals['response_bytes']), (3, 2, 200))

    def test_large_histories_are_not_retained(self):
        # Full histories would retain >80 MB; the index must remain compact.
        tracemalloc.start()
        try:
            index = ResultIndex()
            for number in range(20000):
                row = result(str(number))
                row['attempts'][0]['diagnostic'] = str(number) + ('X' * 4096)
                index.record(row)
            current, peak = tracemalloc.get_traced_memory()
            self.assertEqual(len(index), 20000)
            self.assertLess(current, 10 * 1024 * 1024)
            self.assertLess(peak, 12 * 1024 * 1024)
        finally:
            tracemalloc.stop()

    def test_batch_bounds_bytes_and_count_without_truncating_large_record(self):
        batch = JsonlBatch(2, 20)
        batch.append(b'12345\n')
        self.assertFalse(batch.fits(b'X' * 16))
        batch.append(b'67890\n')
        self.assertTrue(batch.full)
        with self.assertRaises(ValueError):
            batch.append(b'1\n')
        batch.clear()
        large = b'Z' * 25
        batch.append(large)
        self.assertTrue(batch.full)
        self.assertEqual(batch.data(), large)
        self.assertFalse(batch.fits(b'1\n'))

    def test_manifest_is_decoded_across_small_chunks(self):
        rows = [{'url': 'https://news.ke/a?text=中囯'}, {'nested': ['a', {'b': '}]['}], 'empty': {}}]
        for chunk_size in (1, 7, 1024):
            self.assertEqual(list(iter_input_rows(io.StringIO(json.dumps(rows)), chunk_size)), rows)
        self.assertEqual(list(iter_input_rows(io.StringIO(' [ ] '), 1)), [])
        for invalid in ('[{"a": 1},]', '[{"a": 1}', '[{"a": 1}]x', '[1]', '{}', '[{}{}]'):
            with self.assertRaises(ValueError):
                list(iter_input_rows(io.StringIO(invalid), 1))

    def new_run(self, data):
        run = Run.__new__(Run)
        run.bucket = Bucket(data)
        run.prefix = 'runs/test/'
        run.done = ResultIndex()
        run.checkpoint = JsonlBatch(2, 1500)
        run.to_bq = JsonlBatch(3, 1500)
        run.config = {'dataset': 'project.dataset'}
        run.last_checkpoint = run.last_bq = time.monotonic()
        run.queues = defaultdict(deque)
        run.inflight_items = {}
        run.lease_update = Mock()
        run.bq = Mock()
        run.loads = []
        def load(stream, table, **kwargs):
            run.loads.append(stream.read())
            return Mock()
        run.bq.load_table_from_file.side_effect = load
        return run

    def test_restore_replays_in_bounded_batches_with_full_evidence(self):
        rows = [result(str(i)) for i in range(11)]
        rows.extend([result('a'), result('a', updated_at='2026-10-04T03:00:00+00:00', status='deferred'),
                     result('a', updated_at='2026-10-04T02:00:00+00:00', status='failed')])
        checkpoint = gzip.compress(b''.join(encode_jsonl(row) for row in rows))
        run = self.new_run({'runs/test/checkpoints/one.jsonl.gz': checkpoint})
        run.restore()
        self.assertEqual(len(run.done), 11)
        self.assertNotIn('a', run.done)
        self.assertEqual(len(run.to_bq), 0)
        self.assertGreater(len(run.loads), 1)
        self.assertTrue(all(len(batch) <= 1500 for batch in run.loads))
        reloaded = [json.loads(line) for batch in run.loads for line in batch.splitlines()]
        self.assertEqual(len(reloaded), len(rows))
        for original, stored in zip(rows, reloaded):
            self.assertEqual(json.loads(stored.pop('attempts_json')), original['attempts'])
            self.assertEqual(stored, {k: v for k, v in original.items() if k != 'attempts'})

    def test_failed_bq_load_keeps_checkpoint_and_batch_for_retry(self):
        run = self.new_run({})
        row = result()
        run.result(row)
        run.bq.load_table_from_file.side_effect = RuntimeError('temporary load error')
        with self.assertRaises(RuntimeError):
            run.flush(final=True)
        self.assertEqual(len(run.to_bq), 1)
        persisted = [json.loads(line) for name, data in run.bucket.data.items()
                     if '/checkpoints/' in name for line in gzip.decompress(data).splitlines()]
        self.assertEqual(persisted, [row])

    def test_memory_pressure_pauses_without_marking_pending_urls_done(self):
        rows = [{'url': 'https://news.ke/a', 'outlet': 'news.ke'}]
        run = self.new_run({'runs/test/inputs.json.gz': gzip.compress(json.dumps(rows).encode())})
        run.recovery = Mock()
        run.recovery.reserve_restart.return_value = True
        run.country = 'KE'
        run.run_id = 'test'
        run.started = time.monotonic()
        run.active = {}
        run.paused = {}
        run.fetcher = Mock()
        run.execution='test-execution'
        run.lease = Mock()
        run.config.update(workers=1, max_runtime_seconds=1000,
                          max_response_bytes=100000, max_total_attempts=10000)
        run.progress = Mock(return_value={'response_bytes': 0, 'attempts': 0})
        with patch('worker.reclaim_memory'), patch('worker.memory_usage', return_value={'current_bytes': 90, 'limit_bytes': 100}), patch('builtins.print'):
            run.run()
        run.fetcher.fetch.assert_not_called()
        self.assertEqual(run.progress.call_args.args[0], 'recovering_memory')
        self.assertEqual(len(run.done), 0)
        self.assertEqual(list(run.queues['news.ke']), rows)

    def test_restore_failure_keeps_real_total_not_false_completion(self):
        run = self.new_run({})
        run.recovery = Mock()
        run.country = 'KE';run.run_id = 'test';run.execution = 'test-execution'
        run.started = time.monotonic();run.active = {};run.paused = {}
        run.fetcher = Mock();run.lease = Mock()
        run.config.update(input_count=85993, max_runtime_seconds=1000)
        run.restore = Mock(side_effect=RuntimeError('storage temporarily unavailable'))
        run.progress = Mock()
        with self.assertRaises(RuntimeError):run.run()
        self.assertEqual(run.progress.call_args.args[:2], ('failed', 85993))

    def assert_failed_worker_drains(self, fail_after_recording=False):
        rows = [{'url': 'https://news.ke/' + str(i), 'outlet': 'news.ke'} for i in range(3)]
        run = self.new_run({'runs/test/inputs.json.gz': gzip.compress(json.dumps(rows).encode())})
        run.recovery = Mock()
        run.country = 'KE';run.run_id = 'test';run.execution = 'test-execution'
        run.started = time.monotonic();run.active = {};run.paused = {}
        run.fetcher = Mock();run.lease = Mock()
        run.config.update(workers=2, per_outlet_workers=2, max_runtime_seconds=1000,
                          max_response_bytes=100000, max_total_attempts=10000)
        failure = RuntimeError('first failure')
        release_success = threading.Event()
        reports = []
        fetched = []
        def report(state, total, error=None):
            reports.append((state, len(run.done), len(run.active), sum(map(len, run.queues.values())), error))
            if state == 'failed':release_success.set()
            return {'response_bytes': 0, 'attempts': 0}
        run.progress = Mock(side_effect=report)
        def fetch(item, *_):
            fetched.append(item['url'])
            if item == rows[0] and not fail_after_recording:raise failure
            if item == rows[1]:
                if not release_success.wait(2):raise AssertionError('Worker did not report failure while draining')
            return result(key(item['url']))
        run.fetcher.fetch.side_effect = fetch
        if fail_after_recording:
            record = run.result
            def fail_once(row):
                record(row)
                if row['article_id'] == key(rows[0]['url']):raise failure
            run.result = fail_once
        with patch('worker.memory_usage', return_value={}):
            with self.assertRaises(RuntimeError) as caught:run.run()
        self.assertIs(caught.exception, failure)
        self.assertEqual(set(fetched), {rows[0]['url'], rows[1]['url']})
        self.assertFalse(run.active);self.assertFalse(run.inflight_items)
        expected = [rows[2]] if fail_after_recording else [rows[0], rows[2]]
        self.assertEqual(list(run.queues['news.ke']), expected)
        self.assertIn(key(rows[1]['url']), run.done)
        persisted = [json.loads(line) for name, data in run.bucket.data.items()
                     if '/checkpoints/' in name for line in gzip.decompress(data).splitlines()]
        self.assertIn(key(rows[1]['url']), {row['article_id'] for row in persisted})
        if fail_after_recording:self.assertIn(key(rows[0]['url']), run.done)
        else:self.assertNotIn(key(rows[0]['url']), run.done)
        failed_reports = [r for r in reports if r[0] == 'failed']
        self.assertTrue(any(r[2] == 1 for r in failed_reports))
        self.assertTrue(all(r[1] + r[2] + r[3] == 3 for r in reports))
        self.assertEqual(reports[-1][0], 'failed')
        self.assertIn('first failure', reports[-1][4])

    def test_failing_future_preserves_later_success_before_reraising(self):
        self.assert_failed_worker_drains()

    def test_result_exception_does_not_requeue_already_recorded_article(self):
        self.assert_failed_worker_drains(fail_after_recording=True)

    def test_input_manifest_only_queues_unfinished_urls(self):
        rows = [{'url': 'https://news.ke/' + str(i), 'outlet': 'news.ke'} for i in range(5)]
        run = self.new_run({'runs/test/inputs.json.gz': gzip.compress(json.dumps(rows).encode())})
        run.done.record(result(key(rows[1]['url'])))
        run.done.record(result(key(rows[3]['url']), status='deferred'))
        self.assertEqual(run.load_inputs(), 5)
        self.assertEqual(list(run.queues['news.ke']), [rows[i] for i in (0, 2, 3, 4)])


if __name__ == '__main__':
    unittest.main()
