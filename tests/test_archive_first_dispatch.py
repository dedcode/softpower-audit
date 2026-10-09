"""Real worker/pipeline/queue handoffs, with in-memory storage and no network."""
import time
import unittest
from unittest.mock import Mock, patch

from test_distributed_worker import bare_run
from test_shared_queue import Database, transactional, article
from shared_queue import SharedQueue
from shared_hosts import Admission
from result_store import ResultIndex
from retrying import RetryingPipeline
from crawl import Fetcher


class EvidenceBucket:
    name = 'test-private'

    def __init__(self):
        self.data = {}

    def blob(self, path):
        data = self.data

        class Blob:
            def exists(self, **kwargs):
                return path in data

            def download_as_text(self, **kwargs):
                value = data[path]
                return value.decode() if isinstance(value, bytes) else value

            def download_as_bytes(self, **kwargs):
                value = data[path]
                return value if isinstance(value, bytes) else value.encode()

            def upload_from_string(self, value, **kwargs):
                data[path] = value

        return Blob()


class ArchiveFirstIntegrationTests(unittest.TestCase):
    def test_cooling_unattempted_url_runs_archive_and_miss_stays_pending_until_publisher_checked(self):
        run = bare_run()
        run.verification = False
        run.country = 'KE'
        run.config['workers'] = 48
        run.bucket = EvidenceBucket()
        run.fetcher = RetryingPipeline(run.bucket, run.run_id, max_attempts=1)
        run.fetcher.dispatch_availability = Mock(side_effect=lambda hosts: {
            host: Admission(False, 300, 300) for host in hosts})
        clock_offset = [0.]
        run.queue = SharedQueue(Database(), run.run_id, 'worker-a',
                                clock=lambda: time.time() + clock_offset[0])
        item = article(1)

        def execute(claim):
            run.claims[claim.article_id] = claim
            run.update_deadlines([claim])
            result = run.fetch_claim(claim)
            run.persist_result(claim, result)
            return result

        with patch('shared_queue.firestore.transactional', transactional), \
             patch.dict('os.environ', {'CRAWL_ARCHIVE_SLOTS': '8'}), \
             patch.object(Fetcher, 'fetch') as publisher, \
             patch.object(run.fetcher, 'one', return_value=(200, {}, b'{"archived_snapshots":{}}', False)) as archive:
            run.queue.initialize([item], ResultIndex(), {**run.config, 'country': 'KE', 'input_count': 1})
            metadata = run.claim_available()
            self.assertEqual(len(metadata), 1)
            self.assertTrue(metadata[0].archive_first)
            first = execute(metadata[0])
            self.assertEqual((first['status'], first['next_phase']), ('queued', 'archive'))
            self.assertEqual(first['_checkpoint']['completed_passes'], 0)
            publisher.assert_not_called()
            archive.assert_not_called()
            summary = run.queue.summary()
            self.assertEqual((summary['processed'], summary['publisher_remaining'], summary['archive_remaining']),
                             (0, 0, 1))

            archive_claim = run.claim_available()
            self.assertEqual(len(archive_claim), 1)
            self.assertEqual(archive_claim[0].phase, 'archive')
            miss = execute(archive_claim[0])
            self.assertEqual((miss['status'], miss['next_phase']), ('queued', 'publisher'))
            self.assertEqual(miss['_checkpoint']['completed_passes'], 0)
            self.assertTrue(miss['_checkpoint']['archive_first_done'])
            publisher.assert_not_called()
            self.assertEqual(archive.call_count, 2)
            summary = run.queue.summary()
            self.assertEqual((summary['processed'], summary['publisher_remaining'], summary['archive_remaining']),
                             (0, 1, 0))

            # A subsequent worker/refill may inspect the durable marker, but
            # must never perform the archive lookup twice during this pause.
            for claim in run.claim_available():
                execute(claim)
            self.assertEqual(archive.call_count, 2)
            publisher.assert_not_called()
            self.assertEqual(run.queue.summary()['processed'], 0)

            clock_offset[0] += 301
            run.fetcher.dispatch_availability.side_effect = lambda hosts: {
                host: Admission(True) for host in hosts}
            publisher.return_value = {'status': 'unavailable', 'attempts': [{'http_status': 404}],
                                      'http_status': 404, 'raw_uri': None,
                                      'response_bytes': 10, 'stored_bytes': 4}
            publisher_claim = run.claim_available()
            self.assertEqual(len(publisher_claim), 1)
            self.assertFalse(getattr(publisher_claim[0], 'archive_first', False))
            final = execute(publisher_claim[0])
            self.assertEqual(final['status'], 'exhausted')
            publisher.assert_called_once()
            self.assertEqual(archive.call_count, 2)
            self.assertEqual(sum(event['stage'] == 'archive_lookup' for event in final['attempts']), 2)
            self.assertEqual(sum(event['stage'] == 'http' for event in final['attempts']), 1)
            summary = run.queue.summary()
            self.assertEqual((summary['processed'], summary['publisher_remaining'], summary['archive_remaining']),
                             (1, 0, 0))
            self.assertEqual(len(run.queue.export_pending()), 1)


if __name__ == '__main__':
    unittest.main()
