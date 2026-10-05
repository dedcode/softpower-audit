import gzip
import json
import time
import unittest
from test_distributed_worker import bare_run, claim


class DistributedPhasesTests(unittest.TestCase):
    def prepare(self, phase='publisher'):
        run = bare_run()
        run.verification = False
        item = claim()
        item.phase = phase
        item.checkpoint_uri = None
        run.claims[item.article_id] = item
        run.deadlines[item.article_id] = time.monotonic() + 500
        return run, item

    def queued(self):
        return {'status': 'queued', 'next_phase': 'archive', 'retry_at': 0,
                '_checkpoint': {'best': {'text': 'preserved partial article'}},
                'attempts': [], 'response_bytes': 123, 'stored_bytes': 45}

    def test_handoff_preserves_checkpoint_before_releasing_slot_without_completing(self):
        run, item = self.prepare()
        result = self.queued()
        order = []
        run.bucket.blob.return_value.upload_from_string.side_effect = lambda *a, **k: order.append('preserved')
        run.queue.handoff.side_effect = lambda *a, **k: order.append('handoff') or True
        run.persist_result(item, result)
        self.assertEqual(order, ['preserved', 'handoff'])
        payload = run.bucket.blob.return_value.upload_from_string.call_args.args[0]
        envelope = json.loads(gzip.decompress(payload))
        self.assertEqual(envelope['checkpoint'], result['_checkpoint'])
        self.assertEqual(envelope['phase'], 'archive')
        self.assertEqual(envelope['article_id'], item.article_id)
        self.assertNotIn(item.article_id, run.claims)
        run.queue.complete.assert_not_called()
        run.bq.load_table_from_file.assert_not_called()

    def test_failed_checkpoint_write_keeps_owned_work_unfinished(self):
        run, item = self.prepare()
        run.bucket.blob.return_value.upload_from_string.side_effect = RuntimeError('GCS unavailable')
        with self.assertRaisesRegex(RuntimeError, 'GCS unavailable'):
            run.persist_result(item, self.queued())
        run.queue.handoff.assert_not_called()
        run.queue.complete.assert_not_called()
        self.assertIn(item.article_id, run.claims)

    def test_stale_handoff_aborts_without_forgetting_or_completing(self):
        run, item = self.prepare()
        run.queue.handoff.return_value = False
        with self.assertRaisesRegex(RuntimeError, 'handoff rejected'):
            run.persist_result(item, self.queued())
        self.assertTrue(run.fetcher.abort_event.is_set())
        self.assertIn(item.article_id, run.claims)
        run.queue.complete.assert_not_called()

    def test_archive_resumes_private_checkpoint_under_new_claim_scope(self):
        run, item = self.prepare('archive')
        item.checkpoint_uri = 'gs://private-test/' + run.prefix + 'distributed/checkpoints/' + item.article_id + '/old-token.json.gz'
        checkpoint = {'prior': 'publisher evidence'}
        run.bucket.blob.return_value.download_as_bytes.return_value = gzip.compress(json.dumps({
            'article_id': item.article_id, 'run_id': run.run_id,
            'phase': 'archive', 'checkpoint': checkpoint}).encode())
        run.fetcher.fetch_phase.return_value = {'status': 'saved'}
        self.assertEqual(run.fetch_claim(item)['status'], 'saved')
        run.fetcher.fetch_phase.assert_called_once_with(item.item, run.run_id, run.country,
                                                        phase='archive', checkpoint=checkpoint)
        self.assertIsNone(run.fetcher.request_context.output_scope)

    def test_foreign_checkpoint_is_rejected(self):
        run, item = self.prepare('archive')
        item.checkpoint_uri = 'gs://another-bucket/secret'
        with self.assertRaisesRegex(RuntimeError, 'Unexpected phase checkpoint'):
            run.fetch_claim(item)
        run.fetcher.fetch_phase.assert_not_called()

    def test_checkpoint_identity_must_match_queue(self):
        run, item = self.prepare('archive')
        item.checkpoint_uri = 'gs://private-test/' + run.prefix + 'distributed/checkpoints/' + item.article_id + '/old-token.json.gz'
        run.bucket.blob.return_value.download_as_bytes.return_value = gzip.compress(json.dumps({
            'article_id': 'different', 'run_id': run.run_id, 'phase': 'archive', 'checkpoint': {}}).encode())
        with self.assertRaisesRegex(RuntimeError, 'does not match'):
            run.fetch_claim(item)
        run.fetcher.fetch_phase.assert_not_called()

    def test_recovery_backlog_cannot_consume_publisher_capacity(self):
        run, item = self.prepare('archive')
        run.config['workers'] = 48
        other = claim()
        other.article_id = 'second'
        other.phase = 'archive'
        run.claims[other.article_id] = other
        for index in range(2):
            browser = claim()
            browser.article_id = 'browser-' + str(index)
            browser.phase = 'browser'
            run.claims[browser.article_id] = browser
        run.queue.claim.return_value = []
        run.claim_available()
        run.queue.claim.assert_called_once()
        args = run.queue.claim.call_args.kwargs
        self.assertEqual(args['phase'], 'publisher')
        self.assertEqual(args['limit'], 44)
        self.assertEqual(dict(args['inflight']), {})

    def test_idle_worker_reserves_small_recovery_pools_within_48(self):
        run = bare_run()
        run.config['workers'] = 48
        run.queue.claim.return_value = []
        run.claim_available()
        calls = [call.kwargs for call in run.queue.claim.call_args_list]
        self.assertEqual([(c['phase'], c['limit']) for c in calls], [('publisher', 44), ('browser', 2), ('archive', 2)])

    def test_memory_reduced_worker_never_exceeds_single_slot(self):
        run = bare_run()
        item = claim()
        run.queue.claim.side_effect = [[item]]
        self.assertEqual(run.claim_available(), [item])
        run.queue.claim.assert_called_once()
        run.queue.claim.reset_mock(side_effect=True)
        run.queue.claim.side_effect = [[], [], [item]]
        self.assertEqual(run.claim_available(), [item])
        self.assertEqual([c.kwargs['phase'] for c in run.queue.claim.call_args_list], ['publisher', 'browser', 'archive'])

    def test_browser_backlog_does_not_block_archive_admission(self):
        run = bare_run()
        run.config['workers'] = 48
        for index in range(44):
            item = claim()
            item.article_id = 'publisher-' + str(index)
            run.claims[item.article_id] = item
        for index in range(2):
            item = claim()
            item.article_id = 'browser-' + str(index)
            item.phase = 'browser'
            run.claims[item.article_id] = item
        run.queue.claim.return_value = []
        run.claim_available()
        run.queue.claim.assert_called_once()
        self.assertEqual(run.queue.claim.call_args.kwargs['phase'], 'archive')
        self.assertEqual(run.queue.claim.call_args.kwargs['limit'], 2)


if __name__ == '__main__':
    unittest.main()
