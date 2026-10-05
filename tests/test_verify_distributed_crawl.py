import unittest
from verify_distributed_crawl import BUCKET, inspect_article_documents, newest_tasks, article_key


class DistributedVerificationTests(unittest.TestCase):
    def fixture(self):
        run_id = 'verify-distributed-unit'
        workers = [{'owner': f'worker-{i}', 'task_index': str(i), 'task_attempt': '0',
                    'execution': 'execution', 'state': 'completed', 'updated_at': 10} for i in range(2)]
        documents = []
        for index in range(96):
            url = f'https://example.test/article/{index}'
            aid = article_key(url)
            token = f'token-{index}'
            documents.append((aid, {'article_id': aid, 'item': {'url': url}, 'state': 'done',
                'status': 'saved', 'owner': f'worker-{index % 2}', 'claim_token': token,
                'needs_export': False,
                'result_uri': f'gs://{BUCKET}/runs/{run_id}/distributed/results/{aid}/{token}.json.gz'}))
        return run_id, workers, documents

    def test_accepts_96_unique_saved_articles_with_both_instances_participating(self):
        run_id, workers, documents = self.fixture()
        counts, refs = inspect_article_documents(documents, workers, run_id)
        self.assertEqual(counts, {'0': 48, '1': 48})
        self.assertEqual(len(refs), 96)
        self.assertEqual(set(newest_tasks(workers, 'execution')), {'0', '1'})

    def test_rejects_pending_outbox_due_or_wrong_evidence_reference(self):
        for field, value in [('needs_export', True), ('due_at', 0), ('result_uri', 'gs://other/wrong')]:
            with self.subTest(field=field):
                run_id, workers, documents = self.fixture()
                documents[0][1][field] = value
                with self.assertRaises(AssertionError):
                    inspect_article_documents(documents, workers, run_id)

    def test_rejects_missing_peer_and_nonterminal_latest_attempt(self):
        _, workers, _ = self.fixture()
        with self.assertRaises(AssertionError):
            newest_tasks(workers[:1], 'execution')
        replacement = {**workers[0], 'owner': 'replacement', 'task_attempt': '1',
                       'updated_at': 20, 'state': 'running'}
        with self.assertRaises(AssertionError):
            newest_tasks(workers + [replacement], 'execution')


if __name__ == '__main__':
    unittest.main()
