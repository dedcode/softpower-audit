import gzip
import hashlib
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'backend'))
import downloads
from fastapi import FastAPI
from fastapi.testclient import TestClient


class DownloadTests(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.include_router(downloads.router)
        self.client = TestClient(app)
        self.url = 'https://example.cl/story'
        self.aid = hashlib.sha256(self.url.encode()).hexdigest()
        self.run = 'ci-full-20261011-034158'
        self.result_path = f'runs/{self.run}/distributed/results/{self.aid}/token123.json.gz'
        self.text_path = f'runs/{self.run}/claims/{self.aid}/token123/extracted/{self.aid}.txt.gz'
        self.row = {'state': 'done', 'status': 'saved', 'item': {'url': self.url},
                    'result_uri': 'gs://' + downloads.BUCKET + '/' + self.result_path}
        self.result = {'article_id': self.aid, 'url': self.url, 'run_id': self.run, 'country': 'CI',
                       'status': 'saved', 'outlet': 'example.cl', 'raw_uri': 'raw-evidence',
                       'text_uri': 'gs://' + downloads.BUCKET + '/' + self.text_path,
                       'attempts': [{'stage': 'extract:http', 'title': 'Wrong candidate', 'raw_uri': 'other'},
                                    {'stage': 'extract:archive', 'title': '<script>article title</script>', 'raw_uri': 'raw-evidence'}]}

    def open_article(self, row=None, result=None):
        doc = SimpleNamespace(exists=True, to_dict=lambda: row if row is not None else self.row)
        db = MagicMock()
        db.collection.return_value.document.return_value.collection.return_value.document.return_value.get.return_value = doc
        data = result if result is not None else self.result
        def read(path, limit):
            if path == self.result_path:
                return json.dumps(data).encode()
            if path == self.text_path:
                return b'Original stored text.\n\n<script>untrusted markup</script>'
            raise AssertionError('Unexpected storage read: ' + path)
        with patch.object(downloads, 'database', return_value=db), patch.object(downloads, 'progress', return_value={'run_id': self.run}), patch.object(downloads, 'read_object', side_effect=read) as reader:
            response = self.client.get('/downloaded-article', params={'country': 'CI', 'url': self.url})
        return response, reader

    def test_viewer_reads_accepted_text_and_matching_title(self):
        response, reader = self.open_article()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['title'], '<script>article title</script>')
        self.assertEqual(response.json()['text'], 'Original stored text.\n\n<script>untrusted markup</script>')
        self.assertEqual([call.args[0] for call in reader.call_args_list], [self.result_path, self.text_path])
        self.assertNotIn('text_uri', response.json())

    def test_partial_remains_explicitly_partial(self):
        response, _ = self.open_article({**self.row, 'status': 'partial'}, {**self.result, 'status': 'partial'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['status'], 'partial')

    def test_imported_batch_selects_only_the_accepted_article_version(self):
        row = {**self.row, 'result_updated_at': '2026-10-16T00:00:00Z'}
        accepted = {**self.result, 'updated_at': row['result_updated_at']}
        older = {**self.result, 'updated_at': '2026-10-15T00:00:00Z', 'status': 'failed'}
        other = {**accepted, 'article_id': '0' * 64}
        data = b'\n'.join(json.dumps(r).encode() for r in [older, other, accepted])
        self.assertEqual(downloads.accepted_result(data, row, 'CI', self.run, self.aid, self.url), accepted)
        with self.assertRaises(ValueError):
            downloads.accepted_result(json.dumps(older).encode(), row, 'CI', self.run, self.aid, self.url)
        uri = 'gs://' + downloads.BUCKET + f'/runs/{self.run}/checkpoints/abcdef.jsonl.gz'
        self.assertTrue(downloads.evidence_path(uri, self.run, self.aid).endswith('abcdef.jsonl.gz'))

    def test_unknown_pending_and_wrong_url_cannot_open_storage(self):
        for row in [{}, {**self.row, 'state': 'leased'}, {**self.row, 'status': 'failed'},
                    {**self.row, 'item': {'url': 'https://another.cl/story'}}]:
            response, reader = self.open_article(row)
            self.assertEqual(response.status_code, 404)
            reader.assert_not_called()

    def test_evidence_cannot_cross_country_or_article(self):
        for result in [{**self.result, 'country': 'KE'}, {**self.result, 'article_id': '0' * 64},
                       {**self.result, 'url': 'https://other.cl'}, {**self.result, 'status': 'partial'}]:
            with self.assertLogs(level='ERROR'):
                response, reader = self.open_article(result=result)
            self.assertEqual(response.status_code, 503)
            self.assertEqual(reader.call_count, 1)

    def test_no_arbitrary_bucket_or_raw_html_access(self):
        for uri in ['gs://another-bucket/' + self.text_path,
                    'gs://' + downloads.BUCKET + '/raw/private.body.gz',
                    'https://example.cl/data',
                    'gs://' + downloads.BUCKET + '/' + self.text_path + '?generation=1',
                    'gs://' + downloads.BUCKET + '/' + self.text_path.replace(self.aid, '0' * 64)]:
            with self.assertRaises(ValueError):
                downloads.text_path(uri, self.aid)
        with self.assertRaises(ValueError):
            downloads.evidence_path('gs://' + downloads.BUCKET + '/' + self.result_path.replace(self.run, 'other-run'), self.run, self.aid)

    def test_decompression_is_bounded(self):
        self.assertEqual(downloads.unpack(gzip.compress(b'saved text'), 20), b'saved text')
        for data in [b'a' * 101, gzip.compress(b'a' * 100000)]:
            with self.assertRaises(ValueError):
                downloads.unpack(data, 100)

    def test_availability_does_not_expose_private_metadata(self):
        value = {'country': 'CI', 'run_id': self.run, 'updated_at': '2026-10-16',
                 'entries': [[self.url, 'saved']], 'signature': [self.run, 1, 0], 'watermark': 123}
        self.assertEqual(set(downloads.DownloadIndex.public(value)), {'country', 'run_id', 'updated_at', 'entries'})
        with patch.object(downloads.index, 'get', return_value=downloads.DownloadIndex.public(value)):
            response = self.client.get('/downloads?country=CI')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['entries'], [[self.url, 'saved']])
        self.assertEqual(response.headers['cache-control'], 'public, max-age=60')

    def test_unsupported_country_never_reads_cloud(self):
        with patch.object(downloads, 'progress') as reader:
            self.assertEqual(self.client.get('/downloads?country=US').status_code, 422)
            self.assertEqual(self.client.get('/downloaded-article', params={'country': 'US', 'url': self.url}).status_code, 422)
            reader.assert_not_called()

    def test_unchanged_counts_reuse_persistent_index_without_database_scan(self):
        value = {'country': 'CI', 'run_id': self.run, 'updated_at': '2026-10-16',
                 'entries': [[self.url, 'saved']], 'signature': [self.run, 1, 0], 'watermark': 100}
        data = gzip.compress(json.dumps(value).encode())
        target = MagicMock(size=len(data), generation=7)
        target.download_as_bytes.return_value = data
        store = MagicMock()
        store.blob.return_value = target
        with patch.object(downloads, 'bucket', return_value=store), patch.object(downloads, 'progress', return_value={
                'country': 'CI', 'run_id': self.run, 'counts': {'saved': 1, 'partial': 0}}), patch.object(downloads, 'database') as db:
            result = downloads.DownloadIndex().get('CI')
        self.assertEqual(result['entries'], [[self.url, 'saved']])
        db.assert_not_called()
        target.upload_from_string.assert_not_called()

    def test_incremental_refresh_preserves_earlier_downloads_and_adds_partial(self):
        value = {'country': 'CI', 'run_id': self.run, 'updated_at': '2026-10-15',
                 'entries': [[self.url, 'saved']], 'signature': [self.run, 1, 0], 'watermark': 1000}
        data = gzip.compress(json.dumps(value).encode())
        target = MagicMock(size=len(data), generation=7)
        target.download_as_bytes.return_value = data
        store = MagicMock()
        store.blob.return_value = target
        db = MagicMock()
        query = db.collection.return_value.document.return_value.collection.return_value
        query.where.return_value = query
        query.select.return_value = query
        query.stream.return_value = [SimpleNamespace(to_dict=lambda: {
            'state': 'done', 'status': 'partial', 'item': {'url': 'https://example.cl/second'}, 'result_uri': 'gs://accepted'})]
        with patch.object(downloads, 'bucket', return_value=store), patch.object(downloads, 'database', return_value=db), patch.object(downloads, 'progress', return_value={
                'country': 'CI', 'run_id': self.run, 'counts': {'saved': 1, 'partial': 1}}):
            result = downloads.DownloadIndex().get('CI')
        self.assertEqual(dict(result['entries']), {self.url: 'saved', 'https://example.cl/second': 'partial'})
        self.assertEqual(query.where.call_args.kwargs['filter'].field_path, 'updated_at')
        self.assertEqual(query.where.call_args.kwargs['filter'].value, 880)
        self.assertEqual(target.upload_from_string.call_args.kwargs['if_generation_match'], 7)
        stored = json.loads(gzip.decompress(target.upload_from_string.call_args.args[0]))
        self.assertEqual(stored['signature'], [self.run, 1, 1])


if __name__ == '__main__':
    unittest.main()
