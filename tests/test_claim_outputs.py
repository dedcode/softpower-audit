"""A stale claim cannot overwrite the output URI accepted from its successor."""
import gzip
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
from crawl import Fetcher,key
from pipeline import Pipeline
from retrying import RetryingPipeline
from test_retrying import Bucket


class ClaimOutputTests(unittest.TestCase):
 def setUp(self):
  self.bucket=Bucket();self.item={'url':'https://example.org/story','outlet':'example.org'}
  self.aid=key(self.item['url'])

 def test_two_claims_return_distinct_immutable_text_uris_for_same_article(self):
  fetcher=Pipeline(self.bucket,'run')
  got={'status':'retrieved','attempts':[],'http_status':200,'raw_uri':'gs://test/raw',
       'final_url':self.item['url']}
  results=[]
  for token,text in [('first','First extracted text'),('replacement','Replacement text')]:
   fetcher.request_context.output_scope=self.aid+'/'+token
   with patch.object(Fetcher,'fetch',return_value=got),patch.object(fetcher,'read',return_value=b'html'), \
        patch('pipeline.extract',return_value={'quality':'candidate','text':text}):
    results.append(fetcher.fetch(self.item,'run','KE'))
  self.assertNotEqual(results[0]['text_uri'],results[1]['text_uri'])
  for result,text in zip(results,['First extracted text','Replacement text']):
   path=result['text_uri'].removeprefix('gs://test/')
   self.assertEqual(gzip.decompress(self.bucket.data[path]).decode(),text)
   self.assertTrue(path.startswith('runs/run/claims/'+self.aid+'/'))

 def test_retry_cache_reads_and_writes_are_isolated_by_claim(self):
  fetcher=RetryingPipeline(self.bucket,'run')
  deferred={'status':'deferred','attempts':[],'response_bytes':0,'stored_bytes':0}
  saved={'status':'saved','attempts':[],'response_bytes':0,'stored_bytes':0}
  fetcher.request_context.output_scope=self.aid+'/first'
  with patch.object(Pipeline,'fetch',return_value=deferred) as first,patch('retrying.time.sleep'):
   self.assertEqual(fetcher.fetch(self.item,'run','KE')['status'],'failed')
  self.assertEqual(first.call_count,2)
  fetcher.request_context.output_scope=self.aid+'/replacement'
  with patch.object(Pipeline,'fetch',return_value=saved) as replacement:
   self.assertEqual(fetcher.fetch(self.item,'run','KE')['status'],'saved')
  replacement.assert_called_once()
  paths=[path for path in self.bucket.data if '/retry-state/' in path]
  self.assertEqual(len(paths),2)
  self.assertTrue(all('/claims/'+self.aid+'/' in path for path in paths))

 def test_unscoped_legacy_paths_and_content_addressed_paths_stay_unchanged(self):
  fetcher=Fetcher(self.bucket,'run')
  self.assertEqual(fetcher.put('runs/run/extracted/article.txt.gz',b'legacy','application/gzip'),
                   'gs://test/runs/run/extracted/article.txt.gz')
  fetcher.request_context.output_scope=self.aid+'/token'
  for path in ['raw/article/hash.body.gz','text/article/hash.txt.gz','latest/article.json','runs/other/result.json']:
   self.assertEqual(fetcher.put(path,b'content','application/octet-stream'),'gs://test/'+path)

 def test_ownership_guard_runs_before_any_object_write(self):
  fetcher=Fetcher(self.bucket,'run');fetcher.request_context.output_scope=self.aid+'/old'
  def expired():raise RuntimeError('Article lease expired')
  fetcher.request_context.guard=expired
  with self.assertRaisesRegex(RuntimeError,'Article lease expired'):
   fetcher.put('runs/run/extracted/article.txt.gz',b'stale','application/gzip')
  self.assertEqual(self.bucket.data,{})


if __name__=='__main__':unittest.main()
