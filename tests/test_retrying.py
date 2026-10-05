import sys,unittest,json
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
from retrying import RetryingPipeline
from pipeline import Pipeline
from crawl import key
class Bucket:
 name='test'
 def __init__(self):self.data={}
 def blob(self,path):
  data=self.data
  class Blob:
   def exists(self):return path in data
   def download_as_text(self):return data[path]
   def upload_from_string(self,value,**kw):data[path]=value
  return Blob()
class RetryTests(unittest.TestCase):
 def result(self,status):return {'status':status,'attempts':[{'stage':'final','status':status}],'response_bytes':1,'stored_bytes':1}
 def test_retry_then_success(self):
  f=RetryingPipeline(Bucket(),'run')
  with patch.object(Pipeline,'fetch',side_effect=[self.result('deferred'),self.result('saved')]) as fetch,patch('retrying.time.sleep'):
   r=f.fetch({'url':'https://example.org/a','outlet':'example.org'},'run','KE')
  self.assertEqual(fetch.call_count,2);self.assertEqual(r['status'],'saved');self.assertEqual(len(r['attempts']),2)
 def test_exhausted_retries_are_failed(self):
  f=RetryingPipeline(Bucket(),'run')
  with patch.object(Pipeline,'fetch',side_effect=[self.result('deferred'),self.result('deferred')]),patch('retrying.time.sleep'):
   r=f.fetch({'url':'https://example.org/a','outlet':'example.org'},'run','KE')
  self.assertEqual(r['status'],'failed');self.assertEqual(r['attempts'][-1]['status'],'failed')
 def test_claim_loss_during_lower_pipeline_does_not_consume_retry_or_cache_failure(self):
  f=RetryingPipeline(Bucket(),'run');item={'url':'https://example.org/a','outlet':'example.org'}
  def lost(*_):
   f.abort_event.set()
   return self.result('deferred')
  with patch.object(Pipeline,'fetch',side_effect=lost):
   with self.assertRaisesRegex(RuntimeError,'ownership lost'):f.fetch(item,'run','KE')
  self.assertEqual(f.bucket.data,{})
 def test_expired_article_guard_preserves_previous_retry_state(self):
  f=RetryingPipeline(Bucket(),'run');item={'url':'https://example.org/a','outlet':'example.org'}
  path='runs/run/retry-state/'+key(item['url'])+'.json'
  previous=json.dumps({'passes':1,'history':[],'response_bytes':1,'stored_bytes':1,'result':self.result('deferred')})
  f.bucket.data[path]=previous;expired=[False]
  def guard():
   if expired[0]:raise RuntimeError('Article lease expired')
  f.request_context.guard=guard
  def lost(*_):
   expired[0]=True
   return self.result('saved')
  with patch.object(Pipeline,'fetch',side_effect=lost),patch('retrying.time.sleep'):
   with self.assertRaisesRegex(RuntimeError,'Article lease expired'):f.fetch(item,'run','KE')
  self.assertEqual(f.bucket.data,{path:previous})
 def test_cached_retry_result_cannot_return_or_write_after_ownership_loss(self):
  f=RetryingPipeline(Bucket(),'run');item={'url':'https://example.org/a','outlet':'example.org'}
  cached=json.dumps({'passes':2,'history':[],'response_bytes':1,'stored_bytes':1,'result':self.result('deferred')})
  class LostDuringRead:
   def exists(self):return True
   def download_as_text(self):
    f.abort_event.set()
    return cached
  with patch.object(f.bucket,'blob',return_value=LostDuringRead()),patch.object(f,'put') as put,patch.object(Pipeline,'fetch') as fetch:
   with self.assertRaisesRegex(RuntimeError,'ownership lost'):f.fetch(item,'run','KE')
  put.assert_not_called();fetch.assert_not_called()
 def test_guard_rechecked_after_retry_delay_before_any_new_pass(self):
  f=RetryingPipeline(Bucket(),'run');item={'url':'https://example.org/a','outlet':'example.org'}
  path='runs/run/retry-state/'+key(item['url'])+'.json'
  previous=json.dumps({'passes':1,'history':[],'response_bytes':1,'stored_bytes':1,'result':self.result('deferred')})
  f.bucket.data[path]=previous
  with patch.object(Pipeline,'fetch') as fetch,patch('retrying.time.sleep',side_effect=lambda _:f.abort_event.set()):
   with self.assertRaisesRegex(RuntimeError,'ownership lost'):f.fetch(item,'run','KE')
  fetch.assert_not_called();self.assertEqual(f.bucket.data,{path:previous})
if __name__=='__main__':unittest.main()
