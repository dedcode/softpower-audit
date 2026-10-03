import sys,unittest,json
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
from retrying import RetryingPipeline
from pipeline import Pipeline
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
if __name__=='__main__':unittest.main()
