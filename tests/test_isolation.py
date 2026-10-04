import sys,unittest,time
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
from isolation import extract_isolated,run_task,IsolationError
from extract import extract
from pipeline import Pipeline
from crawl import Fetcher
from test_pipeline import Bucket

class IsolationTests(unittest.TestCase):
 def test_fidelity_same_parser_in_fresh_process(self):
  for body in [b'<html><title>404 Not Found</title></html>',('<article><p>'+('Real reporting. '*70)+'</p></article>').encode(),('<div class="story-content"><p>'+('Introduction. '*60)+'</p><p>Get full access for Ksh299.</p></div>').encode()]:
   self.assertEqual(extract_isolated(body,'https://example.org/story'),extract(body,'https://example.org/story'))
 def test_timeout_reaps_process(self):
  with self.assertRaises(IsolationError):run_task('extract',body=b'<html/>',url='https://example.org/story',timeout=-1)
  self.assertEqual(extract_isolated(b'', 'https://example.org/story')['quality'],'missing')
 def test_memory_guard_is_retryable(self):
  with patch('isolation.memory_pressure',return_value=True),self.assertRaises(IsolationError):
   extract_isolated(b'<html/>','https://example.org/story')
 def test_parser_failure_not_overwritten(self):
  f=Pipeline(Bucket(),'run',max_attempts=1)
  got={'status':'retrieved','attempts':[],'http_status':200,'raw_uri':'gs://test/raw','final_url':'https://example.org/x'}
  with patch.object(Fetcher,'fetch',return_value=got),patch.object(f,'read',return_value=b'<html/>'),patch('pipeline.extract',side_effect=IsolationError('memory guard')),patch.object(f,'render',return_value=(b'<html/>','https://example.org/x')),patch.object(f,'one',return_value=(200,{},b'{"archived_snapshots":{}}',False)):
   result=f.fetch({'url':'https://example.org/x','outlet':'example.org'},'run','KE')
  self.assertEqual(result['status'],'deferred')
if __name__=='__main__':unittest.main()
