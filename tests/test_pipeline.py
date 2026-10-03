import sys,unittest,json,gzip
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
from extract import extract
from pipeline import Pipeline
from crawl import Fetcher
class Bucket:
 name='test'
 def blob(self,path):
  class B:
   def upload_from_string(self,*a,**k):pass
  return B()
class PipelineTests(unittest.TestCase):
 def test_advertisement_not_article(self):
  a=extract(b'<html><p>The Standard Group Plc is a media company.</p><p>Subscribe now</p></html>','https://example.org/x')
  self.assertNotEqual(a['quality'],'candidate')
 def test_paywall_never_success(self):
  body=('<div class="story-content"><p>'+('Introduction. '*60)+'</p><p>Get Full Access for Ksh299/Week.</p></div>').encode()
  self.assertEqual(extract(body,'https://example.org/x')['quality'],'partial')
 def test_premium_articles_sidebar_not_paywall(self):
  body=('<div class="story-content"><p>'+('Actual article. '*60)+'</p></div><h3>Premium Articles</h3>').encode()
  self.assertEqual(extract(body,'https://example.org/x')['quality'],'candidate')
 def test_failure_traverses_archive_before_terminal(self):
  f=Pipeline(Bucket(),'run',max_attempts=1)
  got={'status':'unavailable','attempts':[],'http_status':404,'raw_uri':None}
  with patch.object(Fetcher,'fetch',return_value=got),patch.object(f,'one',return_value=(200,{},b'{"archived_snapshots":{}}',False)) as lookup:
   r=f.fetch({'url':'https://example.org/x','outlet':'example.org'},'run','KE')
  self.assertNotIn('timestamp=',lookup.call_args_list[-1].args[0]);self.assertEqual(r['status'],'exhausted');self.assertEqual(sum(e['stage']=='archive_lookup' for e in r['attempts']),2)
 def test_archive_outage_is_deferred(self):
  f=Pipeline(Bucket(),'run',max_attempts=1)
  got={'status':'unavailable','attempts':[],'http_status':404,'raw_uri':None}
  with patch.object(Fetcher,'fetch',return_value=got),patch.object(f,'one',side_effect=TimeoutError('timeout')),patch('pipeline.time.sleep'):
   r=f.fetch({'url':'https://example.org/x','outlet':'example.org'},'run','KE')
  self.assertEqual(r['status'],'deferred')
 def test_partial_runs_browser_and_both_archive_lookups(self):
  f=Pipeline(Bucket(),'run',max_attempts=1)
  body=b'<div class="story-content"><p>A short incomplete preview of the article which is not long enough to qualify as complete text at this stage.</p></div>'
  got={'status':'needs_inspection','attempts':[],'http_status':200,'raw_uri':'gs://test/raw','final_url':'https://example.org/x'}
  with patch.object(Fetcher,'fetch',return_value=got),patch.object(f,'read',return_value=body),patch.object(f,'render',return_value=(body,'https://example.org/x')) as render,patch.object(f,'one',return_value=(200,{},b'{"archived_snapshots":{}}',False)):
   r=f.fetch({'url':'https://example.org/x','outlet':'example.org'},'run','KE')
  render.assert_called_once();self.assertEqual(r['status'],'partial');self.assertEqual(sum(e['stage']=='archive_lookup' for e in r['attempts']),2)
 def test_empty_article_marker_cannot_be_replaced_by_navigation(self):
  body=b'<meta property="og:type" content="article"><section class="body-copy"></section><p>Unrelated headlines</p>'
  with patch('extract.trafilatura.extract',return_value='Unrelated headlines. '*60):
   self.assertNotEqual(extract(body,'https://example.org/x')['quality'],'candidate')
if __name__=='__main__':unittest.main()
