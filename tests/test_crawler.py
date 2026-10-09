"""Offline behavior checks: no publisher or cloud requests."""
import gzip,json,sys,unittest
from pathlib import Path
from unittest.mock import patch
from unittest.mock import Mock
from types import SimpleNamespace
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
import crawl

class Blob:
 def __init__(self,bucket,path):self.bucket=bucket;self.path=path
 def exists(self):return self.path in self.bucket.data
 def download_as_text(self):return self.bucket.data[self.path]
 def upload_from_string(self,data,**kwargs):self.bucket.data[self.path]=data
class Bucket:
 name='test-private'
 def __init__(self):self.data={}
 def blob(self,path):return Blob(self,path)

class RetrievalTests(unittest.TestCase):
 def setUp(self):
  self.bucket=Bucket();self.f=crawl.Fetcher(self.bucket,'run',max_attempts=3)
  self.item={'url':'https://example.org/news/123','outlet':'example.org','source_table':'p.d.t'}
  self.policy=patch.object(self.f,'policy',return_value=(None,True,3,'gs://robots')).start();self.addCleanup(patch.stopall)
 def fetch(self):return self.f.fetch(self.item,'run','KE')
 def test_original_and_text_preserved(self):
  body=b'<html><title>Railway</title><article>Actual original</article></html>'
  with patch.object(self.f,'one',return_value=(200,{'Content-Type':'text/html'},body,False)),patch('trafilatura.extract',return_value='article '*100):r=self.fetch()
  self.assertEqual(r['status'],'saved');self.assertEqual(gzip.decompress(self.bucket.data[r['raw_uri'].split('test-private/')[1]]),body)
  with patch.object(self.f,'one',side_effect=AssertionError('must not refetch')):reused=self.fetch()
  self.assertTrue(reused['reused']);self.assertEqual(reused['raw_uri'],r['raw_uri'])
 def test_missing_and_blocked(self):
  for code,expected in [(404,'unavailable'),(410,'unavailable'),(403,'blocked'),(401,'blocked')]:
   with patch.object(self.f,'one',return_value=(code,{},b'error',False)):r=self.fetch()
   self.assertEqual(r['status'],expected);self.assertEqual(len(r['attempts']),1);self.assertTrue(r['raw_uri'])
 def test_challenge_not_saved(self):
  with patch.object(self.f,'one',return_value=(200,{'Content-Type':'text/html'},b'<title>Just a moment</title>',False)):r=self.fetch()
  self.assertEqual(r['status'],'blocked')
 def test_robots_disallow(self):
  parser=RobotFileParser();parser.parse(['User-agent: *','Disallow: /'])
  self.policy.return_value=(parser,True,3,'gs://robots')
  with patch.object(self.f,'one',side_effect=AssertionError('robots must stop fetch')):r=self.fetch()
  self.assertEqual(r['status'],'robots_denied');self.assertIsNone(r['raw_uri'])
 def test_retry_after_deferred(self):
  with patch.object(self.f,'one',return_value=(429,{'Retry-After':'3600'},b'slow down',False)),patch.object(crawl.time,'sleep') as sleep:r=self.fetch()
  self.assertEqual(r['retry_after_seconds'],3600);sleep.assert_not_called();self.assertEqual(len(r['attempts']),1)
 def test_publisher_rate_limit_returns_first_preserved_response_without_internal_retry(self):
  body=b'<html><title>Too many requests</title><p>Please try later.</p></html>'
  headers={'Retry-After':'30','Content-Type':'text/html'}
  with patch.object(self.f,'one',return_value=(429,headers,body,False)) as one,patch.object(crawl.time,'sleep') as sleep:r=self.fetch()
  one.assert_called_once_with(self.item['url'],3);sleep.assert_not_called()
  self.assertEqual(r['status'],'rate_limited');self.assertEqual(r['http_status'],429)
  self.assertEqual(r['retry_after_seconds'],30);self.assertEqual(len(r['attempts']),1)
  event=r['attempts'][0];self.assertEqual(event['http_status'],429);self.assertEqual(event['retry_after_seconds'],30)
  self.assertEqual(gzip.decompress(self.bucket.data[r['raw_uri'].split('test-private/')[1]]),body)
  metadata=json.loads(self.bucket.data[event['response_metadata_uri'].split('test-private/')[1]])
  self.assertEqual(metadata['headers'],headers);self.assertEqual(metadata['status'],429)
  self.assertEqual(metadata['body_uri'],r['raw_uri']);self.assertEqual(r['robots_uri'],'gs://robots')
  self.assertNotIn('maximum attempts',r.get('error') or '')
  stored=json.loads(self.bucket.data['latest/'+r['article_id']+'.json'])
  self.assertEqual(stored['status'],'rate_limited');self.assertEqual(stored['retry_after_seconds'],30)
 def test_rate_limit_without_retry_after_or_with_partial_body_still_returns_once(self):
  for headers,large,expected_wait in [({},False,10),({'Retry-After':'5'},True,5)]:
   with self.subTest(headers=headers,large=large),patch.object(self.f,'one',return_value=(429,headers,b'partial denial',large)) as one,patch.object(crawl.time,'sleep') as sleep:r=self.fetch()
   self.assertEqual(one.call_count,1);sleep.assert_not_called()
   self.assertEqual(r['status'],'rate_limited');self.assertEqual(r['retry_after_seconds'],expected_wait)
   self.assertEqual(len(r['attempts']),1);self.assertTrue(r['raw_uri'])
   metadata=json.loads(self.bucket.data[r['attempts'][0]['response_metadata_uri'].split('test-private/')[1]])
   self.assertEqual(metadata['truncated'],large)
   if large:self.assertIn('partial',r['error'])
 def test_server_error_500_keeps_internal_retry_behavior(self):
  with patch.object(self.f,'one',return_value=(500,{},b'server error',False)) as one,patch.object(crawl.time,'sleep') as sleep:r=self.fetch()
  self.assertEqual(one.call_count,3);self.assertEqual(sleep.call_count,2)
  self.assertEqual(len(r['attempts']),3);self.assertEqual(r['status'],'temporary_error')
  self.assertIn('maximum attempts reached',r['error'])
 def test_transient_retry_bound(self):
  with patch.object(self.f,'one',return_value=(503,{},b'unavailable',False)),patch.object(crawl.time,'sleep') as sleep:r=self.fetch()
  self.assertEqual(len(r['attempts']),3);self.assertEqual(sleep.call_count,2);self.assertEqual(r['status'],'temporary_error')
 def test_homepage_redirect(self):
  with patch.object(self.f,'one',side_effect=[(302,{'Location':'/'},b'',False),(200,{'Content-Type':'text/html'},b'<title>Home</title>',False)]):r=self.fetch()
  self.assertEqual(r['status'],'needs_inspection');self.assertEqual(r['error'],'Redirected to homepage')
 def test_private_destination_rejected(self):
  with patch.object(crawl.socket,'getaddrinfo',return_value=[(2,1,6,'',('127.0.0.1',443))]):
   with self.assertRaises(ValueError):crawl.public_url('https://example.org/')

class ArchiveServiceTests(unittest.TestCase):
 def setUp(self):
  self.fetcher=crawl.Fetcher(Bucket(),'run',delay=0,max_attempts=3)
  self.clock=[100.]
  self.addCleanup(patch.stopall)
  patch('crawl.public_url',side_effect=urlsplit).start()
  patch('crawl.time.monotonic',side_effect=lambda:self.clock[0]).start()
  patch('crawl.time.time',side_effect=lambda:1000.+self.clock[0]).start()
  self.sleep=patch('crawl.time.sleep').start()
  self.item={'url':'https://web.archive.org/web/20200101/https://publisher.test/a','outlet':'publisher.test'}
 def fetch(self):return self.fetcher.fetch(self.item,'run','KE')
 def test_robots_timeout_is_shared_negative_cache_then_rechecked_after_ttl(self):
  with patch.object(self.fetcher,'one',side_effect=crawl.requests.ReadTimeout('archive robots unavailable')) as one:
   first=self.fetch();second=self.fetch()
   self.assertEqual(one.call_count,1)
  for result in (first,second):
   self.assertEqual(result['status'],'robots_unavailable')
   self.assertEqual(len(result['attempts']),1)
   self.assertEqual(result['service_retry_at'],1130.)
   self.assertIsNone(result['raw_uri'])
  self.sleep.assert_not_called()
  self.clock[0]+=31
  body=b'<html><title>Article</title>Real article</html>'
  with patch.object(self.fetcher,'one',side_effect=[(200,{},b'User-agent: *\nAllow: /',False),(200,{'Content-Type':'text/html'},body,False)]) as one,patch('trafilatura.extract',return_value='article '*100):
   recovered=self.fetch()
  self.assertEqual(recovered['status'],'saved');self.assertEqual(one.call_count,2)
  self.assertNotIn('service_retry_at',recovered)
 def test_http_robots_failure_is_not_cached_forever_or_bypassed(self):
  with patch.object(self.fetcher,'one',return_value=(503,{},b'temporarily down',False)) as one:
   first=self.fetch();second=self.fetch()
  self.assertEqual(one.call_count,1)
  self.assertEqual(first['status'],'robots_unavailable');self.assertEqual(second['status'],'robots_unavailable')
  self.assertNotIn('https://web.archive.org',self.fetcher.robots)
  self.sleep.assert_not_called()
 def test_archive_transport_error_yields_queue_retry_without_three_attempt_sleeps(self):
  with patch.object(self.fetcher,'policy',return_value=(None,True,0,None)),patch.object(self.fetcher,'one',side_effect=crawl.requests.ConnectTimeout('service down')) as one:
   result=self.fetch()
  self.assertEqual(result['status'],'temporary_error');self.assertEqual(one.call_count,1)
  self.assertEqual(result['service_retry_at'],1130.);self.sleep.assert_not_called()
 def test_shared_cooldown_returns_fast_without_sending_http_or_acquisition_sleeps(self):
  coordinator=Mock();coordinator.try_acquire.return_value=(None,SimpleNamespace(cooldown_seconds=25.,wait_seconds=25.))
  self.fetcher.host_coordinator=coordinator
  with patch.object(self.fetcher,'policy',return_value=(None,True,0,None)),patch('crawl.requests.Session') as session:
   result=self.fetch()
  self.assertEqual(result['status'],'temporary_error');self.assertEqual(result['service_retry_at'],1125.)
  coordinator.sleep.assert_not_called();session.assert_not_called()
  with patch.object(self.fetcher,'policy',side_effect=AssertionError('Cooldown must not retry robots')),patch.object(self.fetcher,'one',side_effect=AssertionError('Cooldown must not send HTTP')):
   cached_pause=self.fetch()
  self.assertEqual(cached_pause['status'],'temporary_error')
  self.assertEqual(cached_pause['service_retry_at'],1125.)
  self.assertEqual(len(cached_pause['attempts']),1)
  with patch('crawl.public_url',side_effect=AssertionError('Fast hint must not resolve DNS')):
   self.assertEqual(self.fetcher.archive_service_retry_at(self.item['url']),1125.)
 def test_archive_negative_cache_defers_shared_host_without_active_lease(self):
  coordinator=Mock();self.fetcher.host_coordinator=coordinator
  with patch.object(self.fetcher,'one',side_effect=crawl.requests.ConnectTimeout('robots unavailable')):
   self.fetch()
  coordinator.defer.assert_called_with('web.archive.org',30.)
 def test_successful_robots_disallow_remains_authoritative(self):
  with patch.object(self.fetcher,'one',return_value=(200,{},b'User-agent: *\nDisallow: /web/',False)) as one:
   first=self.fetch();second=self.fetch()
  self.assertEqual(one.call_count,1)
  for result in (first,second):
   self.assertEqual(result['status'],'robots_denied');self.assertNotIn('service_retry_at',result)
  self.sleep.assert_not_called()
if __name__=='__main__':unittest.main()
