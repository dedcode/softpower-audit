"""Offline behavior checks: no publisher or cloud requests."""
import gzip,json,sys,unittest
from pathlib import Path
from unittest.mock import patch
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
 def test_transient_retry_bound(self):
  with patch.object(self.f,'one',return_value=(503,{},b'unavailable',False)),patch.object(crawl.time,'sleep') as sleep:r=self.fetch()
  self.assertEqual(len(r['attempts']),3);self.assertEqual(sleep.call_count,2);self.assertEqual(r['status'],'temporary_error')
 def test_homepage_redirect(self):
  with patch.object(self.f,'one',side_effect=[(302,{'Location':'/'},b'',False),(200,{'Content-Type':'text/html'},b'<title>Home</title>',False)]):r=self.fetch()
  self.assertEqual(r['status'],'needs_inspection');self.assertEqual(r['error'],'Redirected to homepage')
 def test_private_destination_rejected(self):
  with patch.object(crawl.socket,'getaddrinfo',return_value=[(2,1,6,'',('127.0.0.1',443))]):
   with self.assertRaises(ValueError):crawl.public_url('https://example.org/')
if __name__=='__main__':unittest.main()
