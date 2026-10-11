import sys,time,threading,http.server,unittest,gzip,io
from pathlib import Path
from unittest.mock import patch
import requests
from google.api_core import exceptions
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
from extract import extract
from http_body import read_body
from infrastructure_recovery import temporary_failure,recoverable_cohort
TEXT='La relación económica entre Chile y China incluye inversión, cooperación y participación de pequeñas empresas. '
class ExtractionTests(unittest.TestCase):
 def test_long_explicit_teaser_cannot_count_as_full_text(self):
  body=('<article><h1>Investment and cooperation in Chile with China</h1><p>'+TEXT*8+'</p><a href="https://original.example/full">Read more</a></article>').encode()
  r=extract(body,'https://www.kenyastar.com/story');self.assertEqual(r['quality'],'partial');self.assertEqual(r['continuations'],['https://original.example/full'])
 def test_related_read_more_does_not_turn_full_article_into_teaser(self):
  for wrapper in ['nav','aside','footer','div class="related"']:
   tag=wrapper.split()[0];body=('<article><p>'+TEXT*8+'</p><'+wrapper+'><a href="https://example.cl/other">Read more</a></'+tag+'></article>').encode()
   r=extract(body,'https://example.cl/story');self.assertEqual(r['quality'],'candidate');self.assertEqual(r['continuations'],[])
 def test_utf8_without_charset_preserves_spanish(self):
  body=('<article class="article-body"><h1>Relación económica con China</h1><p>'+TEXT*8+'</p></article>').encode()
  r=extract(body,'https://example.cl/story');self.assertEqual(r['quality'],'candidate');self.assertEqual(r['title'],'Relación económica con China');self.assertIn(TEXT.strip(),r['text']);self.assertNotIn('Ã',r['text'])
 def test_windows_1252_document_preserves_accents(self):
  body=('<meta charset="windows-1252"><article><p>'+TEXT*8+'</p></article>').encode('cp1252');r=extract(body,'https://example.cl/story');self.assertIn('relación económica',r['text']);self.assertNotIn('�',r['text'])
class Handler(http.server.BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def do_GET(self):
  body=gzip.compress(b'x'*100000) if self.path=='/gzip' else b'x'*100
  self.send_response(200);self.send_header('Content-Length',str(len(body)))
  if self.path=='/gzip':self.send_header('Content-Encoding','gzip')
  self.end_headers()
  try:
   if self.path=='/stall':time.sleep(2)
   for byte in body:
    self.wfile.write(bytes([byte]));self.wfile.flush()
    if self.path in ('/slow','/gzip'):time.sleep(.02)
  except (BrokenPipeError,ConnectionResetError):pass
class BodyTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=cls.server.serve_forever,daemon=True).start();cls.url='http://127.0.0.1:'+str(cls.server.server_port)
 @classmethod
 def tearDownClass(cls):cls.server.shutdown();cls.server.server_close()
 def test_slow_drip_stall_and_compression_are_interruptible(self):
  for path in ['/slow','/stall','/gzip']:
   with self.subTest(path=path),requests.get(self.url+path,stream=True,timeout=5) as response:
    started=time.monotonic();data,truncated=read_body(response,1000000,timeout=.25)
    self.assertTrue(truncated);self.assertLess(time.monotonic()-started,1)
 def test_cancel_interrupts_a_blocked_read(self):
  cancel=threading.Event();timer=threading.Timer(.2,cancel.set)
  with requests.get(self.url+'/stall',stream=True,timeout=5) as response:
   timer.start();started=time.monotonic()
   with self.assertRaises(requests.Timeout):read_body(response,10000,timeout=5,cancelled=cancel.is_set)
   self.assertLess(time.monotonic()-started,1)
  timer.join()
 def test_complete_response_and_body_limit(self):
  with requests.get(self.url+'/normal',stream=True,timeout=5) as response:self.assertEqual(read_body(response,1000),(b'x'*100,False))
  with requests.get(self.url+'/normal',stream=True,timeout=5) as response:self.assertEqual(read_body(response,10),(b'x'*10,True))
class InfrastructureTests(unittest.TestCase):
 def test_only_temporary_failures_qualify(self):
  for error in [exceptions.ServiceUnavailable('outage'),exceptions.TooManyRequests('busy'),exceptions.DeadlineExceeded('slow')]:self.assertTrue(temporary_failure(error))
  for error in [exceptions.Forbidden('permission'),ValueError('bad checkpoint'),RuntimeError('ownership lost')]:self.assertFalse(temporary_failure(error))
  self.assertTrue(temporary_failure(exceptions.RetryError('retries exhausted',exceptions.ServiceUnavailable('outage'))))
  try:
   try:raise exceptions.ServiceUnavailable('earlier outage')
   except exceptions.ServiceUnavailable:raise RuntimeError('ownership lost')
  except RuntimeError as error:self.assertFalse(temporary_failure(error))
 def test_all_latest_peers_must_be_safe(self):
  failed={'state':'failed','infrastructure_recoverable':True}
  self.assertTrue(recoverable_cohort('failed',[failed],1))
  self.assertTrue(recoverable_cohort('failed',[failed,{'state':'completed'}],2))
  for peer in [{'state':'failed'},{'state':'paused_limit'},{'state':'paused_by_operator'},{'state':'running'}]:self.assertFalse(recoverable_cohort('failed',[failed,peer],2))
  self.assertFalse(recoverable_cohort('failed',[failed],2));self.assertFalse(recoverable_cohort('running',[failed],1))
if __name__=='__main__':unittest.main()
