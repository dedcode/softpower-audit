"""Fetcher overlap, robots-policy propagation and real network accounting."""
import sys
import threading
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import urlsplit

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
from crawl import Fetcher
from test_crawler import Bucket


class Lease:
 def __enter__(self):return self
 def __exit__(self,*_):pass
 def check(self):pass


class FetcherConcurrencyTests(unittest.TestCase):
 def test_same_fetcher_can_download_two_responses_at_once(self):
  coordinator=Mock();coordinator.acquire.side_effect=lambda *a,**k:Lease()
  fetcher=Fetcher(Bucket(),'run',delay=1,host_coordinator=coordinator)
  entered=threading.Barrier(3);release=threading.Event();errors=[]
  class Session:
   def __enter__(self):return self
   def __exit__(self,*_):pass
   def get(self,*a,**kw):
    entered.wait(3)
    if not release.wait(3):raise AssertionError('Fixture did not release downloads')
    return nullcontext(SimpleNamespace(status_code=200,headers={},iter_content=lambda _:[b'article']))
  def request():
   try:fetcher.one('https://example.org/article')
   except BaseException as exc:errors.append(exc)
  with patch('crawl.public_url',side_effect=urlsplit),patch('crawl.requests.Session',Session):
   threads=[threading.Thread(target=request) for _ in range(2)]
   for thread in threads:thread.start()
   try:
    entered.wait(3)
    self.assertEqual(fetcher.network_snapshot()['http_in_flight'],2)
    self.assertFalse(fetcher.hostlock('example.org').locked())
   finally:
    release.set()
    for thread in threads:thread.join(4)
  self.assertEqual(errors,[])
  self.assertTrue(all(not t.is_alive() for t in threads))
  metrics=fetcher.network_snapshot()
  self.assertEqual((metrics['http_started'],metrics['http_completed'],metrics['http_in_flight'],metrics['host_waiters']),(2,2,0,0))

 def test_network_exception_releases_metrics_and_permit(self):
  coordinator=Mock();coordinator.acquire.return_value=Lease()
  fetcher=Fetcher(Bucket(),'run',host_coordinator=coordinator)
  session=Mock();session.__enter__=Mock(return_value=session);session.__exit__=Mock(return_value=False)
  session.get.side_effect=OSError('connection failed')
  with patch('crawl.public_url',side_effect=urlsplit),patch('crawl.requests.Session',return_value=session):
   with self.assertRaises(OSError):fetcher.one('https://example.org/a')
  m=fetcher.network_snapshot()
  self.assertEqual((m['http_started'],m['http_errors'],m['http_completed'],m['http_in_flight'],m['host_waiters']),(1,1,0,0,0))

 def test_robots_requirement_is_separate_from_our_start_interval(self):
  for body,expected in [(b'User-agent: *\nCrawl-delay: 20\n',20.),(b'User-agent: *\nRequest-rate: 1/10\n',10.),(b'User-agent: *\nAllow: /\n',0.)]:
   with self.subTest(body=body):
    coordinator=Mock();coordinator.acquire.return_value=Lease()
    fetcher=Fetcher(Bucket(),'run',delay=1,host_coordinator=coordinator)
    with patch('crawl.public_url',side_effect=urlsplit),patch.object(fetcher,'one',return_value=(200,{},body,False)):
     policy=fetcher.policy('https://example.org/a')
    self.assertEqual(policy[2],max(1,expected))
    with fetcher.host_slot('example.org',policy[2]):pass
    self.assertEqual(coordinator.acquire.call_args.kwargs['robots_delay'],expected)
    fetcher.dispatch_availability(['example.org'])
    coordinator.availability.assert_called_once_with(['example.org'],delay=1,robots_delays={'example.org':expected})

 def test_missing_robots_allows_default_but_temporary_failure_does_not_clear_unknown_policy(self):
  for status,expected in [(404,0.),(503,None)]:
   fetcher=Fetcher(Bucket(),'run',delay=1)
   with patch('crawl.public_url',side_effect=urlsplit),patch.object(fetcher,'one',return_value=(status,{},b'',False)):
    fetcher.policy('https://example.org/a')
   self.assertEqual(fetcher.robots_delays.get('example.org'),expected)

 def test_invalid_interval_rejected(self):
  for delay in (-1,float('inf'),float('nan')):
   with self.assertRaises(ValueError):Fetcher(Bucket(),'run',delay=delay)

if __name__=='__main__':unittest.main()
