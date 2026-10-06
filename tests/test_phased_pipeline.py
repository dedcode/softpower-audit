"""Publisher slots are released before durable browser and archive recovery."""
import copy
import gzip
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
from crawl import Fetcher,key
from extractor_version import VERSION as EXTRACTOR_VERSION
from retrying import RetryingPipeline
from test_retrying import Bucket
from test_host_queue import Clock


class PhasedPipelineTests(unittest.TestCase):
 def setUp(self):
  self.bucket=Bucket();self.item={'url':'https://publisher.example/story','outlet':'publisher.example','first_observed':'2020-01-02'}
  self.aid=key(self.item['url']);self.pipeline=self.worker('publisher-token')
  self.missing={'status':'unavailable','attempts':[{'http_status':404}], 'http_status':404,
                'raw_uri':None,'response_bytes':10,'stored_bytes':4}
  self.no_snapshot=(200,{},b'{"archived_snapshots":{}}',False)

 def worker(self,token):
  worker=RetryingPipeline(self.bucket,'run',max_attempts=1)
  worker.request_context.output_scope=self.aid+'/'+token
  return worker

 def publisher_missing(self,worker=None):
  worker=worker or self.pipeline
  with patch.object(Fetcher,'fetch',return_value=copy.deepcopy(self.missing)),patch.object(worker,'one') as archive:
   result=worker.fetch_phase(self.item,'run','KE')
  archive.assert_not_called()
  return result

 def resume(self,queued,worker=None):
  worker=worker or self.worker('archive-token')
  # Simulate the durable JSON checkpoint crossing process/claim boundaries.
  checkpoint=json.loads(json.dumps(queued['_checkpoint']))
  return worker,checkpoint

 def legacy_checkpoint(self,phase,events,best=None):
  return {'version':1,'kind':'phased-toolbox','article_id':self.aid,'run_id':'run','country':'KE',
   'next_phase':phase,'completed_passes':0,'attempts':[],'response_bytes':0,'stored_bytes':0,
   'retry_best':None,'pipeline':{'version':1,'next_phase':phase,'elapsed_seconds':3,'unresolved':False,
    'best':copy.deepcopy(best),'first':{'status':'retrieved','http_status':200,'final_url':self.item['url']},
    'analysis':{'quality':'missing'},
    'result':{**self.item,'article_id':self.aid,'run_id':'run','country':'KE','status':'deferred',
              'response_bytes':20,'stored_bytes':8,'attempts':copy.deepcopy(events)}}}

 def partial_candidate(self):
  return {'quality':'partial','text':'The previously retained partial article text','url':self.item['url'],
          'raw_uri':'gs://test/raw/retained','digest':'retained-digest','http_status':200}

 def test_older_checkpoints_reanalyse_stored_successful_html_before_any_request(self):
  for phase,name,status,version in [('browser','http','retrieved',None),
                                  ('archive','http','saved','older-extractor'),
                                  ('browser','publisher_url_discovery','retrieved','older-extractor'),
                                  ('archive','browser','rendered',None),
                                  ('archive','archive','retrieved','older-extractor')]:
   with self.subTest(phase=phase,stage=name,status=status,version=version):
    raw='gs://test/raw/preserved';source='https://publisher.example/canonical-story';text='Recovered complete article'
    checkpoint=self.legacy_checkpoint(phase,[{'stage':name,'status':status,'raw_uri':raw,
                                             'url':self.item['url'],'final_url':source}],self.partial_candidate())
    if version:checkpoint['pipeline']['extractor_version']=version
    before=copy.deepcopy(checkpoint);worker=self.worker('reanalysis-'+name)
    with patch.object(Fetcher,'fetch') as fetch,patch.object(worker,'one') as network, \
         patch.object(worker,'render') as render,patch.object(worker,'read',return_value=b'preserved HTML') as read, \
         patch('pipeline.extract',return_value={'quality':'candidate','text':text}) as extract:
     result=worker.fetch_phase(self.item,'run','KE',phase=phase,checkpoint=checkpoint)
    fetch.assert_not_called();network.assert_not_called();render.assert_not_called()
    read.assert_called_once_with(raw);extract.assert_called_once_with(b'preserved HTML',source)
    self.assertEqual((result['status'],result['raw_uri'],result['final_url']),('saved',raw,source))
    self.assertEqual(result['response_bytes'],20)
    self.assertEqual(result['stored_bytes'],8+len(gzip.compress(text.encode(),mtime=0)))
    self.assertEqual(result['extractor_version'],EXTRACTOR_VERSION)
    evidence=[event for event in result['attempts'] if event['stage']=='stored_html_reanalysis']
    self.assertEqual(len(evidence),1);self.assertEqual(evidence[0]['source_stage'],name)
    self.assertEqual(evidence[0]['raw_uri'],raw);self.assertEqual(evidence[0]['extractor_version'],EXTRACTOR_VERSION)
    self.assertEqual(checkpoint,before)

 def test_reanalysis_deduplicates_raw_references_and_ignores_unsuccessful_responses(self):
  events=[{'stage':'http','status':'blocked','raw_uri':'gs://test/raw/blocked','url':self.item['url']},
          {'stage':'http','status':'retrieved','raw_uri':'gs://test/raw/first','url':self.item['url']},
          {'stage':'publisher_url_discovery','status':'retrieved','raw_uri':'gs://test/raw/first','url':self.item['url']},
          {'stage':'browser','status':'rendered','raw_uri':'gs://test/raw/second','url':self.item['url']},
          {'stage':'archive','status':'needs_inspection','raw_uri':'gs://test/raw/truncated','url':self.item['url']}]
  checkpoint=self.legacy_checkpoint('archive',events,self.partial_candidate());worker=self.worker('deduplicated')
  with patch.object(Fetcher,'fetch') as fetch,patch.object(worker,'read',return_value=b'HTML') as read, \
       patch('pipeline.extract',side_effect=lambda *_:{'quality':'missing','text':''}) as extract, \
       patch.object(worker,'one',return_value=self.no_snapshot) as lookup:
   result=worker.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
  fetch.assert_not_called();self.assertEqual(lookup.call_count,2)
  self.assertEqual([call.args[0] for call in read.call_args_list],['gs://test/raw/first','gs://test/raw/second'])
  self.assertEqual(extract.call_count,2);self.assertEqual(result['status'],'partial')
  self.assertEqual(result['raw_uri'],'gs://test/raw/retained')
  self.assertEqual(gzip.decompress(self.bucket.data[result['text_uri'].removeprefix('gs://test/')]).decode(),self.partial_candidate()['text'])

 def test_missing_or_corrupt_stored_body_keeps_recovery_and_is_not_reparsed_in_next_phase(self):
  events=[{'stage':'http','status':'retrieved','raw_uri':'gs://test/raw/missing','url':self.item['url']},
          {'stage':'publisher_url_discovery','status':'retrieved','raw_uri':'gs://test/raw/corrupt','url':self.item['url']}]
  checkpoint=self.legacy_checkpoint('browser',events,self.partial_candidate());worker=self.worker('unavailable-body')
  with patch.object(Fetcher,'fetch') as fetch,patch.object(worker,'read',side_effect=[FileNotFoundError('missing'),gzip.BadGzipFile('corrupt')]) as read, \
       patch.object(worker,'render',return_value=(b'browser body',self.item['url'])) as render, \
       patch('pipeline.extract',return_value={'quality':'missing','text':''}) as extract,patch.object(worker,'one') as lookup:
   queued=worker.fetch_phase(self.item,'run','KE',phase='browser',checkpoint=checkpoint)
  fetch.assert_not_called();lookup.assert_not_called();self.assertEqual(read.call_count,2)
  render.assert_called_once();extract.assert_called_once()
  self.assertEqual(queued['next_phase'],'archive')
  self.assertEqual(queued['_checkpoint']['pipeline']['extractor_version'],EXTRACTOR_VERSION)
  evidence=[event for event in queued['attempts'] if event['stage']=='stored_html_reanalysis']
  self.assertEqual([event['status'] for event in evidence],['unavailable','unavailable'])
  archive,checkpoint=self.resume(queued,self.worker('unavailable-body-archive'))
  with patch.object(archive,'read') as read,patch('pipeline.extract') as extract, \
       patch.object(archive,'one',return_value=self.no_snapshot) as lookup:
   result=archive.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
  read.assert_not_called();extract.assert_not_called();self.assertEqual(lookup.call_count,2)
  self.assertEqual((result['status'],result['raw_uri']),('partial','gs://test/raw/retained'))
  self.assertEqual(sum(event['stage']=='stored_html_reanalysis' for event in result['attempts']),2)

 def test_same_extractor_checkpoint_does_not_reanalyse_stored_body(self):
  event={'stage':'http','status':'retrieved','raw_uri':'gs://test/raw/old','url':self.item['url']}
  checkpoint=self.legacy_checkpoint('browser',[event]);checkpoint['pipeline']['extractor_version']=EXTRACTOR_VERSION
  worker=self.worker('current-parser')
  with patch.object(worker,'read') as read,patch.object(worker,'render',return_value=(b'new browser body',self.item['url'])) as render, \
       patch('pipeline.extract',return_value={'quality':'candidate','text':'Complete rendered article'}) as extract:
   result=worker.fetch_phase(self.item,'run','KE',phase='browser',checkpoint=checkpoint)
  read.assert_not_called();render.assert_called_once();extract.assert_called_once_with(b'new browser body',self.item['url'])
  self.assertEqual(result['status'],'saved')
  self.assertFalse(any(event['stage']=='stored_html_reanalysis' for event in result['attempts']))

 def test_publisher_yields_before_any_archive_work_and_is_not_terminal(self):
  result=self.publisher_missing()
  self.assertEqual((result['status'],result['next_phase']),('queued','archive'))
  self.assertEqual(result['_checkpoint']['completed_passes'],0)
  self.assertEqual(result['response_bytes'],10)
  self.assertEqual(result['stored_bytes'],4)
  self.assertFalse(any(event['stage'].startswith('archive') or event['stage']=='final' for event in result['attempts']))
  self.assertFalse(self.pipeline.live)

 def test_resumed_archive_never_refetches_publisher_and_preserves_attempts_once(self):
  queued=self.publisher_missing();worker,checkpoint=self.resume(queued);before=copy.deepcopy(checkpoint)
  with patch.object(Fetcher,'fetch') as fetch,patch.object(worker,'one',return_value=self.no_snapshot) as lookup:
   result=worker.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
  fetch.assert_not_called();self.assertEqual(lookup.call_count,2)
  self.assertEqual(result['status'],'exhausted')
  self.assertEqual((result['response_bytes'],result['stored_bytes']),(10,4))
  self.assertEqual(sum(event['stage']=='http' for event in result['attempts']),1)
  self.assertEqual(checkpoint,before,'Input checkpoint must remain reusable after a crash')
  self.assertNotIn('_checkpoint',result)

 def test_partial_text_survives_handoff_to_another_claim(self):
  body=b'publisher page';text='Retained partial article text'
  got={**self.missing,'status':'retrieved','http_status':200,'raw_uri':'gs://test/raw/immutable','final_url':self.item['url']}
  with patch.object(Fetcher,'fetch',return_value=got),patch.object(self.pipeline,'read',return_value=body), \
       patch('pipeline.extract',side_effect=lambda *_:{'quality':'partial','text':text}), \
       patch.object(self.pipeline,'render') as render,patch.object(self.pipeline,'one') as lookup:
   queued=self.pipeline.fetch_phase(self.item,'run','KE')
  lookup.assert_not_called();render.assert_not_called();self.assertEqual(queued['next_phase'],'browser')
  browser,checkpoint=self.resume(queued,self.worker('browser-token'))
  with patch.object(Fetcher,'fetch') as fetch,patch.object(browser,'render',return_value=(body,self.item['url'])), \
       patch('pipeline.extract',return_value={'quality':'partial','text':text}),patch.object(browser,'one') as lookup:
   queued=browser.fetch_phase(self.item,'run','KE',phase='browser',checkpoint=checkpoint)
  fetch.assert_not_called();lookup.assert_not_called();self.assertEqual(queued['next_phase'],'archive')
  worker,checkpoint=self.resume(queued)
  self.assertEqual(checkpoint['pipeline']['best']['text'],text)
  with patch.object(Fetcher,'fetch') as fetch,patch.object(worker,'one',return_value=self.no_snapshot):
   result=worker.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
  fetch.assert_not_called();self.assertEqual(result['status'],'partial')
  self.assertEqual(result['raw_uri'],'gs://test/raw/immutable')
  self.assertIn('/archive-token/extracted/',result['text_uri'])
  self.assertEqual(gzip.decompress(self.bucket.data[result['text_uri'].removeprefix('gs://test/')]).decode(),text)
  self.assertEqual(result['response_bytes'],queued['response_bytes'])
  self.assertEqual(result['stored_bytes'],queued['stored_bytes']+len(gzip.compress(text.encode(),mtime=0)))

 def test_archive_can_finish_article_after_process_restart(self):
  queued=self.publisher_missing();worker,checkpoint=self.resume(queued)
  snapshot='https://web.archive.org/web/20200102/https://publisher.example/story'
  payload=json.dumps({'archived_snapshots':{'closest':{'available':True,'url':snapshot,'timestamp':'20200102'}}}).encode()
  got={'status':'retrieved','attempts':[{'http_status':200}],'http_status':200,
       'raw_uri':'gs://test/raw/archive-content','final_url':snapshot,'response_bytes':30,'stored_bytes':8}
  text='Full recovered article text'
  with patch.object(worker,'one',return_value=(200,{},payload,False)) as lookup, \
       patch.object(Fetcher,'fetch',return_value=got) as fetch,patch.object(worker,'read',return_value=b'archive body'), \
       patch('pipeline.extract',return_value={'quality':'candidate','text':text}):
   result=worker.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
  lookup.assert_called_once();self.assertEqual(fetch.call_args.args[0]['url'],snapshot)
  self.assertEqual(result['status'],'saved');self.assertEqual(result['response_bytes'],40)
  self.assertEqual(result['stored_bytes'],12+len(gzip.compress(text.encode(),mtime=0)))
  self.assertEqual(sum(event['stage']=='http' for event in result['attempts']),1)
  self.assertEqual(sum(event['stage']=='archive' for event in result['attempts']),1)

 def test_publisher_success_finishes_without_checkpoint_or_archive_queue(self):
  got={**self.missing,'status':'retrieved','http_status':200,'raw_uri':'gs://test/raw/full','final_url':self.item['url']}
  with patch.object(Fetcher,'fetch',return_value=got),patch.object(self.pipeline,'read',return_value=b'article'), \
       patch('pipeline.extract',return_value={'quality':'candidate','text':'Complete text'}),patch.object(self.pipeline,'one') as lookup:
   result=self.pipeline.fetch_phase(self.item,'run','KE')
  self.assertEqual(result['status'],'saved');self.assertNotIn('_checkpoint',result);lookup.assert_not_called()

 def test_full_retry_pass_is_scheduled_without_sleep_and_counts_each_pass_once(self):
  current=self.publisher_missing()
  for pass_index in range(2):
   archive,checkpoint=self.resume(current,self.worker('archive-'+str(pass_index)))
   with patch.object(archive,'one',side_effect=TimeoutError('archive unavailable')) as lookup, \
        patch('pipeline.time.sleep') as sleep,patch('retrying.time.time',return_value=1000):
    recovered=archive.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
   self.assertEqual(lookup.call_count,4,'Both historical/latest lookups retain both attempts')
   self.assertEqual([call.args[0] for call in sleep.call_args_list],[5,5])
   if pass_index==0:
    self.assertEqual((recovered['status'],recovered['next_phase'],recovered['retry_at']),('queued','publisher',1030))
    publisher=self.worker('publisher-second')
    with patch.object(Fetcher,'fetch',return_value=copy.deepcopy(self.missing)) as fetch, \
         patch('retrying.time.sleep') as sleep,patch.object(publisher,'one') as lookup:
     current=publisher.fetch_phase(self.item,'run','KE',phase='publisher',checkpoint=recovered['_checkpoint'])
    fetch.assert_called_once();sleep.assert_not_called();lookup.assert_not_called()
    self.assertEqual(current['_checkpoint']['completed_passes'],1)
   else:
    self.assertEqual(recovered['status'],'failed');self.assertNotIn('_checkpoint',recovered)
    self.assertEqual((recovered['response_bytes'],recovered['stored_bytes']),(20,8))
    self.assertEqual(sum(event['stage']=='http' for event in recovered['attempts']),2)
    self.assertEqual(sum(event['stage']=='archive_lookup' for event in recovered['attempts']),8)

 def test_partial_candidate_is_not_lost_when_a_later_retry_cannot_retrieve_publisher(self):
  text='The partial text from the first pass'
  got={**self.missing,'status':'retrieved','http_status':200,'raw_uri':'gs://test/raw/partial','final_url':self.item['url']}
  with patch.object(Fetcher,'fetch',return_value=got),patch.object(self.pipeline,'read',return_value=b'partial'), \
       patch('pipeline.extract',return_value={'quality':'partial','text':text}):
   current=self.pipeline.fetch_phase(self.item,'run','KE')
  browser,checkpoint=self.resume(current,self.worker('browser-first'))
  with patch.object(browser,'render',side_effect=RuntimeError('temporary browser outage')):
   current=browser.fetch_phase(self.item,'run','KE',phase='browser',checkpoint=checkpoint)
  archive,checkpoint=self.resume(current)
  with patch.object(archive,'one',return_value=self.no_snapshot):
   retry=archive.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
  self.assertEqual(retry['next_phase'],'publisher')
  publisher=self.worker('publisher-retry')
  with patch.object(Fetcher,'fetch',return_value=copy.deepcopy(self.missing)):
   current=publisher.fetch_phase(self.item,'run','KE',checkpoint=retry['_checkpoint'])
  archive,checkpoint=self.resume(current,self.worker('archive-retry'))
  with patch.object(archive,'one',return_value=self.no_snapshot):
   result=archive.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
  self.assertEqual(result['status'],'partial')
  self.assertEqual(gzip.decompress(self.bucket.data[result['text_uri'].removeprefix('gs://test/')]).decode(),text)
  self.assertEqual(result['raw_uri'],'gs://test/raw/partial')

 def test_waiting_in_durable_queue_does_not_consume_article_work_budget(self):
  clock=Clock()
  def fetch(*_):
   with self.pipeline.queued_wait():clock.advance(400)
   clock.advance(2)
   return copy.deepcopy(self.missing)
  with patch('pipeline.time.monotonic',side_effect=clock.now),patch.object(Fetcher,'fetch',side_effect=fetch):
   queued=self.pipeline.fetch_phase(self.item,'run','KE')
  self.assertAlmostEqual(queued['_checkpoint']['pipeline']['elapsed_seconds'],2)
  clock.advance(7*24*3600);archive,checkpoint=self.resume(queued)
  with patch('pipeline.time.monotonic',side_effect=clock.now),patch.object(archive,'one',return_value=self.no_snapshot) as lookup:
   result=archive.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
  self.assertEqual(result['status'],'exhausted');self.assertEqual(lookup.call_count,2)

 def test_actual_publisher_work_budget_is_preserved_across_handoff(self):
  clock=Clock()
  def fetch(*_):clock.advance(310);return copy.deepcopy(self.missing)
  with patch('pipeline.time.monotonic',side_effect=clock.now),patch.object(Fetcher,'fetch',side_effect=fetch):
   queued=self.pipeline.fetch_phase(self.item,'run','KE')
  archive,checkpoint=self.resume(queued)
  with patch.object(archive,'one') as lookup:
   result=archive.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
  lookup.assert_not_called();self.assertEqual((result['status'],result['next_phase']),('queued','publisher'))

 def test_checkpoint_cannot_be_reused_for_another_article_or_phase(self):
  queued=self.publisher_missing();archive,checkpoint=self.resume(queued)
  for item,phase in [(dict(self.item,url='https://publisher.example/other'),'archive'),(self.item,'publisher')]:
   with self.subTest(item=item,phase=phase),patch.object(Fetcher,'fetch') as fetch:
    with self.assertRaises(ValueError):archive.fetch_phase(item,'run','KE',phase=phase,checkpoint=checkpoint)
   fetch.assert_not_called()

 def test_browser_resumes_on_fresh_worker_without_repeating_http_or_canonical_discovery(self):
  got={**self.missing,'status':'retrieved','http_status':200,'raw_uri':'gs://test/raw/publisher','final_url':self.item['url']}
  canonical='https://publisher.example/canonical-story'
  with patch.object(Fetcher,'fetch',return_value=got) as fetch,patch.object(self.pipeline,'read',return_value=b'publisher'), \
       patch('pipeline.extract',side_effect=lambda *_:{'quality':'partial','text':'Preview','discovered':[canonical]}), \
       patch.object(self.pipeline,'render') as render:
   queued=self.pipeline.fetch_phase(self.item,'run','KE')
  self.assertEqual(fetch.call_count,2);render.assert_not_called();self.assertEqual(queued['next_phase'],'browser')
  browser,checkpoint=self.resume(queued,self.worker('browser-complete'))
  self.assertEqual(checkpoint['pipeline']['first']['final_url'],self.item['url'])
  text='The complete article rendered in the browser';body=b'<article>complete browser body</article>'
  with patch.object(Fetcher,'fetch') as fetch,patch.object(browser,'render',return_value=(body,self.item['url'])) as render, \
       patch('pipeline.extract',return_value={'quality':'candidate','text':text}),patch.object(browser,'one') as archive:
   result=browser.fetch_phase(self.item,'run','KE',phase='browser',checkpoint=checkpoint)
  fetch.assert_not_called();archive.assert_not_called();render.assert_called_once_with(self.item['url'])
  self.assertEqual(result['status'],'saved');self.assertNotIn('_checkpoint',result)
  self.assertIn('/browser-complete/rendered/',result['raw_uri']);self.assertIn('/browser-complete/extracted/',result['text_uri'])
  self.assertEqual(result['response_bytes'],20+len(body))
  self.assertEqual(result['stored_bytes'],8+len(gzip.compress(body))+len(gzip.compress(text.encode(),mtime=0)))
  self.assertEqual(sum(event['stage']=='http' for event in result['attempts']),1)
  self.assertEqual(sum(event['stage']=='publisher_url_discovery' for event in result['attempts']),1)
  self.assertEqual(sum(event['stage']=='browser' for event in result['attempts']),1)

 def test_ineligible_browser_goes_directly_to_archive_with_reason(self):
  for status,http_status,paywall in [('unavailable',404,False),('blocked',403,False),('retrieved',200,True)]:
   worker=self.worker(status)
   got={**self.missing,'status':status,'http_status':http_status,
        'raw_uri':'gs://test/raw/paywall' if paywall else None,'final_url':self.item['url']}
   with self.subTest(status=status),patch.object(Fetcher,'fetch',return_value=got), \
        patch.object(worker,'read',return_value=b'preview'), \
        patch('pipeline.extract',return_value={'quality':'partial','text':'Preview','paywall':paywall}), \
        patch.object(worker,'render') as browser,patch.object(worker,'one') as archive:
    result=worker.fetch_phase(self.item,'run','KE')
   browser.assert_not_called();archive.assert_not_called();self.assertEqual(result['next_phase'],'archive')
   event=next(event for event in result['attempts'] if event['stage']=='browser')
   self.assertEqual(event['status'],'not_applicable');self.assertTrue(event['reason'])

 def test_confirmed_homepage_redirect_skips_browser_and_retains_partial_for_archive(self):
  for phase in ('publisher','browser'):
   with self.subTest(phase=phase):
    worker=self.worker('homepage-'+phase);text='Previously recovered partial text'
    got={**self.missing,'status':'needs_inspection','http_status':200,
         'raw_uri':'gs://test/raw/partial','final_url':'https://publisher.example/',
         'error':'Redirected to homepage'}
    checkpoint=None
    if phase=='browser':
     # Reproduce a persisted browser checkpoint from the older implementation,
     # which queued any HTTP 200 response even after a confirmed root redirect.
     legacy={**got,'error':None}
     with patch.object(Fetcher,'fetch',return_value=legacy),patch.object(worker,'read',return_value=b'partial'), \
          patch('pipeline.extract',return_value={'quality':'partial','text':text}):
      queued=worker.fetch_phase(self.item,'run','KE')
     self.assertEqual(queued['next_phase'],'browser')
     checkpoint=json.loads(json.dumps(queued['_checkpoint']))
     checkpoint['pipeline']['first']['error']=got['error']
     checkpoint['pipeline']['result']['attempts'][0]['error']=got['error']
     worker=self.worker('legacy-browser-homepage')
    with patch.object(Fetcher,'fetch',return_value=got) as fetch,patch.object(worker,'read',return_value=b'partial'), \
         patch('pipeline.extract',return_value={'quality':'partial','text':text}), \
         patch.object(worker,'render') as render,patch.object(worker,'one') as lookup:
     queued=worker.fetch_phase(self.item,'run','KE',phase=phase,checkpoint=checkpoint)
    render.assert_not_called();lookup.assert_not_called()
    self.assertEqual(fetch.call_count,int(phase=='publisher'))
    self.assertEqual((queued['status'],queued['next_phase']),('queued','archive'))
    self.assertEqual(queued['_checkpoint']['completed_passes'],0)
    self.assertEqual(queued['_checkpoint']['pipeline']['best']['text'],text)
    event=next(event for event in queued['attempts'] if event['stage']=='browser')
    self.assertEqual(event['status'],'not_applicable');self.assertIn('bare homepage',event['reason'])
    archive,checkpoint=self.resume(queued,self.worker('homepage-archive-'+phase))
    with patch.object(Fetcher,'fetch') as fetch,patch.object(archive,'one',return_value=self.no_snapshot) as lookup:
     result=archive.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
    fetch.assert_not_called();self.assertEqual(lookup.call_count,2)
    self.assertEqual(result['status'],'partial');self.assertEqual(result['response_bytes'],10)
    self.assertEqual(gzip.decompress(self.bucket.data[result['text_uri'].removeprefix('gs://test/')]).decode(),text)

 def test_query_fragment_and_unconfirmed_root_urls_remain_browser_eligible(self):
  for final,error in [('https://publisher.example/?articleID=42','Redirected to homepage'),
                      ('https://publisher.example/#/story','Redirected to homepage'),
                      ('https://publisher.example/news/','Redirected to homepage'),
                      ('https://publisher.example/',None)]:
   with self.subTest(final=final,error=error):
    worker=self.worker('eligible-browser')
    got={**self.missing,'status':'needs_inspection','http_status':200,
         'raw_uri':'gs://test/raw/page','final_url':final,'error':error}
    with patch.object(Fetcher,'fetch',return_value=got),patch.object(worker,'read',return_value=b'page'), \
         patch('pipeline.extract',return_value={'quality':'missing','text':'','reason':'Listing page, not a single article'}):
     queued=worker.fetch_phase(self.item,'run','KE')
    self.assertEqual(queued['next_phase'],'browser')
    browser,checkpoint=self.resume(queued,self.worker('eligible-browser-resume'))
    with patch.object(browser,'render',return_value=(b'full',final)) as render, \
         patch('pipeline.extract',return_value={'quality':'candidate','text':'Complete article'}):
     result=browser.fetch_phase(self.item,'run','KE',phase='browser',checkpoint=checkpoint)
    render.assert_called_once_with(final);self.assertEqual(result['status'],'saved')

 def test_literal_r5_archive_checkpoint_without_browser_fields_resumes_without_replay(self):
  # Shape emitted by r5 before browser became a separate queue phase.
  text='Previously extracted partial text'
  checkpoint={'version':1,'kind':'phased-toolbox','article_id':self.aid,'run_id':'run','country':'KE',
   'next_phase':'archive','completed_passes':0,'attempts':[],'response_bytes':0,'stored_bytes':0,
   'retry_best':None,'pipeline':{'version':1,'next_phase':'archive','elapsed_seconds':10,'unresolved':False,
    'best':{'quality':'partial','text':text,'url':self.item['url'],'raw_uri':'gs://test/runs/run/claims/old/rendered.html.gz',
            'digest':'old-content-digest','http_status':200},
    'result':{**self.item,'article_id':self.aid,'run_id':'run','country':'KE','status':'deferred','response_bytes':30,
              'stored_bytes':12,'attempts':[{'stage':'http','status':'retrieved'},{'stage':'browser','status':'rendered'}]}}}
  worker=self.worker('new-archive-claim')
  with patch.object(Fetcher,'fetch') as fetch,patch.object(worker,'render') as browser, \
       patch.object(worker,'one',return_value=self.no_snapshot):
   result=worker.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=json.loads(json.dumps(checkpoint)))
  fetch.assert_not_called();browser.assert_not_called();self.assertEqual(result['status'],'partial')
  self.assertEqual(result['response_bytes'],30);self.assertEqual(sum(event['stage']=='browser' for event in result['attempts']),1)
  self.assertEqual(gzip.decompress(self.bucket.data[result['text_uri'].removeprefix('gs://test/')]).decode(),text)
  self.assertEqual(result['raw_uri'],checkpoint['pipeline']['best']['raw_uri'])

 def test_retries_preserve_publisher_browser_archive_order_and_do_not_duplicate_counters(self):
  current=None;phases=[]
  got={**self.missing,'status':'retrieved','http_status':200,'raw_uri':'gs://test/raw/partial','final_url':self.item['url']}
  for pass_index in range(2):
   publisher=self.worker('publisher-'+str(pass_index));phases.append('publisher')
   with patch.object(Fetcher,'fetch',return_value=got),patch.object(publisher,'read',return_value=b'partial'), \
        patch('pipeline.extract',return_value={'quality':'partial','text':'Earlier partial'}),patch.object(publisher,'render') as render:
    current=publisher.fetch_phase(self.item,'run','KE',checkpoint=current['_checkpoint'] if current else None)
   render.assert_not_called();self.assertEqual(current['next_phase'],'browser')
   browser,checkpoint=self.resume(current,self.worker('browser-'+str(pass_index)));phases.append('browser')
   with patch.object(Fetcher,'fetch') as fetch,patch.object(browser,'render',side_effect=RuntimeError('temporary render failure')):
    current=browser.fetch_phase(self.item,'run','KE',phase='browser',checkpoint=checkpoint)
   fetch.assert_not_called();self.assertEqual(current['next_phase'],'archive')
   archive,checkpoint=self.resume(current,self.worker('archive-'+str(pass_index)));phases.append('archive')
   with patch.object(archive,'one',return_value=self.no_snapshot),patch('retrying.time.sleep') as sleep:
    current=archive.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
   sleep.assert_not_called()
   if pass_index==0:self.assertEqual((current['status'],current['next_phase']),('queued','publisher'))
  self.assertEqual(phases,['publisher','browser','archive']*2);self.assertEqual(current['status'],'failed')
  self.assertEqual(current['response_bytes'],20)
  self.assertEqual(sum(event['stage']=='http' for event in current['attempts']),2)
  self.assertEqual(sum(event['stage']=='browser' for event in current['attempts']),2)
  self.assertEqual(sum(event['stage']=='archive_lookup' for event in current['attempts']),4)

 def test_browser_queue_time_does_not_consume_render_work_budget(self):
  clock=Clock();got={**self.missing,'status':'retrieved','http_status':200,'raw_uri':'gs://test/raw/partial','final_url':self.item['url']}
  with patch('pipeline.time.monotonic',side_effect=clock.now),patch.object(Fetcher,'fetch',return_value=got), \
       patch.object(self.pipeline,'read',return_value=b'partial'),patch('pipeline.extract',return_value={'quality':'partial','text':'Preview'}):
   queued=self.pipeline.fetch_phase(self.item,'run','KE')
  clock.advance(7*24*3600);browser,checkpoint=self.resume(queued,self.worker('browser-later'))
  with patch('pipeline.time.monotonic',side_effect=clock.now),patch.object(browser,'render',return_value=(b'full',self.item['url'])) as render, \
       patch('pipeline.extract',return_value={'quality':'candidate','text':'Full article'}):
   result=browser.fetch_phase(self.item,'run','KE',phase='browser',checkpoint=checkpoint)
  render.assert_called_once();self.assertEqual(result['status'],'saved')


if __name__=='__main__':unittest.main()
