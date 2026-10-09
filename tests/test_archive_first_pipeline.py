"""Archive preflight never completes work whose publisher remains unattempted."""
import copy,gzip,hashlib,json,sys,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
from crawl import Fetcher,key
from retrying import RetryingPipeline
from extractor_version import VERSION as EXTRACTOR_VERSION
from test_retrying import Bucket

class ArchiveFirstTests(unittest.TestCase):
 def setUp(self):
  self.bucket=Bucket();self.item={'url':'https://publisher.example/story','outlet':'publisher.example','first_observed':'2020-01-02'}
  self.aid=key(self.item['url']);self.worker=self.pipeline('publisher')
  self.snapshot='https://web.archive.org/web/20200102/https://publisher.example/story'
  self.payload=(200,{},json.dumps({'archived_snapshots':{'closest':{'available':True,'url':self.snapshot,'timestamp':'20200102'}}}).encode(),False)
  self.no_snapshot=(200,{},b'{"archived_snapshots":{}}',False)
  self.missing={'status':'unavailable','http_status':404,'raw_uri':None,'attempts':[{'http_status':404}],'response_bytes':10,'stored_bytes':4}
  self.body=b'<article>Immutable original archive body</article>'
  self.got={'status':'retrieved','http_status':200,'raw_uri':'gs://test/raw/archive','final_url':self.snapshot,
            'attempts':[{'http_status':200}],'response_bytes':32,'stored_bytes':8}
 def pipeline(self,token):
  worker=RetryingPipeline(self.bucket,'run',max_attempts=1);worker.request_context.output_scope=self.aid+'/'+token;return worker
 def fresh(self,checkpoint=None,deadline=None):
  return self.worker.archive_first_handoff(self.item,'run','KE',checkpoint=checkpoint,publisher_retry_at=deadline)
 def resume(self,queued,phase,**patches):
  worker=self.pipeline(phase);checkpoint=json.loads(json.dumps(queued['_checkpoint']))
  return worker,checkpoint
 def run_archive(self,queued,quality=None,text=None):
  worker,checkpoint=self.resume(queued,'archive')
  with patch.object(worker,'one',return_value=self.payload if quality else self.no_snapshot), \
       patch.object(Fetcher,'fetch',return_value=copy.deepcopy(self.got)) as fetch,patch.object(worker,'read',return_value=self.body), \
       patch('pipeline.extract',side_effect=lambda *_:{'quality':quality,'text':text or ''}):
   result=worker.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
  return result,fetch
 def test_handoff_is_metadata_only_and_fenced(self):
  with patch.object(Fetcher,'fetch') as fetch,patch.object(self.worker,'one') as one,patch.object(self.worker,'put') as put:
   queued=self.fresh(deadline=100000000000.)
  fetch.assert_not_called();one.assert_not_called();put.assert_not_called()
  state=queued['_checkpoint'];inner=state['pipeline']
  self.assertEqual((queued['status'],queued['next_phase'],state['completed_passes']),('queued','archive',0))
  self.assertTrue(inner['publisher_unresolved']);self.assertTrue(inner['unresolved'])
  self.assertEqual((queued['response_bytes'],queued['stored_bytes'],queued['attempts']),(0,0,[]))
  self.worker.abort_event.set()
  with self.assertRaisesRegex(RuntimeError,'ownership lost'):self.fresh()
 def test_archive_miss_returns_unattempted_publisher_not_terminal_and_not_rerouted(self):
  queued=self.fresh();result,fetch=self.run_archive(queued)
  fetch.assert_not_called();inner=result['_checkpoint']['pipeline']
  self.assertEqual((result['status'],result['next_phase'],result['_checkpoint']['completed_passes']),('queued','publisher',0))
  self.assertTrue(inner['publisher_unresolved']);self.assertTrue(inner['archive_first_done']);self.assertTrue(inner['archive_first_complete'])
  self.assertIsNone(self.fresh(checkpoint=result['_checkpoint']))
  publisher,checkpoint=self.resume(result,'publisher')
  with patch.object(Fetcher,'fetch',return_value=copy.deepcopy(self.missing)) as fetch,patch.object(publisher,'one') as archive:
   final=publisher.fetch_phase(self.item,'run','KE',checkpoint=checkpoint)
  fetch.assert_called_once();archive.assert_not_called();self.assertEqual(final['status'],'exhausted')
  self.assertEqual((final['response_bytes'],final['stored_bytes']),(10,4))
  self.assertEqual(sum(e['stage']=='archive_lookup' for e in final['attempts']),2)
 def test_verified_archive_text_finishes_without_publisher(self):
  result,fetch=self.run_archive(self.fresh(),quality='candidate',text='A verified full article body.')
  self.assertEqual(result['status'],'saved');self.assertNotIn('_checkpoint',result)
  self.assertEqual([call.args[0]['url'] for call in fetch.call_args_list],[self.snapshot])
  self.assertFalse(any(e['stage']=='http' for e in result['attempts']))
  self.assertEqual(result['content_sha256'],hashlib.sha256(self.body).hexdigest())
  self.assertEqual(gzip.decompress(self.bucket.data[result['text_uri'].removeprefix('gs://test/')]).decode(),'A verified full article body.')
 def test_partial_archive_is_retained_until_publisher_attempt_and_not_repeated(self):
  partial='A preserved but incomplete archive article.'
  queued,fetch=self.run_archive(self.fresh(),quality='partial',text=partial)
  self.assertEqual(queued['status'],'queued');self.assertEqual(queued['next_phase'],'publisher')
  self.assertEqual(queued['_checkpoint']['pipeline']['best']['text'],partial)
  publisher,checkpoint=self.resume(queued,'publisher')
  with patch.object(Fetcher,'fetch',return_value=copy.deepcopy(self.missing)),patch.object(publisher,'one') as archive:
   final=publisher.fetch_phase(self.item,'run','KE',checkpoint=checkpoint)
  archive.assert_not_called();self.assertEqual(final['status'],'partial');self.assertEqual(final['raw_uri'],self.got['raw_uri'])
  self.assertEqual(final['response_bytes'],42)
  self.assertEqual(final['stored_bytes'],12+len(gzip.compress(partial.encode(),mtime=0)))
 def test_archive_service_wait_wakes_at_publisher_deadline_without_spending_a_pass(self):
  with patch('pipeline.time.time',return_value=1000):queued=self.fresh(deadline=1020)
  archive,checkpoint=self.resume(queued,'archive');wait={**self.got,'status':'robots_unavailable','http_status':None,'raw_uri':None,'response_bytes':0,'stored_bytes':0,'service_retry_at':1060,'error':'Archive service unavailable'}
  with patch('pipeline.time.time',return_value=1000),patch.object(archive,'one',return_value=self.payload) as lookup,patch.object(Fetcher,'fetch',return_value=wait):
   waiting=archive.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
  self.assertEqual((waiting['status'],waiting['next_phase'],waiting['retry_at']),('queued','archive',1020))
  self.assertEqual(waiting['_checkpoint']['completed_passes'],0);self.assertTrue(waiting['_checkpoint']['pipeline']['publisher_unresolved'])
  later,checkpoint=self.resume(waiting,'archive-after-deadline')
  with patch('pipeline.time.time',return_value=1020),patch.object(later,'one') as lookup,patch.object(Fetcher,'fetch') as fetch:
   publisher=later.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
  lookup.assert_not_called();fetch.assert_not_called()
  inner=publisher['_checkpoint']['pipeline'];self.assertEqual(publisher['next_phase'],'publisher')
  self.assertTrue(inner['archive_first_done']);self.assertFalse(inner['archive_first_complete']);self.assertTrue(inner['archive_lookups'])
  self.assertIsNone(self.fresh(checkpoint=publisher['_checkpoint']))
  # A genuine publisher failure leaves the unfinished archive recovery eligible.
  worker,checkpoint=self.resume(publisher,'publisher')
  with patch.object(Fetcher,'fetch',return_value=copy.deepcopy(self.missing)),patch.object(worker,'one') as lookup:
   recovery=worker.fetch_phase(self.item,'run','KE',checkpoint=checkpoint)
  lookup.assert_not_called();self.assertEqual(recovery['next_phase'],'archive')
  archive,checkpoint=self.resume(recovery,'archive-recovered')
  with patch.object(archive,'one') as lookup,patch.object(Fetcher,'fetch',return_value=self.got) as fetch, \
       patch.object(archive,'read',return_value=self.body),patch('pipeline.extract',return_value={'quality':'candidate','text':'Recovered full article'}):
   saved=archive.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
  lookup.assert_not_called();fetch.assert_called_once();self.assertEqual(saved['status'],'saved')
  self.assertEqual(saved['response_bytes'],42)
 def test_unknown_archive_outage_still_queues_publisher_without_consuming_pass(self):
  queued=self.fresh();archive,checkpoint=self.resume(queued,'archive')
  with patch.object(archive,'one',side_effect=TimeoutError('archive unavailable')),patch('pipeline.time.sleep'),patch.object(Fetcher,'fetch') as fetch:
   pending=archive.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=checkpoint)
  fetch.assert_not_called();self.assertEqual((pending['status'],pending['next_phase']),('queued','publisher'))
  self.assertEqual(pending['_checkpoint']['completed_passes'],0);self.assertFalse(pending['_checkpoint']['pipeline']['archive_first_complete'])
 def test_prior_pass_prefix_and_current_partial_are_counted_once_after_restart(self):
  previous={'stage':'http','status':'temporary_error','url':self.item['url']};current={'stage':'publisher-note','status':'retained'}
  partial={'quality':'partial','text':'A retained partial before preflight','url':self.item['url'],'raw_uri':'gs://test/raw/retained','digest':'retained','http_status':200}
  initial=self.fresh()['_checkpoint'];initial.update(next_phase='publisher',completed_passes=1,attempts=[previous],response_bytes=17,stored_bytes=7)
  initial['pipeline']={'version':1,'next_phase':'publisher','best':partial,'extractor_version':EXTRACTOR_VERSION,
    'result':{**self.item,'article_id':self.aid,'run_id':'run','country':'KE','status':'deferred','attempts':[current],
      'response_bytes':23,'stored_bytes':9,'raw_uri':None,'text_uri':None},'unresolved':False}
  before=copy.deepcopy(initial);queued=self.fresh(checkpoint=initial)
  self.assertEqual(initial,before);self.assertEqual((queued['response_bytes'],queued['stored_bytes']),(40,16))
  self.assertEqual(queued['_checkpoint']['completed_passes'],1)
  pending,fetch=self.run_archive(queued);fetch.assert_not_called()
  publisher,checkpoint=self.resume(pending,'publisher-after-prefix')
  self.assertEqual(checkpoint['pipeline']['best'],partial)
  with patch.object(Fetcher,'fetch',return_value=copy.deepcopy(self.missing)),patch.object(publisher,'one') as lookup:
   final=publisher.fetch_phase(self.item,'run','KE',checkpoint=checkpoint)
  lookup.assert_not_called();self.assertEqual(final['status'],'partial')
  self.assertEqual(final['response_bytes'],50);self.assertEqual(final['stored_bytes'],20+len(gzip.compress(partial['text'].encode(),mtime=0)))
  self.assertEqual(sum(e==previous for e in final['attempts']),1);self.assertEqual(sum(e==current for e in final['attempts']),1)
  self.assertEqual(sum(e['stage']=='archive_lookup' for e in final['attempts']),2)
 def test_temporary_publisher_retry_retains_archive_state_without_double_accounting(self):
  pending,_=self.run_archive(self.fresh(),quality='partial',text='Retained partial article')
  original_lookups=sum(e['stage']=='archive_lookup' for e in pending['attempts'])
  publisher,checkpoint=self.resume(pending,'publisher-transient');temporary={**self.missing,'status':'temporary_error','http_status':None}
  with patch.object(Fetcher,'fetch',return_value=temporary),patch.object(publisher,'one') as lookup:
   retry=publisher.fetch_phase(self.item,'run','KE',checkpoint=checkpoint)
  lookup.assert_not_called();self.assertEqual(retry['next_phase'],'publisher');self.assertEqual(retry['_checkpoint']['completed_passes'],1)
  self.assertTrue(retry['_checkpoint']['archive_first_done']);self.assertTrue(retry['_checkpoint']['pipeline']['archive_lookups'])
  publisher,checkpoint=self.resume(retry,'publisher-retry')
  with patch.object(Fetcher,'fetch',return_value=copy.deepcopy(self.missing)),patch.object(publisher,'one') as lookup:
   final=publisher.fetch_phase(self.item,'run','KE',checkpoint=checkpoint)
  lookup.assert_not_called();self.assertEqual(final['status'],'partial');self.assertEqual(final['response_bytes'],52)
  self.assertEqual(sum(e['stage']=='http' for e in final['attempts']),2)
  self.assertEqual(sum(e['stage']=='archive_lookup' for e in final['attempts']),original_lookups)
 def test_simple_legacy_marker_survives_outer_retry_and_skips_completed_archive(self):
  state=self.fresh()['_checkpoint'];state.update(next_phase='publisher',completed_passes=1,pipeline=None,
   archive_first_done=True,archive_first_complete=True,response_bytes=17,stored_bytes=7,
   attempts=[{'stage':'archive_lookup','status':'checked','lookup_uri':'gs://test/original-evidence'}])
  with patch.object(Fetcher,'fetch',return_value=copy.deepcopy(self.missing)),patch.object(self.worker,'one') as lookup:
   final=self.worker.fetch_phase(self.item,'run','KE',checkpoint=state)
  lookup.assert_not_called();self.assertEqual(final['status'],'exhausted')
  self.assertEqual((final['response_bytes'],final['stored_bytes']),(27,11))
  self.assertEqual(sum(e['stage']=='archive_lookup' for e in final['attempts']),1)
 def test_existing_verified_candidate_finishes_even_after_publisher_deadline(self):
  state=self.fresh(deadline=1000)['_checkpoint'];text='Previously verified complete archived text'
  state['pipeline']['best']={'quality':'candidate','text':text,'url':self.snapshot,'raw_uri':self.got['raw_uri'],
   'digest':hashlib.sha256(self.body).hexdigest(),'http_status':200}
  state['pipeline']['service_wait_until']=2000
  with patch('pipeline.time.time',return_value=1050),patch.object(Fetcher,'fetch') as fetch,patch.object(self.worker,'one') as lookup:
   final=self.worker.fetch_phase(self.item,'run','KE',phase='archive',checkpoint=state)
  fetch.assert_not_called();lookup.assert_not_called();self.assertEqual(final['status'],'saved')
  self.assertEqual(final['raw_uri'],self.got['raw_uri'])
 def test_foreign_or_already_completed_checkpoint_is_not_eligible(self):
  state=self.fresh()['_checkpoint'];self.assertIsNone(self.fresh(checkpoint=state))
  state['next_phase']='publisher';state['completed_passes']=2;self.assertIsNone(self.fresh(checkpoint=state))
  state['completed_passes']=0;state['country']='US'
  with self.assertRaisesRegex(ValueError,'different article or run'):self.fresh(checkpoint=state)

if __name__=='__main__':unittest.main()
