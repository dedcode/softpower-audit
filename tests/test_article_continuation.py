import gzip,hashlib,json,sys,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
from extract import extract,continuation_matches
from pipeline import Pipeline
from crawl import Fetcher,key
from retrying import RetryingPipeline
from test_pipeline import Bucket
TITLE='Mobile money access lifted Kenyan households out of poverty'
LEAD='Researchers studied how mobile money services helped Kenyan households save money and manage unexpected financial shocks.'
def teaser(target='https://origin.example/story'):
 return f'<html><title>{TITLE}</title><article><h1>{TITLE}</h1><p>{LEAD}</p><p>...</p><p><a href="{target}">Read more</a></p></article></html>'.encode()
def full(title=TITLE):
 return f'<html><h1>{title}</h1><article><p>{LEAD}</p><p>{"Further research describes the economic and social outcomes in Kenya. "*20}</p></article></html>'.encode()
class ContinuationTests(unittest.TestCase):
 def test_explicit_article_link_and_archived_original(self):
  a=extract(teaser('https://web.archive.org/web/20161210133052/http://phys.org/story'),'https://web.archive.org/web/20161210/http://www.kenyastar.com/story')
  self.assertEqual(a['quality'],'partial')
  self.assertEqual(a['continuations'],['https://web.archive.org/web/20161210133052/http://phys.org/story','https://phys.org/story'])
 def test_no_navigation_listing_or_paywall_continuations(self):
  for body in (b'<nav><a href="https://other.example/story">Read more</a></nav>',teaser()+teaser(),teaser().replace(b'<p>...</p>',b'<p>Subscribe to read this article</p>')):
   self.assertEqual(extract(body,'https://publisher.example/story')['continuations'],[])
 def test_story_identity_requires_headline_or_opening_match(self):
  self.assertEqual(continuation_matches(TITLE,LEAD,TITLE+' - Phys.org',LEAD),'headline')
  self.assertEqual(continuation_matches(TITLE,LEAD,'Different headline about this research',LEAD),'opening_text')
  self.assertIsNone(continuation_matches(TITLE,LEAD,'A completely different article about football','Unrelated content'))
 def test_standard_body_wins_over_long_video_and_replay_navigation(self):
  body=f'<html><h1>{TITLE}</h1><div class="main-article"><p>{LEAD}</p><p>{"Actual article paragraph. "*25}</p></div><table><td><p>WATCH THIS</p><p>{"Unrelated video listings. "*90}</p></td></table></html>'.encode()
  a=extract(body,'https://web.archive.org/web/20170118170305/http://www.standardmedia.co.ke/article/2000230244/story')
  self.assertEqual(a['quality'],'candidate');self.assertEqual(a['method'],'article selector: main-article');self.assertNotIn('Unrelated video',a['text'])
 def test_short_standard_brief_requires_three_real_body_paragraphs(self):
  paragraphs=['The local research team presented its latest findings at a public meeting held in the county yesterday.',
              'Several residents discussed the findings and asked how the proposed changes would affect their daily work.',
              'The officials said another public meeting would take place next month to review the remaining questions.']
  body=('<h1>'+TITLE+'</h1><div class="main-article">'+''.join('<p>'+p+'</p>' for p in paragraphs)+'</div><p>WATCH THIS</p><p>'+'Unrelated video headline. '*80+'</p>').encode()
  a=extract(body,'https://www.standardmedia.co.ke/article/123/story')
  self.assertEqual(a['quality'],'candidate');self.assertLess(len(a['text']),400);self.assertNotIn('Unrelated video',a['text'])
  a=extract(body.replace(('<p>'+paragraphs[1]+'</p>').encode(),b'').replace(('<p>'+paragraphs[2]+'</p>').encode(),b''),'https://www.standardmedia.co.ke/article/123/story')
  self.assertNotEqual(a['quality'],'candidate')
 def checkpoint(self,phase):
  url='https://www.kenyastar.com/story';aid=key(url);preview=extract(teaser(),url);text=preview.pop('text')
  best={**preview,'text':text,'url':url,'raw_uri':'gs://test/preview','digest':'preview','http_status':200}
  # Reproduce the old persisted partial: new discovery must survive an equal
  # quality/length reanalysis instead of silently retaining the old metadata.
  best.pop('continuations',None)
  result={'url':url,'outlet':'kenyastar.com','article_id':aid,'run_id':'run','country':'KE','status':'deferred','attempts':[{'stage':'archive','status':'retrieved','raw_uri':'gs://test/preview','url':url}],'response_bytes':11,'stored_bytes':7}
  return {'url':url,'outlet':'kenyastar.com'},{'version':1,'next_phase':phase,'result':result,'best':best,'unresolved':False,'elapsed_seconds':0,'extractor_version':'old','archive_first_complete':True}
 def test_restart_recovers_before_original_publisher_and_preserves_provenance(self):
  item,checkpoint=self.checkpoint('publisher');f=Pipeline(Bucket(),'run',max_attempts=1)
  got={'status':'retrieved','http_status':200,'raw_uri':'gs://test/original','final_url':'https://origin.example/story','attempts':[],'response_bytes':13,'stored_bytes':5}
  with patch.object(Fetcher,'fetch',return_value=got) as fetch,patch.object(f,'read',side_effect=lambda uri:teaser() if uri.endswith('preview') else full()),patch('pipeline.extract',side_effect=extract):
   r=f.fetch_phase(item,'run','KE',phase='publisher',checkpoint=json.loads(json.dumps(checkpoint)))
  self.assertEqual(fetch.call_count,1);self.assertEqual(fetch.call_args.args[0]['url'],'https://origin.example/story')
  self.assertEqual(r['status'],'saved');self.assertEqual(r['text_origin'],'linked_original');self.assertEqual(r['publication_raw_uri'],'gs://test/preview')
  self.assertEqual(r['final_url'],'https://origin.example/story');self.assertEqual(r['url'],item['url']);self.assertEqual(r['content_sha256'],hashlib.sha256(full()).hexdigest());self.assertEqual(r['response_bytes'],24)
  self.assertEqual(sum(e['stage']=='article_continuation' and e['status']=='verified' for e in r['attempts']),1)
 def test_unrelated_full_article_rejected_without_losing_teaser(self):
  item,c=self.checkpoint('archive');f=Pipeline(Bucket(),'run',max_attempts=1);unrelated=full('An unrelated football story').replace(LEAD.encode(),b'The football team had an unexpected result in a different country.')
  got={'status':'retrieved','http_status':200,'raw_uri':'gs://test/original','final_url':'https://origin.example/story','attempts':[]}
  with patch.object(Fetcher,'fetch',return_value=got) as fetch,patch.object(f,'read',side_effect=lambda uri:teaser() if uri.endswith('preview') else unrelated),patch('pipeline.extract',side_effect=extract):r=f.fetch_phase(item,'run','KE',phase='archive',checkpoint=c)
  self.assertEqual(fetch.call_count,1);self.assertEqual(r['status'],'partial');self.assertEqual(r['raw_uri'],'gs://test/preview');self.assertNotIn('text_origin',r)
 def test_old_completed_teaser_is_eligible_for_new_recovery_once(self):
  item,p=self.checkpoint('publisher');p.update(archive_first_done=True,publisher_unresolved=True)
  state={'version':1,'kind':'phased-toolbox','article_id':key(item['url']),'run_id':'run','country':'KE','next_phase':'publisher','completed_passes':0,'attempts':[],'response_bytes':0,'stored_bytes':0,'pipeline':p,'archive_first_done':True}
  f=RetryingPipeline(Bucket(),'run',max_attempts=1)
  q=f.archive_first_handoff(item,'run','KE',checkpoint=state,publisher_retry_at=1)
  self.assertEqual(q['next_phase'],'archive');self.assertEqual(q['_checkpoint']['pipeline']['extractor_version'],'old')
  from extractor_version import VERSION
  p['extractor_version']=VERSION
  self.assertIsNone(f.archive_first_handoff(item,'run','KE',checkpoint=state,publisher_retry_at=1))
 def test_stored_continuation_can_recover_after_publisher_deadline(self):
  item,c=self.checkpoint('archive');c.update(archive_first=True,publisher_unresolved=True,publisher_retry_at=1)
  f=Pipeline(Bucket(),'run',max_attempts=1);got={'status':'retrieved','http_status':200,'raw_uri':'gs://test/original','final_url':'https://origin.example/story','attempts':[]}
  with patch.object(Fetcher,'fetch',return_value=got),patch.object(f,'read',side_effect=lambda uri:teaser() if uri.endswith('preview') else full()),patch('pipeline.extract',side_effect=extract):r=f.fetch_phase(item,'run','KE',phase='archive',checkpoint=c)
  self.assertEqual(r['status'],'saved');self.assertEqual(r['text_origin'],'linked_original')
if __name__=='__main__':unittest.main()
