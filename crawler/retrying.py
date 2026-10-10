"""Retry incomplete toolbox passes before returning a terminal URL result."""
import copy,json,time,math
from crawl import key,now
from pipeline import Pipeline
from extractor_version import VERSION as EXTRACTOR_VERSION

def completed_publisher_before_archive_outage(attempts,url):
 """Conservatively recognize old retries of definitively unavailable pages."""
 if not isinstance(attempts,list) or any(not isinstance(event,dict) for event in attempts):return False
 http=[event for event in attempts if event.get('stage')=='http']
 if len(http)!=1 or http[0].get('url')!=url or http[0].get('status') not in ('unavailable','blocked'):return False
 replies=http[0].get('http_attempts',[])
 if not isinstance(replies,list) or not replies or not isinstance(replies[-1],dict) or replies[-1].get('http_status') not in (401,403,404,410):return False
 browser=[event for event in attempts if event.get('stage')=='browser']
 if not browser or any(event.get('status')!='not_applicable' for event in browser):return False
 for event in attempts:
  name=event.get('stage');status=event.get('status')
  if name=='publisher_url_discovery' and status not in ('no_candidate','not_needed','unavailable','blocked'):return False
  if name=='publisher_url_discovery' and status in ('unavailable','blocked'):
   replies=event.get('http_attempts',[])
   if not isinstance(replies,list) or not replies or not isinstance(replies[-1],dict) or replies[-1].get('http_status') not in (401,403,404,410):return False
  if name and name.startswith('extract:') and status=='deferred':return False
 return any((event.get('stage') in ('archive','archive_canonical') and event.get('status') in ('temporary_error','rate_limited','robots_unavailable'))
            or (event.get('stage')=='archive_lookup' and event.get('status')=='error') for event in attempts)

def migrate_legacy_archive_retry(state,item,run,country):
 """Pure, conservative migration shared by workers and a fenced operator."""
 aid=key(item['url'])
 if (not isinstance(state,dict) or state.get('version')!=1 or state.get('kind')!='phased-toolbox'
     or (state.get('article_id'),state.get('run_id'),state.get('country'))!=(aid,run,country)
     or state.get('next_phase')!='publisher' or state.get('completed_passes')!=1
     or any(type(state.get(field)) is not int or state[field]<0 for field in ('response_bytes','stored_bytes'))
     or state.get('pipeline') is not None or not isinstance(state.get('attempts'),list)
     or not completed_publisher_before_archive_outage(state['attempts'],item['url'])):return None
 migrated=copy.deepcopy(state)
 finals=[event for event in state['attempts'] if event.get('stage')=='final']
 old_result={**item,'article_id':aid,'run_id':run,'country':country,'status':'deferred',
             'updated_at':finals[-1].get('finished_at') if finals else None,
             'attempts':copy.deepcopy(state['attempts']),'response_bytes':state['response_bytes'],
             'stored_bytes':state['stored_bytes'],'raw_uri':None,'text_uri':None,'http_status':None,
             'error':None,'reused':False}
 pipeline={'version':1,'next_phase':'archive','result':old_result,
           'best':copy.deepcopy(state.get('retry_best')),'unresolved':False,'elapsed_seconds':0.,
           'extractor_version':finals[-1].get('extractor_version') if finals else None}
 migrated.update(next_phase='archive',pipeline=pipeline,attempts=[],response_bytes=0,stored_bytes=0)
 return migrated

class RetryingPipeline(Pipeline):
 retry_passes=2
 retry_delay=30
 def archive_first_handoff(self,item,run,country,*,checkpoint=None,publisher_retry_at=None):
  """Route explicitly requested unfinished publisher work to the archive pool."""
  self.check_running();aid=key(item['url'])
  state=copy.deepcopy(checkpoint) if checkpoint is not None else {
   'version':1,'kind':'phased-toolbox','article_id':aid,'run_id':run,'country':country,
   'next_phase':'publisher','completed_passes':0,'attempts':[],
   'response_bytes':0,'stored_bytes':0,'pipeline':None,'retry_best':None}
  if not isinstance(state,dict) or state.get('version')!=1 or state.get('kind')!='phased-toolbox':raise ValueError('Invalid phased extraction checkpoint')
  if (state.get('article_id'),state.get('run_id'),state.get('country'))!=(aid,run,country):raise ValueError('Checkpoint belongs to a different article or run')
  passes=state.get('completed_passes')
  if type(passes) is not int or passes<0:raise ValueError('Invalid checkpoint retry passes')
  pipeline=state.get('pipeline')
  if state.get('next_phase')!='publisher' or passes>=self.retry_passes:return None
  if state.get('archive_first_done') or (isinstance(pipeline,dict) and pipeline.get('archive_first_done')):
   # A temporary archive outage is unfinished work. Only a verified completed
   # preflight stays publisher-only; incomplete preflights may resume after
   # their own backoff even while the publisher remains in cooldown.
   stored_recovery=(isinstance(pipeline,dict) and pipeline.get('extractor_version')!=EXTRACTOR_VERSION
                    and isinstance(pipeline.get('best'),dict) and pipeline['best'].get('quality')=='partial'
                    and pipeline['best'].get('raw_uri'))
   if not stored_recovery:
    if not (isinstance(pipeline,dict) and pipeline.get('archive_first_complete') is False
            and pipeline.get('publisher_unresolved') is True):return None
    retry_at=float(pipeline.get('archive_first_retry_at',0.))
    if not math.isfinite(retry_at):raise ValueError('Invalid archive retry time')
    if retry_at>time.time():return None
  if pipeline is None:pipeline={'version':1,'next_phase':'publisher','best':state.get('retry_best')}
  pipeline=self.archive_first_checkpoint(item,run,country,checkpoint=pipeline,publisher_retry_at=publisher_retry_at)
  state.update(next_phase='archive',pipeline=pipeline)
  result=pipeline['result'];self.check_running()
  return {**result,'status':'queued','next_phase':'archive','retry_at':time.time(),
   'attempts':state['attempts']+result['attempts'],
   'response_bytes':state['response_bytes']+result.get('response_bytes',0),
   'stored_bytes':state['stored_bytes']+result.get('stored_bytes',0),'updated_at':now(),
   '_checkpoint':state,'error':None}
 def fetch_phase(self,item,run,country,*,phase='publisher',checkpoint=None):
  """Run one durable phase; waiting for a future retry never occupies a slot."""
  self.check_running()
  aid=key(item['url'])
  state=copy.deepcopy(checkpoint) if checkpoint is not None else {
   'version':1,'kind':'phased-toolbox','article_id':aid,'run_id':run,'country':country,
   'next_phase':'publisher','completed_passes':0,'attempts':[],
   'response_bytes':0,'stored_bytes':0,'pipeline':None,'retry_best':None}
  if not isinstance(state,dict) or state.get('version')!=1 or state.get('kind')!='phased-toolbox':raise ValueError('Invalid phased extraction checkpoint')
  if (state.get('article_id'),state.get('run_id'),state.get('country'))!=(aid,run,country):raise ValueError('Checkpoint belongs to a different article or run')
  if phase not in ('publisher','browser','archive') or state.get('next_phase')!=phase:raise ValueError('Checkpoint phase does not match requested work')
  passes=state['completed_passes']
  if not isinstance(passes,int) or not 0<=passes<self.retry_passes:raise ValueError('Checkpoint retry passes are already exhausted')
  pipeline_checkpoint=state['pipeline']
  if phase=='publisher' and passes:
   migrated=migrate_legacy_archive_retry(state,item,run,country)
   if migrated is not None:
    # Older images queued a second publisher pass for archive service outages.
    # Rebuild its recovery checkpoint instead of re-requesting a known 404/403.
    old_result=migrated['pipeline']['result']
    return {**old_result,'status':'queued','next_phase':'archive','retry_at':time.time(),
            '_checkpoint':migrated,'error':None}
   # The new pass refreshes transient publisher/robots failures, retaining any
   # earlier partial text even if this attempt cannot retrieve that page again.
   with self.lock:self.robots={k:v for k,v in self.robots.items() if v[1]}
   if not (isinstance(pipeline_checkpoint,dict) and pipeline_checkpoint.get('archive_first_done')):
    pipeline_checkpoint={'version':1,'next_phase':'publisher','best':state['retry_best'],
     'archive_first_done':bool(state.get('archive_first_done',False)),
     'archive_first_complete':bool(state.get('archive_first_complete',False))}
  result=super().fetch_phase(item,run,country,phase=phase,checkpoint=pipeline_checkpoint)
  self.check_running()
  pipeline_state=result.pop('_checkpoint',None)
  retry_best=result.pop('_retry_best',None)
  retry_phase=result.pop('_retry_phase',None);retry_checkpoint=result.pop('_retry_checkpoint',None)
  combined={**result,'attempts':state['attempts']+result['attempts'],
            'response_bytes':state['response_bytes']+result.get('response_bytes',0),
            'stored_bytes':state['stored_bytes']+result.get('stored_bytes',0),'updated_at':now()}
  if result['status']=='queued':
   state.update(next_phase=result['next_phase'],pipeline=pipeline_state)
   if isinstance(pipeline_state,dict) and pipeline_state.get('archive_first_done'):
    state['archive_first_done']=True;state['archive_first_complete']=bool(pipeline_state.get('archive_first_complete',False))
   combined['_checkpoint']=state
   return combined
  if result['status']=='deferred':
   passes+=1
   if passes<self.retry_passes:
    if retry_phase in ('archive','publisher') and isinstance(retry_checkpoint,dict):
     # The checkpoint already contains this pass's cumulative evidence. Keep
     # the earlier pass prefix separately, avoiding duplicated HTTP counters.
     state.update(next_phase=retry_phase,completed_passes=passes,pipeline=retry_checkpoint,retry_best=retry_best)
     if retry_checkpoint.get('archive_first_done'):
      state['archive_first_done']=True;state['archive_first_complete']=bool(retry_checkpoint.get('archive_first_complete',False))
    else:
     retry_phase='publisher'
     state.update(next_phase=retry_phase,completed_passes=passes,pipeline=None,retry_best=retry_best,
                  attempts=combined['attempts'],response_bytes=combined['response_bytes'],stored_bytes=combined['stored_bytes'])
    return {**combined,'status':'queued','next_phase':retry_phase,
            'retry_at':time.time()+self.retry_delay,'_checkpoint':state,
            'error':None}
   combined['status']='failed';combined['error']=f'Automatic retry limit reached ({self.retry_passes} toolbox passes); see stage errors'
   combined['attempts'].append({'stage':'final','status':'failed','reason':combined['error'],'finished_at':now()})
  self.check_running()
  self.put(f'runs/{run}/toolbox/{aid}.json',json.dumps(combined),'application/json')
  self.check_running()
  return combined
 def fetch(self,item,run,country):
  self.check_running()
  aid=key(item['url']);path=f'runs/{run}/retry-state/{aid}.json';blob=self.bucket.blob(self.scoped_path(path))
  state=json.loads(blob.download_as_text()) if blob.exists() else {'passes':0,'history':[],'response_bytes':0,'stored_bytes':0}
  self.check_running()
  result=state.get('result')
  while state['passes']<self.retry_passes:
   self.check_running()
   if state['passes']:
    with self.lock:self.live[aid]={'outlet':item['outlet'],'stage':'Retrying automatically'}
    time.sleep(self.retry_delay)
    # Transient robots retrieval failures must not be cached across retry passes.
    with self.lock:self.robots={k:v for k,v in self.robots.items() if v[1]}
   self.check_running()
   result=super().fetch(item,run,country)
   # Lower stages convert retrieval exceptions into deferred outcomes. Losing
   # the queue claim must escape that path without consuming a retry pass or
   # caching a terminal failure that a replacement worker would later reuse.
   self.check_running()
   state['passes']+=1;state['history'].extend(result['attempts']);state['response_bytes']+=result.get('response_bytes',0);state['stored_bytes']+=result.get('stored_bytes',0)
   state['result']=result
   self.check_running()
   self.put(path,json.dumps(state),'application/json')
   if result['status']!='deferred':break
  self.check_running()
  result={**result,'attempts':state['history'],'response_bytes':state['response_bytes'],'stored_bytes':state['stored_bytes'],'updated_at':now()}
  if result['status']=='deferred':
   result['status']='failed';result['error']=f'Automatic retry limit reached ({self.retry_passes} toolbox passes); see stage errors'
   result['attempts'].append({'stage':'final','status':'failed','reason':result['error'],'finished_at':now()})
  self.check_running()
  self.put(f'runs/{run}/toolbox/{aid}.json',json.dumps(result),'application/json')
  with self.lock:self.live.pop(aid,None)
  self.check_running()
  return result
