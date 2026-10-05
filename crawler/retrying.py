"""Retry incomplete toolbox passes before returning a terminal URL result."""
import copy,json,time
from crawl import key,now
from pipeline import Pipeline

class RetryingPipeline(Pipeline):
 retry_passes=2
 retry_delay=30
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
  if phase not in ('publisher','archive') or state.get('next_phase')!=phase:raise ValueError('Checkpoint phase does not match requested work')
  passes=state['completed_passes']
  if not isinstance(passes,int) or not 0<=passes<self.retry_passes:raise ValueError('Checkpoint retry passes are already exhausted')
  pipeline_checkpoint=state['pipeline']
  if phase=='publisher' and passes:
   # The new pass refreshes transient publisher/robots failures, retaining any
   # earlier partial text even if this attempt cannot retrieve that page again.
   with self.lock:self.robots={k:v for k,v in self.robots.items() if v[1]}
   pipeline_checkpoint={'version':1,'next_phase':'publisher','best':state['retry_best']}
  result=super().fetch_phase(item,run,country,phase=phase,checkpoint=pipeline_checkpoint)
  self.check_running()
  pipeline_state=result.pop('_checkpoint',None)
  retry_best=result.pop('_retry_best',None)
  combined={**result,'attempts':state['attempts']+result['attempts'],
            'response_bytes':state['response_bytes']+result.get('response_bytes',0),
            'stored_bytes':state['stored_bytes']+result.get('stored_bytes',0),'updated_at':now()}
  if result['status']=='queued':
   state.update(next_phase=result['next_phase'],pipeline=pipeline_state)
   combined['_checkpoint']=state
   return combined
  if result['status']=='deferred':
   passes+=1
   if passes<self.retry_passes:
    state.update(next_phase='publisher',completed_passes=passes,pipeline=None,retry_best=retry_best,
                 attempts=combined['attempts'],response_bytes=combined['response_bytes'],stored_bytes=combined['stored_bytes'])
    return {**combined,'status':'queued','next_phase':'publisher',
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
