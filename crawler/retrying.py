"""Retry incomplete toolbox passes before returning a terminal URL result."""
import json,time
from crawl import key,now
from pipeline import Pipeline

class RetryingPipeline(Pipeline):
 retry_passes=2
 retry_delay=30
 def fetch(self,item,run,country):
  aid=key(item['url']);path=f'runs/{run}/retry-state/{aid}.json';blob=self.bucket.blob(path)
  state=json.loads(blob.download_as_text()) if blob.exists() else {'passes':0,'history':[],'response_bytes':0,'stored_bytes':0}
  result=state.get('result')
  while state['passes']<self.retry_passes:
   if state['passes']:
    with self.lock:self.live[aid]={'outlet':item['outlet'],'stage':'Retrying automatically'}
    time.sleep(self.retry_delay)
    # Transient robots retrieval failures must not be cached across retry passes.
    with self.lock:self.robots={k:v for k,v in self.robots.items() if v[1]}
   result=super().fetch(item,run,country)
   state['passes']+=1;state['history'].extend(result['attempts']);state['response_bytes']+=result.get('response_bytes',0);state['stored_bytes']+=result.get('stored_bytes',0)
   state['result']=result
   self.put(path,json.dumps(state),'application/json')
   if result['status']!='deferred':break
  result={**result,'attempts':state['history'],'response_bytes':state['response_bytes'],'stored_bytes':state['stored_bytes'],'updated_at':now()}
  if result['status']=='deferred':
   result['status']='failed';result['error']=f'Automatic retry limit reached ({self.retry_passes} toolbox passes); see stage errors'
   result['attempts'].append({'stage':'final','status':'failed','reason':result['error'],'finished_at':now()})
  self.put(f'runs/{run}/toolbox/{aid}.json',json.dumps(result),'application/json')
  with self.lock:self.live.pop(aid,None)
  return result
