"""Bounded automated recovery toolbox; every stage leaves an auditable outcome."""
import copy,gzip,json,time,threading,hashlib,math
from urllib.parse import urlsplit,urlencode
from crawl import Fetcher,key,now,public_url,UA
from isolation import extract_isolated as extract,render_isolated,IsolationError,queue_wait_seconds
VERSION="toolbox-2-isolated"

class Pipeline(Fetcher):
 parse_initial=False
 def __init__(self,*args,**kwargs):
  super().__init__(*args,**kwargs);self.live={};self.browser_lock=threading.Lock()
 def read(self,uri):return gzip.decompress(self.bucket.blob(uri.split('/'+self.bucket.name+'/',1)[1]).download_as_bytes(timeout=30))
 def fetch_phase(self,item,run,country,*,phase='publisher',checkpoint=None):
  return Pipeline.fetch(self,item,run,country,_phase=phase,_checkpoint=checkpoint)
 def fetch(self,item,run,country,*,_phase=None,_checkpoint=None):
  if _phase not in (None,'publisher','browser','archive'):raise ValueError('Unknown extraction phase')
  aid=key(item['url']);began=time.monotonic();queued_at_start=queue_wait_seconds();host_queued_at_start=self.host_queue_wait_seconds();events=[];best=None;unresolved=False
  previous_elapsed=0.
  result={**item,'article_id':aid,'run_id':run,'country':country,'updated_at':now(),'status':'deferred','attempts':events,'response_bytes':0,'stored_bytes':0,'raw_uri':None,'text_uri':None,'http_status':None,'error':None,'reused':False}
  if _phase in ('browser','archive'):
   checkpoint=copy.deepcopy(_checkpoint)
   if not isinstance(checkpoint,dict) or checkpoint.get('version')!=1 or checkpoint.get('next_phase')!=_phase:raise ValueError('Recovery phase requires its matching checkpoint')
   result=checkpoint['result']
   if (result.get('article_id'),result.get('run_id'),result.get('country'))!=(aid,run,country):raise ValueError('Checkpoint belongs to a different article or run')
   events=result['attempts'];best=checkpoint['best'];unresolved=checkpoint['unresolved'];previous_elapsed=float(checkpoint['elapsed_seconds'])
   if not math.isfinite(previous_elapsed) or previous_elapsed<0:raise ValueError('Invalid checkpoint work duration')
   result.update(status='deferred',updated_at=now())
   if _phase=='browser':
    first=checkpoint['first'];analysis=checkpoint.get('analysis')
    if not isinstance(first,dict):raise ValueError('Browser checkpoint requires publisher response metadata')
  elif _checkpoint is not None:
   if _phase!='publisher' or _checkpoint.get('version')!=1 or _checkpoint.get('next_phase')!='publisher':raise ValueError('Publisher phase cannot resume an archive checkpoint')
   best=copy.deepcopy(_checkpoint.get('best'))
  def elapsed():return previous_elapsed+time.monotonic()-began-(queue_wait_seconds()-queued_at_start)-(self.host_queue_wait_seconds()-host_queued_at_start)
  def handoff(next_phase,**phase_state):
   # Candidate text and immutable evidence survive process and claim changes.
   # Queueing browser/archive work never consumes a publisher download slot.
   self.check_running()
   checkpoint={'version':1,'next_phase':next_phase,'result':copy.deepcopy(result),
               'best':copy.deepcopy(best),'unresolved':unresolved,'elapsed_seconds':max(0.,elapsed()),
               **copy.deepcopy(phase_state)}
   return {**result,'status':'queued','next_phase':next_phase,'retry_at':time.time(),
           '_checkpoint':checkpoint,'error':None}
  def stage(name,outcome,**kw):
   events.append({'stage':name,'status':outcome,'finished_at':now(),**kw});result['updated_at']=now()
   self.put(f'runs/{run}/toolbox/{aid}.json',json.dumps(result),'application/json')
  def active(name):
   with self.lock:self.live[aid]={'outlet':item['outlet'],'stage':name}
  def consider(body,source,raw_uri,http_status=200,method='html'):
   nonlocal best,unresolved
   try:analysis=extract(body,source)
   except IsolationError as exc:
    unresolved=True;stage('extract:'+method,'deferred',reason=str(exc));return {}
   text=analysis.pop('text');stage('extract:'+method,analysis['quality'],url=source,**analysis)
   if http_status!=200:return analysis
   rank={'candidate':3,'partial':2,'missing':0}
   if text and (best is None or (rank[analysis['quality']],len(text))>(rank[best['quality']],len(best['text']))):best={**analysis,'text':text,'url':source,'raw_uri':raw_uri,'digest':hashlib.sha256(body).hexdigest(),'http_status':http_status}
   return analysis
  def retrieve(url,name):
   active(name)
   got=super(Pipeline,self).fetch({**item,'url':url},run,country)
   result['response_bytes']+=got.get('response_bytes',0);result['stored_bytes']+=got.get('stored_bytes',0)
   stage(name,got['status'],url=url,http_attempts=got['attempts'],error=got.get('error'),raw_uri=got.get('raw_uri'),final_url=got.get('final_url'))
   body=self.read(got['raw_uri']) if got.get('raw_uri') else b''
   analysis=consider(body,got.get('final_url',url),got.get('raw_uri'),got.get('http_status'),name) if body and got.get('http_status')==200 and 'partial' not in (got.get('error') or '') else None
   return got,analysis
  try:
   if _phase in (None,'publisher'):
    active('HTTP + extraction')
    first,analysis=retrieve(item['url'],'http')
    unresolved|=first['status'] in ('temporary_error','rate_limited')
    # Canonical/OG URLs are discovered from the response, never manually supplied.
    if not best or best['quality']!='candidate':
     choices=list(dict.fromkeys((analysis or {}).get('discovered',[])))[:2]
     if not choices:stage('publisher_url_discovery','no_candidate',reason='No alternative same-host canonical/OG article URL in response')
     for url in choices:
      if elapsed()>240:unresolved=True;stage('publisher_url_discovery','deferred',reason='Per-URL time budget');break
      got,_=retrieve(url,'publisher_url_discovery');unresolved|=got['status'] in ('temporary_error','rate_limited')
      if best and best['quality']=='candidate':break
    else:stage('publisher_url_discovery','not_needed',reason='Usable candidate from initial HTML')
   if _phase!='archive':
    if not best or best['quality']!='candidate':
     if first['status'] in ('blocked','robots_denied','robots_unavailable','rate_limited') or (analysis or {}).get('paywall'):
      stage('browser','not_applicable',reason='Access restriction, robots policy or paywall; browser is not a bypass')
     elif first.get('http_status')!=200:
      stage('browser','not_applicable',reason='No successful HTML document to render')
     elif elapsed()>240:
      unresolved=True;stage('browser','deferred',reason='Per-URL time budget')
     else:
      if _phase=='publisher':return handoff('browser',first=first,analysis=analysis)
      active('browser')
      try:
       body,source=self.render(first.get('final_url',item['url']))
       raw=self.put(f'runs/{run}/rendered/{aid}.html.gz',gzip.compress(body,mtime=0),'application/gzip')
       result['stored_bytes']+=len(gzip.compress(body));result['response_bytes']+=len(body)
       stage('browser','rendered',raw_uri=raw,url=source);consider(body,source,raw,method='browser')
      except Exception as e:unresolved=True;stage('browser','deferred',reason=type(e).__name__+': '+str(e)[:250])
    else:stage('browser','not_needed',reason='Usable candidate already found')
   if _phase in ('publisher','browser') and (not best or best['quality']!='candidate'):
    return handoff('archive')
   if not best or best['quality']!='candidate':
    seen=set()
    for stamp in [str(item.get('first_observed','')).replace('-',''),'']:
     if elapsed()>300:
      unresolved=True;stage('archive','deferred',reason='Per-URL time budget');break
     active('archive lookup')
     api='https://archive.org/wayback/available?'+urlencode({'url':item['url'],**({'timestamp':stamp} if stamp else {})})
     payload=None
     for attempt in range(2):
      try:
       code,headers,body,large=self.one(api)
       if code!=200 or large:raise RuntimeError('Archive availability HTTP '+str(code))
       payload=json.loads(body);uri=self.put(f'runs/{run}/archive-lookups/{aid}-{stamp or "latest"}.json',body,'application/json');stage('archive_lookup','checked',timestamp=stamp or 'latest',lookup_uri=uri);break
      except Exception as e:
       stage('archive_lookup','error',attempt=attempt+1,reason=str(e)[:250])
       if attempt==0:time.sleep(5)
     if payload is None:unresolved=True;continue
     match=payload.get('archived_snapshots',{}).get('closest',{})
     if not match.get('available'):stage('archive','no_snapshot',timestamp=stamp or 'latest');continue
     url=match.get('url','').replace('http://web.archive.org/','https://web.archive.org/')
     if urlsplit(url).hostname!='web.archive.org':unresolved=True;stage('archive','deferred',reason='Unexpected archive host');continue
     if url in seen:stage('archive','duplicate_snapshot');continue
     seen.add(url);got,arch_analysis=retrieve(url,'archive');events[-1]['snapshot_timestamp']=match.get('timestamp')
     if not best or best['quality']!='candidate':
      for alternate in list(dict.fromkeys((arch_analysis or {}).get('discovered',[])))[:2]:
       if alternate in seen or urlsplit(alternate).hostname!='web.archive.org':continue
       if elapsed()>300:unresolved=True;stage('archive_canonical','deferred',reason='Per-URL time budget');break
       seen.add(alternate);alt,_=retrieve(alternate,'archive_canonical');unresolved|=alt['status'] in ('temporary_error','rate_limited','robots_unavailable')
       if best and best['quality']=='candidate':break
     unresolved|=got['status'] in ('temporary_error','rate_limited','robots_unavailable')
     if best and best['quality']=='candidate':break
   else:stage('archive','not_needed',reason='Usable candidate already found')
   if best:
    data=gzip.compress(best['text'].encode(),mtime=0)
    result.update(text_uri=self.put(f'runs/{run}/extracted/{aid}.txt.gz',data,'application/gzip'),raw_uri=best['raw_uri'],final_url=best['url'],content_sha256=best['digest'],http_status=best['http_status'])
    result['stored_bytes']+=len(data)
   result['status']='saved' if best and best['quality']=='candidate' else 'deferred' if unresolved else 'partial' if best else 'exhausted'
   result['error']=None if result['status']=='saved' else 'Some toolbox stages need retry' if unresolved else 'All applicable toolbox stages exhausted'
   stage('final',result['status'],version=VERSION,quality=best['quality'] if best else 'missing',characters=len(best['text']) if best else 0)
   if _phase is not None and result['status']=='deferred':result['_retry_best']=copy.deepcopy(best)
   return result
  finally:
   with self.lock:self.live.pop(aid,None)
 def render(self,url):
  request_guard=self.request_guard()
  def authorize(request_url,document):
   with self.guarded_requests(request_guard):
    request_guard()
    parsed=public_url(request_url)
    if document:
     parser,allowed,delay,_=self.policy(request_url)
     if not allowed or (parser and not parser.can_fetch(UA,request_url)):return False
     with self.host_slot(parsed.hostname,delay):pass
    return True
  def response(request_url,status,retry_after):
   with self.guarded_requests(request_guard):
    request_guard()
    self.observe_response(request_url,status,retry_after)
  return render_isolated(url,authorize,queue_wait=self.host_queue_observer(),response=response)
