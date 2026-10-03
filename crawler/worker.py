"""A resumable job: parallel across outlets, serial within each outlet."""
import gzip,json,os,signal,time,uuid
from collections import Counter,defaultdict,deque
from concurrent.futures import ThreadPoolExecutor,wait,FIRST_COMPLETED
from datetime import datetime,timezone,timedelta
from google.cloud import storage,bigquery
from google.api_core.exceptions import NotFound
from crawl import key,now
from pipeline import Pipeline

STOP=False
def halt(*_):
    global STOP
    STOP=True
signal.signal(signal.SIGTERM,halt)
signal.signal(signal.SIGINT,halt)

class Run:
    def __init__(self):
        self.bucket=storage.Client().bucket(os.environ['CRAWL_BUCKET'])
        self.run_id=os.environ['RUN_ID'];self.prefix='runs/'+self.run_id+'/'
        self.config=json.loads(self.bucket.blob(self.prefix+'config.json').download_as_text())
        self.country=self.config['country'];self.started=time.monotonic();self.execution=os.environ.get('CLOUD_RUN_EXECUTION','local')
        self.lease=self.bucket.blob('control/worker-lease.json');self.owner=uuid.uuid4().hex
        self.done={};self.checkpoint=[];self.to_bq=[];self.paused={};self.active={};self.queues=defaultdict(deque)
        self.last_checkpoint=time.monotonic();self.last_bq=time.monotonic()
        self.bq=bigquery.Client(project=self.config['project']);self.fetcher=Pipeline(self.bucket,self.run_id,self.config['delay_seconds'],self.config['max_attempts'])
    def put(self,path,value):self.bucket.blob(path).upload_from_string(json.dumps(value,separators=(',',':')),content_type='application/json',timeout=60)
    def lease_update(self,initial=False):
        payload=json.dumps({'owner':self.owner,'run_id':self.run_id,'expires_at':(datetime.now(timezone.utc)+timedelta(minutes=10)).isoformat()})
        generation=0
        if initial:
            try:
                self.lease.reload();generation=self.lease.generation
                previous=json.loads(self.lease.download_as_text(if_generation_match=generation))
                if datetime.fromisoformat(previous['expires_at'])>datetime.now(timezone.utc):raise RuntimeError('Another crawler holds the global worker lease')
                generation=self.lease.generation
            except NotFound:pass
        else:generation=self.lease.generation
        self.lease.upload_from_string(payload,if_generation_match=generation,content_type='application/json',timeout=60);self.lease.reload()
    def result(self,r):
        self.done[r['article_id']]=r;self.checkpoint.append(r);self.to_bq.append(r)
    def flush(self,final=False):
        if self.checkpoint and (final or len(self.checkpoint)>=50 or time.monotonic()-self.last_checkpoint>60):
            data=gzip.compress(('\n'.join(json.dumps(r) for r in self.checkpoint)+'\n').encode(),mtime=0)
            self.bucket.blob(self.prefix+'checkpoints/'+uuid.uuid4().hex+'.jsonl.gz').upload_from_string(data,content_type='application/gzip',timeout=60)
            self.checkpoint=[];self.last_checkpoint=time.monotonic()
        if self.to_bq and (final or len(self.to_bq)>=1000 or time.monotonic()-self.last_bq>600):
            data=[]
            for r in self.to_bq:
                v={**r};v['attempts_json']=json.dumps(v.pop('attempts'));data.append(v)
            self.lease_update()
            self.bq.load_table_from_json(data,self.config['dataset']+'.crawl_result_events',job_config=bigquery.LoadJobConfig(ignore_unknown_values=True)).result(timeout=90)
            self.to_bq=[];self.last_bq=time.monotonic()
    def progress(self,state,total,error=None):
        counts=Counter();domains=defaultdict(Counter);attempts=0;byte_count=0;stored=0;retries=0
        for r in self.done.values():
            counts[r['status']]+=1;domains[r['outlet']][r['status']]+=1;attempts+=sum(len(e.get('http_attempts',[])) for e in r['attempts']);retries+=sum(max(0,len(e.get('http_attempts',[]))-1) for e in r['attempts'])
            byte_count+=r.get('response_bytes',0);stored+=r.get('stored_bytes',0)
        for domain,items in self.queues.items():domains[domain]['pending']+=len(items)
        for domain in self.active.values():domains[domain]['downloading']+=1
        elapsed=time.monotonic()-self.started
        summary={'country':self.country,'run_id':self.run_id,'phase':self.config['phase'],'state':state,'updated_at':now(),'execution':self.execution,'total':total,'processed':len(self.done),'pending':total-len(self.done)-len(self.active),'downloading':len(self.active),'counts':dict(counts),'attempts':attempts,'retries':retries,'response_bytes':byte_count,'stored_bytes':stored,'elapsed_seconds':round(elapsed),'estimated_compute_usd':round(elapsed*.000020,4),'limits':{k:self.config[k] for k in ['workers','delay_seconds','max_attempts','max_runtime_seconds','max_response_bytes','max_total_attempts']},'domains':[{'outlet':d,**dict(c),'pause_reason':self.paused.get(d)} for d,c in sorted(domains.items())],'toolbox_version':'toolbox-2','active_stages':list(self.fetcher.live.values()),'url_results':[{'url':r['url'],'outlet':r['outlet'],'status':r['status'],'stages':[{'stage':e.get('stage'),'status':e.get('status'),'reason':e.get('reason') or e.get('error'),'url':e.get('url')} for e in r['attempts']]} for r in self.done.values()],'error':error,'result_table':self.config['dataset']+'.crawl_results','source_table':self.config['source_table']}
        self.put(self.prefix+'progress.json',summary);self.put('progress/'+self.country+'.json',summary)
        return summary
    def run(self):
        self.lease_update(initial=True);state='running';total=0
        try:
            for blob in self.bucket.list_blobs(prefix=self.prefix+'checkpoints/'):
                for line in gzip.decompress(blob.download_as_bytes()).decode().splitlines():
                    r=json.loads(line)
                    previous=self.done.get(r['article_id'])
                    if previous is None or r['updated_at']>previous['updated_at']:self.done[r['article_id']]=r
            self.to_bq=list(self.done.values())
            inputs=json.loads(gzip.decompress(self.bucket.blob(self.prefix+'inputs.json.gz').download_as_bytes()));total=len(inputs)
            for r in inputs:
                if key(r['url']) not in self.done:self.queues[r['outlet']].append(r)
            last_progress=0;block_streak=Counter()
            with ThreadPoolExecutor(max_workers=self.config['workers']) as pool:
                while any(self.queues.values()) or self.active:
                    if time.monotonic()-last_progress>=15:
                        self.lease_update();summary=self.progress(state,total);last_progress=time.monotonic();self.flush()
                        if time.monotonic()-self.started>self.config['max_runtime_seconds'] or summary['response_bytes']>=self.config['max_response_bytes'] or summary['attempts']>=self.config['max_total_attempts']:state='paused_limit'
                        if self.bucket.blob(self.prefix+'STOP').exists():state='paused_by_operator'
                    if STOP:state='interrupted'
                    if state=='running':
                        for domain in self.queues:
                            if len(self.active)>=self.config['workers']:break
                            if not self.queues[domain] or domain in self.active.values() or domain in self.paused:continue
                            item=self.queues[domain].popleft();future=pool.submit(self.fetcher.fetch,item,self.run_id,self.country);self.active[future]=domain
                    if not self.active:
                        if any(self.queues.values()) and state=='running':state='completed_with_deferred'
                        elif state=='running':state='completed'
                        break
                    finished,_=wait(self.active,timeout=1,return_when=FIRST_COMPLETED)
                    for future in finished:
                        domain=self.active.pop(future);r=future.result();self.result(r)
                        if r['status'] in ('blocked','rate_limited','robots_unavailable'):
                            block_streak[domain]+=1
                            if block_streak[domain]>=2 or r['status'] in ('rate_limited','robots_unavailable'):self.paused[domain]=r['status']
                        else:block_streak[domain]=0
                if state=='running':state='completed'
            self.flush(final=True);summary=self.progress(state,total)
            self.bq.load_table_from_json([{'run_id':self.run_id,'country':self.country,'updated_at':now(),'state':state,'config_json':json.dumps(self.config),'summary_json':json.dumps(summary)}],self.config['dataset']+'.crawl_run_events').result(timeout=90)
        except Exception as e:
            try:self.flush(final=True);self.progress('failed',total,type(e).__name__+': '+str(e)[:600])
            except Exception:pass
            raise
        finally:
            try:self.lease.delete(if_generation_match=self.lease.generation)
            except Exception:pass
        print(json.dumps({'run_id':self.run_id,'state':state,'processed':len(self.done)}),flush=True)
if __name__=='__main__':Run().run()
