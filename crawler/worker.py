"""A resumable job: parallel across outlets, serial within each outlet."""
import gzip,io,json,os,signal,sys,time,uuid
from collections import Counter,defaultdict,deque
from concurrent.futures import ThreadPoolExecutor,wait,FIRST_COMPLETED
from datetime import datetime,timezone,timedelta
from google.cloud import storage,bigquery
from google.api_core.exceptions import NotFound
from crawl import key,now
from retrying import RetryingPipeline
from memory_recovery import Recovery,reclaim_memory,retry_owns_lease
from result_store import ResultIndex,JsonlBatch,encode_jsonl,iter_input_rows,memory_usage

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
        self.task_attempt=int(os.environ.get('CLOUD_RUN_TASK_ATTEMPT','0'));self.task_index=os.environ.get('CLOUD_RUN_TASK_INDEX','0');self.recovery=None
        self.lease=self.bucket.blob('control/worker-lease.json');self.owner=uuid.uuid4().hex
        self.done=ResultIndex();self.checkpoint=JsonlBatch(50,4*1024*1024);self.to_bq=JsonlBatch(1000,8*1024*1024);self.paused={};self.active={};self.queues=defaultdict(deque)
        self.last_checkpoint=time.monotonic();self.last_bq=time.monotonic()
        self.bq=bigquery.Client(project=self.config['project']);self.fetcher=RetryingPipeline(self.bucket,self.run_id,self.config['delay_seconds'],self.config['max_attempts'])
    def put(self,path,value):self.bucket.blob(path).upload_from_string(json.dumps(value,separators=(',',':')),content_type='application/json',timeout=60)
    def lease_update(self,initial=False):
        payload=json.dumps({'owner':self.owner,'run_id':self.run_id,'execution':self.execution,'task_index':self.task_index,'task_attempt':self.task_attempt,'expires_at':(datetime.now(timezone.utc)+timedelta(minutes=10)).isoformat()})
        generation=0
        if initial:
            try:
                self.lease.reload();generation=self.lease.generation
                previous=json.loads(self.lease.download_as_text(if_generation_match=generation))
                if datetime.fromisoformat(previous['expires_at'])>datetime.now(timezone.utc) and not retry_owns_lease(previous,self.execution,self.task_index,self.task_attempt):raise RuntimeError('Another crawler holds the global worker lease')
                generation=self.lease.generation
            except NotFound:pass
        else:generation=self.lease.generation
        self.lease.upload_from_string(payload,if_generation_match=generation,content_type='application/json',timeout=60);self.lease.reload()
    def result(self,r):
        encoded=encode_jsonl(r)
        if not self.checkpoint.fits(encoded):self.flush_checkpoint()
        self.checkpoint.append(encoded);self.done.record(r)
        self.queue_bq(r)
        # Upload promptly at the cap; buffers never grow with the full run.
        if self.checkpoint.full:self.flush_checkpoint()
    def queue_bq(self,r):
        row={**r};row['attempts_json']=json.dumps(row.pop('attempts',[]),separators=(',',':'))
        encoded=encode_jsonl(row)
        if not self.to_bq.fits(encoded):self.flush_bq()
        self.to_bq.append(encoded)
        if self.to_bq.full:self.flush_bq()
    def flush_checkpoint(self):
        if not self.checkpoint:return
        data=gzip.compress(self.checkpoint.data(),mtime=0)
        self.bucket.blob(self.prefix+'checkpoints/'+uuid.uuid4().hex+'.jsonl.gz').upload_from_string(data,content_type='application/gzip',timeout=60)
        self.checkpoint.clear();self.last_checkpoint=time.monotonic()
    def flush_bq(self):
        if not self.to_bq:return
        # Persist evidence first. A failed load can always be replayed on restart.
        self.flush_checkpoint();self.lease_update()
        with io.BytesIO(self.to_bq.data()) as data:
            config=bigquery.LoadJobConfig(ignore_unknown_values=True,source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
            self.bq.load_table_from_file(data,self.config['dataset']+'.crawl_result_events',job_config=config).result(timeout=90)
        self.to_bq.clear();self.last_bq=time.monotonic()
    def flush(self,final=False):
        if self.checkpoint and (final or self.checkpoint.full or time.monotonic()-self.last_checkpoint>60):self.flush_checkpoint()
        if self.to_bq and (final or self.to_bq.full or time.monotonic()-self.last_bq>600):self.flush_bq()
    def restore(self):
        for blob in self.bucket.list_blobs(prefix=self.prefix+'checkpoints/'):
            with blob.open('rb',chunk_size=1024*1024) as source,gzip.GzipFile(fileobj=source) as records:
                for line in records:
                    if not line.strip():continue
                    result=json.loads(line);self.done.record(result);self.queue_bq(result)
        # Replay all checkpoint events in bounded batches. The existing latest-row
        # BQ view deduplicates events if an earlier load already succeeded.
        self.flush_bq()
    def load_inputs(self):
        total=0
        with self.bucket.blob(self.prefix+'inputs.json.gz').open('rb',chunk_size=1024*1024) as source,gzip.GzipFile(fileobj=source) as decoded,io.TextIOWrapper(decoded,encoding='utf-8') as stream:
            for row in iter_input_rows(stream):
                total+=1
                if key(row['url']) not in self.done:self.queues[row['outlet']].append(row)
        return total
    def progress(self,state,total,error=None):
        totals=self.done.snapshot();counts=totals['counts'];domains=defaultdict(Counter,totals['domains'])
        attempts=totals['attempts'];retries=totals['retries'];byte_count=totals['response_bytes'];stored=totals['stored_bytes']
        for domain,items in self.queues.items():domains[domain]['pending']+=len(items)
        for domain in self.active.values():domains[domain]['downloading']+=1
        elapsed=time.monotonic()-self.started
        summary={'country':self.country,'run_id':self.run_id,'phase':self.config['phase'],'state':state,'updated_at':now(),'execution':self.execution,'total':total,'processed':len(self.done),'pending':total-len(self.done)-len(self.active),'downloading':len(self.active),'counts':dict(counts),'attempts':attempts,'retries':retries,'response_bytes':byte_count,'stored_bytes':stored,'memory':memory_usage(),'elapsed_seconds':round(elapsed),'estimated_compute_usd':round(elapsed*.000020,4),'limits':{k:self.config[k] for k in ['workers','delay_seconds','max_attempts','max_runtime_seconds','max_response_bytes','max_total_attempts']},'domains':[{'outlet':d,**dict(c),'pause_reason':self.paused.get(d)} for d,c in sorted(domains.items())],'toolbox_version':'toolbox-2','active_stages':list(self.fetcher.live.values()),'error':error,'result_table':self.config['dataset']+'.crawl_results','source_table':self.config['source_table']}
        self.put(self.prefix+'progress.json',summary);self.put('progress/'+self.country+'.json',summary)
        return summary
    def run(self):
        self.lease_update(initial=True);state='running';total=0
        try:
            if self.recovery is None:
                self.recovery=Recovery(self.bucket,self.prefix,self.execution,self.config['max_runtime_seconds'])
                self.started=time.monotonic()-self.recovery.elapsed
                if self.recovery.reduced_concurrency:self.config['workers']=1
            # Publish restart state while restoring; an interrupted attempt is not done.
            try:
                previous=json.loads(self.bucket.blob(self.prefix+'progress.json').download_as_text())
                previous.update(state='recovering_memory' if self.recovery.reduced_concurrency else 'starting',execution=self.execution,updated_at=now(),downloading=0,active_stages=[],error=None)
                self.put('progress/'+self.country+'.json',previous)
            except NotFound:pass
            self.restore();total=self.load_inputs()
            last_progress=0;block_streak=Counter()
            with ThreadPoolExecutor(max_workers=self.config['workers']) as pool:
                while any(self.queues.values()) or self.active:
                    if time.monotonic()-last_progress>=15:
                        self.lease_update();summary=self.progress(state,total);last_progress=time.monotonic();self.flush()
                        if time.monotonic()-self.started>self.config['max_runtime_seconds'] or summary['response_bytes']>=self.config['max_response_bytes'] or summary['attempts']>=self.config['max_total_attempts']:state='paused_limit'
                        if self.bucket.blob(self.prefix+'STOP').exists():state='paused_by_operator'
                    if STOP:state='interrupted'
                    memory=memory_usage();memory_pressure=bool(memory.get('limit_bytes') and memory.get('current_bytes',0)>=memory['limit_bytes']*.75)
                    if state=='running' and not memory_pressure:
                        for domain in self.queues:
                            if len(self.active)>=self.config['workers']:break
                            if not self.queues[domain] or domain in self.active.values() or domain in self.paused:continue
                            item=self.queues[domain].popleft();future=pool.submit(self.fetcher.fetch,item,self.run_id,self.country);self.active[future]=domain
                    if not self.active:
                        if memory_pressure and any(self.queues.values()) and state=='running':
                            self.flush(final=True);reclaim_memory()
                            memory=memory_usage()
                            if not memory.get('limit_bytes') or memory.get('current_bytes',0)<memory['limit_bytes']*.75:continue
                            state='recovering_memory' if self.recovery.reserve_restart() else 'recovery_failed'
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
            self.flush(final=True);summary=self.progress(state,total,'Releasing memory and restarting automatically with one download at a time.' if state=='recovering_memory' else 'Automatic memory recovery limit reached; saved progress requires attention.' if state=='recovery_failed' else None)
            self.bq.load_table_from_json([{'run_id':self.run_id,'country':self.country,'updated_at':now(),'state':state,'config_json':json.dumps(self.config),'summary_json':json.dumps(summary)}],self.config['dataset']+'.crawl_run_events').result(timeout=90)
        except Exception as e:
            try:self.flush(final=True);self.progress('failed',total,type(e).__name__+': '+str(e)[:600])
            except Exception:pass
            raise
        finally:
            try:self.lease.delete(if_generation_match=self.lease.generation)
            except Exception:pass
        print(json.dumps({'run_id':self.run_id,'state':state,'processed':len(self.done)}),flush=True)
        return state
if __name__=='__main__':
    if os.environ.get('VERIFY_RECOVERY')=='1':
        import verify_recovery
        verify_recovery.main()
    elif os.environ.get('VERIFY_MEMORY')=='1':
        import verify_memory
        verify_memory.main()
    else:
        state=Run().run()
        if state=='recovering_memory':
            time.sleep(10)
            os.execv(sys.executable,[sys.executable,__file__])
