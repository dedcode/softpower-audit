"""A resumable job with bounded article concurrency and shared host pacing."""
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
        if 'CRAWL_REQUEST_SPACING' in os.environ:
            self.config['delay_seconds']=float(os.environ['CRAWL_REQUEST_SPACING'])
        self.country=self.config['country'];self.started=time.monotonic();self.execution=os.environ.get('CLOUD_RUN_EXECUTION','local')
        self.task_attempt=int(os.environ.get('CLOUD_RUN_TASK_ATTEMPT','0'));self.task_index=os.environ.get('CLOUD_RUN_TASK_INDEX','0');self.recovery=None
        self.lease=self.bucket.blob('control/worker-lease.json');self.owner=uuid.uuid4().hex
        self.done=ResultIndex();self.checkpoint=JsonlBatch(50,4*1024*1024);self.to_bq=JsonlBatch(1000,8*1024*1024);self.paused={};self.active={};self.inflight_items={};self.queues=defaultdict(deque)
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
    def schedule(self,pool):
        # Overlap parsing/archive recovery with later articles from an outlet.
        # Fetcher still serializes actual requests and enforces host spacing.
        # Start large backlogs early rather than exhausting alphabetical domains.
        inflight=Counter(self.active.values())
        per_outlet=self.config.get('per_outlet_workers',1)
        while len(self.active)<self.config['workers']:
            eligible=[d for d,q in self.queues.items() if q and d not in self.paused and inflight[d]<per_outlet]
            if not eligible:break
            domain=max(eligible,key=lambda d:(len(self.queues[d])/(inflight[d]+1),d))
            item=self.queues[domain].popleft()
            future=pool.submit(self.fetcher.fetch,item,self.run_id,self.country)
            self.active[future]=domain;self.inflight_items[future]=item;inflight[domain]+=1
    def progress(self,state,total,error=None):
        totals=self.done.snapshot();counts=totals['counts'];domains=defaultdict(Counter,totals['domains'])
        attempts=totals['attempts'];retries=totals['retries'];byte_count=totals['response_bytes'];stored=totals['stored_bytes']
        for domain,items in self.queues.items():domains[domain]['pending']+=len(items)
        for domain in self.active.values():domains[domain]['downloading']+=1
        elapsed=time.monotonic()-self.started
        compute_rate=float(os.environ.get('CRAWL_CPU','1'))*.000018+float(os.environ.get('CRAWL_MEMORY_GIB','1'))*.000002
        with self.fetcher.lock:active_stages=list(self.fetcher.live.values())
        summary={'country':self.country,'run_id':self.run_id,'phase':self.config['phase'],'state':state,'updated_at':now(),'execution':self.execution,'total':total,'processed':len(self.done),'pending':total-len(self.done)-len(self.active),'downloading':len(self.active),'counts':dict(counts),'attempts':attempts,'retries':retries,'response_bytes':byte_count,'stored_bytes':stored,'memory':memory_usage(),'elapsed_seconds':round(elapsed),'estimated_compute_usd':round(elapsed*compute_rate,4),'limits':{k:self.config[k] for k in ['workers','delay_seconds','max_attempts','max_runtime_seconds','max_response_bytes','max_total_attempts']},'domains':[{'outlet':d,**dict(c),'pause_reason':self.paused.get(d)} for d,c in sorted(domains.items())],'toolbox_version':'toolbox-2','active_stages':active_stages,'error':error,'result_table':self.config['dataset']+'.crawl_results','source_table':self.config['source_table']}
        summary['limits']['per_outlet_workers']=self.config.get('per_outlet_workers',1)
        self.put(self.prefix+'progress.json',summary);self.put('progress/'+self.country+'.json',summary)
        return summary
    def budget_reached(self,summary):
        runtime_limit=self.config.get('max_runtime_seconds')
        return ((runtime_limit is not None and time.monotonic()-self.started>runtime_limit)
                or summary['response_bytes']>=self.config['max_response_bytes']
                or summary['attempts']>=self.config['max_total_attempts'])
    def run(self):
        self.lease_update(initial=True);state='running';total=self.config.get('input_count',0)
        try:
            rotation=os.environ.get('CRAWL_ROTATE_SECONDS')
            rotate_after=None if rotation is None else float(rotation)
            if rotate_after is not None and not 0<rotate_after<float('inf'):raise ValueError('CRAWL_ROTATE_SECONDS must be positive and finite')
            if self.recovery is None:
                self.recovery=Recovery(self.bucket,self.prefix,self.execution,self.config['max_runtime_seconds'])
                self.started=time.monotonic()-self.recovery.elapsed
                if self.recovery.reduced_concurrency:self.config['workers']=1
            # Publish restart state while restoring; an interrupted attempt is not done.
            try:
                previous=json.loads(self.bucket.blob(self.prefix+'progress.json').download_as_text())
                previous.update(state='recovering_memory' if self.recovery.reduced_concurrency else 'starting',execution=self.execution,updated_at=now(),downloading=0,active_stages=[],error=None)
                for domain in previous.get('domains',[]):
                    domain['pending']=domain.get('pending',0)+domain.get('downloading',0);domain['downloading']=0
                self.put('progress/'+self.country+'.json',previous)
            except NotFound:pass
            self.restore();total=self.load_inputs()
            last_progress=0;block_streak=Counter();first_failure=None
            with ThreadPoolExecutor(max_workers=self.config['workers']) as pool:
                while any(self.queues.values()) or self.active:
                    if time.monotonic()-last_progress>=15:
                        try:
                            self.lease_update()
                            summary=self.progress(state,total,type(first_failure).__name__+': '+str(first_failure)[:600] if first_failure else None)
                            self.flush()
                            if first_failure is None:
                                if self.budget_reached(summary):state='paused_limit'
                                if self.bucket.blob(self.prefix+'STOP').exists():state='paused_by_operator'
                        except Exception as exc:
                            # A failed persistence service may remain unavailable
                            # while the other article futures finish. Keep draining.
                            if first_failure is None:first_failure=exc
                            state='failed'
                        last_progress=time.monotonic()
                    if STOP and first_failure is None:state='interrupted'
                    if state=='running' and rotate_after is not None and self.recovery.attempt_elapsed>=rotate_after and len(self.done)<total:
                        state='continuing';last_progress=0
                    memory=memory_usage();memory_pressure=bool(memory.get('limit_bytes') and memory.get('current_bytes',0)>=memory['limit_bytes']*.75)
                    if state=='running' and not memory_pressure:
                        self.schedule(pool)
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
                        domain=self.active.pop(future);item=self.inflight_items.pop(future)
                        try:
                            r=future.result();self.result(r)
                        except Exception as exc:
                            # Preserve the first error, stop scheduling, and still
                            # checkpoint successful peers before Cloud Run retries.
                            if first_failure is None:first_failure=exc
                            state='failed';last_progress=0
                            if key(item['url']) not in self.done:self.queues[domain].appendleft(item)
                            continue
                        if r['status'] in ('blocked','rate_limited','robots_unavailable'):
                            block_streak[domain]+=1
                            if block_streak[domain]>=2 or r['status'] in ('rate_limited','robots_unavailable'):self.paused[domain]=r['status']
                        else:block_streak[domain]=0
                if state=='running':state='completed'
            if first_failure is not None:raise first_failure
            if state=='continuing':
                # Rotation is automatic continuation, never an override of an
                # operator stop or exhausted resource budget while work drains.
                if STOP:state='interrupted'
                elif self.bucket.blob(self.prefix+'STOP').exists():state='paused_by_operator'
                elif len(self.done)>=total:state='completed'
                elif self.budget_reached(self.done.snapshot()):state='paused_limit'
            self.flush(final=True);summary=self.progress(state,total,'Releasing memory and restarting automatically with one download at a time.' if state=='recovering_memory' else 'Automatic memory recovery limit reached; saved progress requires attention.' if state=='recovery_failed' else 'Saved progress; collection will continue automatically in a fresh cloud worker.' if state=='continuing' else None)
            self.bq.load_table_from_json([{'run_id':self.run_id,'country':self.country,'updated_at':now(),'state':state,'config_json':json.dumps(self.config),'summary_json':json.dumps(summary)}],self.config['dataset']+'.crawl_run_events').result(timeout=90)
        except Exception as e:
            try:self.flush(final=True)
            except Exception:pass
            try:self.progress('failed',total,type(e).__name__+': '+str(e)[:600])
            except Exception:pass
            raise
        finally:
            preserve_error=sys.exc_info()[0] is not None
            try:self.lease.delete(if_generation_match=self.lease.generation)
            except NotFound:pass
            except Exception as release_error:
                if state=='continuing' and not preserve_error:
                    # A new execution cannot reclaim this live lease. Fail this
                    # task instead so its Cloud Run retry can reclaim it safely.
                    try:self.progress('failed',total,'Could not release worker lease for automatic continuation: '+type(release_error).__name__+': '+str(release_error)[:500])
                    except Exception:pass
                    raise
        print(json.dumps({'run_id':self.run_id,'state':state,'processed':len(self.done)}),flush=True)
        return state
if __name__=='__main__':
    if os.environ.get('VERIFY_CONTINUATION')=='1':
        import verify_continuation
        verify_continuation.main()
    elif os.environ.get('VERIFY_RECOVERY')=='1':
        import verify_recovery
        verify_recovery.main()
    elif os.environ.get('VERIFY_MEMORY')=='1':
        import verify_memory
        verify_memory.main()
    else:
        if os.environ.get('CRAWL_DISTRIBUTED')=='1':
            from distributed_worker import DistributedRun
            state=DistributedRun().run()
        else:state=Run().run()
        if state=='recovering_memory':
            time.sleep(10)
            os.execv(sys.executable,[sys.executable,__file__])
