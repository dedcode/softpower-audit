"""Conservative HTTP retrieval; originals are retained independently of extraction."""
import gzip,hashlib,ipaddress,json,math,os,socket,threading,time,uuid
from datetime import datetime,timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit,urljoin
from urllib.robotparser import RobotFileParser
import requests
from contextlib import contextmanager

AGENT='ChinaNewsResearchBot'
UA=AGENT+'/1.0 (+https://djelleldifallah.com/softpower-audit/extraction-status/)'
MAX_BODY=5*1024*1024

def now():return datetime.now(timezone.utc).isoformat()
def key(url):return hashlib.sha256(url.encode()).hexdigest()
def public_url(url):
    p=urlsplit(url)
    if p.scheme not in ('http','https') or not p.hostname or p.username or p.password or p.port not in (None,80,443):raise ValueError('Unsupported URL')
    addresses=socket.getaddrinfo(p.hostname,p.port or (443 if p.scheme=='https' else 80),type=socket.SOCK_STREAM)
    if not addresses or any(not ipaddress.ip_address(a[4][0]).is_global for a in addresses):raise ValueError('Non-public destination rejected')
    return p

def retry_seconds(value,attempt):
    try:return max(5,float(value))
    except (ValueError,TypeError):
        try:return max(5,(parsedate_to_datetime(value)-datetime.now(timezone.utc)).total_seconds())
        except Exception:return min(120,10*2**(attempt-1))

class HostCooldown(requests.RequestException):
    """A server-requested pause is longer than an individual bounded wait."""
    def __init__(self,host,seconds):
        self.retry_after_seconds=seconds
        super().__init__(f'{host} is cooling down; retry after {seconds:.0f} seconds')

class Fetcher:
    parse_initial=True
    def __init__(self,bucket,run_id,delay=3,max_attempts=3,host_coordinator=None):
        if not math.isfinite(delay) or delay<0:raise ValueError('Request spacing must be nonnegative and finite')
        self.bucket=bucket;self.run_id=run_id;self.delay=delay;self.max_attempts=max_attempts
        self.lock=threading.Lock();self.hostlocks={};self.last={};self.robots={};self.queue_wait=threading.local();self.cooldowns={}
        self.robots_delays={}
        self.network={'http_in_flight':0,'http_started':0,'http_completed':0,'http_errors':0,
                      'http_seconds':0.,'host_waiters':0,'host_wait_seconds':0.}
        self.host_coordinator=host_coordinator;self.shared_leases=threading.local()
        self.abort_event=threading.Event();self.request_context=threading.local()
        if self.host_coordinator is None and os.environ.get('CRAWL_FIRESTORE_DATABASE'):
            from google.cloud import firestore
            from shared_hosts import FirestoreHostStore,SharedHostCoordinator
            from adaptive_rate import decode_policies
            client=firestore.Client(project=bucket.client.project,database=os.environ['CRAWL_FIRESTORE_DATABASE'])
            self.host_coordinator=SharedHostCoordinator(FirestoreHostStore(client),
                max_concurrency=int(os.environ.get('CRAWL_HOST_CONCURRENCY','4')),poll_seconds=1,
                host_limits={host:int(os.environ.get('CRAWL_ARCHIVE_CONCURRENCY',os.environ.get('CRAWL_HOST_CONCURRENCY','4')))
                             for host in ('archive.org','web.archive.org')},
                adaptive_hosts=decode_policies(os.environ.get('CRAWL_ADAPTIVE_HOSTS_B64','')))
        elif self.host_coordinator is None and os.environ.get('CRAWL_ADAPTIVE_HOSTS_B64'):
            raise ValueError('Adaptive crawling requires the shared host coordinator')
    def network_snapshot(self):
        with self.lock:return dict(self.network)
    def dispatch_availability(self,hosts):
        if self.host_coordinator is None:return None
        with self.lock:delays={host:self.robots_delays[host] for host in hosts if host in self.robots_delays}
        return self.host_coordinator.availability(hosts,delay=self.delay,robots_delays=delays)
    @contextmanager
    def network_wait(self):
        began=time.monotonic()
        with self.lock:self.network['host_waiters']+=1
        try:
            with self.queued_wait():yield
        finally:
            with self.lock:
                self.network['host_waiters']-=1
                self.network['host_wait_seconds']+=max(0.,time.monotonic()-began)
    def request_guard(self):
        guard=getattr(self.request_context,'guard',None)
        def check():
            if self.abort_event.is_set():raise RuntimeError('Crawler ownership lost; no further requests permitted')
            if guard is not None:guard()
        return check
    def check_running(self):self.request_guard()()
    @contextmanager
    def guarded_requests(self,guard):
        previous=getattr(self.request_context,'guard',None)
        self.request_context.guard=guard
        try:yield
        finally:self.request_context.guard=previous
    def hostlock(self,host):
        with self.lock:return self.hostlocks.setdefault(host,threading.Lock())
    def host_queue_wait_seconds(self):
        return self.host_queue_observer()()
    def host_queue_observer(self):
        # Capture this article thread's state, not threading.local itself: the
        # browser watchdog reads it from a separate thread while a lock waits.
        if not hasattr(self.queue_wait,'state'):self.queue_wait.state=[(0.,None)]
        state=self.queue_wait.state
        state[0]=(getattr(self.queue_wait,'seconds',0.),state[0][1])
        def observe():
            total,started=state[0]
            return total+(time.monotonic()-started if started is not None else 0.)
        return observe
    @contextmanager
    def queued_wait(self):
        self.host_queue_observer()
        state=self.queue_wait.state;total,_=state[0]
        state[0]=(total,time.monotonic())
        try:yield
        finally:
            total,started=state[0]
            total+=time.monotonic()-started
            self.queue_wait.seconds=total;state[0]=(total,None)
    @contextmanager
    def queued_host_lock(self,host):
        # Count only acquisition time, not time spent holding an outer robots
        # lock. Nested publisher/robots locks therefore do not double-count.
        lock=self.hostlock(host)
        with self.queued_wait():lock.acquire()
        try:yield
        finally:lock.release()
    @contextmanager
    def host_slot(self,host,delay=None):
        if self.host_coordinator is not None:
            # The shared coordinator independently limits starts and in-flight
            # responses. A local mutex here would serialize the whole download
            # again, defeating the shared capacity even inside one instance.
            self.check_running()
            with self.lock:robots_delay=self.robots_delays.get(host)
            with self.network_wait():
                lease=self.host_coordinator.acquire(host,max(self.delay,delay or 0),
                    cancelled=self.abort_event.is_set,guard=self.request_guard(),robots_delay=robots_delay)
            if not hasattr(self.shared_leases,'active'):self.shared_leases.active={}
            with lease:
                self.shared_leases.active[host]=lease
                try:
                    yield
                    lease.check()
                finally:self.shared_leases.active.pop(host,None)
            return
        with self.queued_host_lock(host):
            self.check_running()
            self.wait_for_host_cooldown(host)
            gap=max(self.delay,delay or 0)-(time.monotonic()-self.last.get(host,0))
            if gap>0:
                with self.queued_wait():time.sleep(gap)
            self.last[host]=time.monotonic()
            yield
    def scoped_path(self,path):
        scope=getattr(self.request_context,'output_scope',None)
        prefix='runs/'+self.run_id+'/'
        if scope is not None and path.startswith(prefix):
            scope=str(scope)
            if not scope or any(part in ('','.','..') for part in scope.split('/')):raise ValueError('Invalid claim output scope')
            return prefix+'claims/'+scope+'/'+path[len(prefix):]
        return path
    def put(self,path,data,content_type):
        self.check_running();path=self.scoped_path(path)
        blob=self.bucket.blob(path);blob.upload_from_string(data,content_type=content_type,timeout=60)
        return 'gs://'+self.bucket.name+'/'+path
    def defer_host(self,host,seconds):
        # Caller retains the host lock until the response is handled. All
        # article threads and archive lookups therefore observe the same pause.
        if self.host_coordinator is not None:
            lease=getattr(self.shared_leases,'active',{}).get(host)
            if lease is None:raise RuntimeError('Shared Retry-After requires the active host lease')
            lease.defer(seconds)
        with self.lock:self.cooldowns[host]=max(self.cooldowns.get(host,0),time.monotonic()+seconds)
    def observe_response(self,url,status,retry_after=None):
        if status not in (429,503):return
        self.check_running()
        host=public_url(url).hostname;seconds=retry_seconds(retry_after,1)
        if self.host_coordinator is not None:self.host_coordinator.defer(host,seconds)
        # Browser callbacks may arrive while another local HTTP request holds
        # its host lock. Only extend the cooldown; do not block that callback.
        with self.lock:self.cooldowns[host]=max(self.cooldowns.get(host,0),time.monotonic()+seconds)
    def wait_for_host_cooldown(self,host):
        # Called with the host lock held, before admitting any HTTP/browser
        # request. Long pauses remain retryable without tying up a thread or
        # sending another request against the server's Retry-After instruction.
        remaining=self.cooldowns.get(host,0)-time.monotonic()
        if remaining>120:raise HostCooldown(host,remaining)
        if remaining>0:
            with self.queued_wait():time.sleep(remaining)
    def one(self,url,delay=None):
        self.check_running()
        p=public_url(url)
        with self.host_slot(p.hostname,delay):
            self.check_running()
            started=time.monotonic();completed=False;status=None
            with self.lock:
                self.network['http_started']+=1;self.network['http_in_flight']+=1
            try:
                with requests.Session() as s:
                    s.trust_env=False
                    with s.get(url,headers={'User-Agent':UA,'Accept':'text/html,application/xhtml+xml;q=0.9,*/*;q=0.1'},timeout=(10,25),allow_redirects=False,stream=True) as r:
                        status=r.status_code
                        if r.status_code in (429,503):self.defer_host(p.hostname,retry_seconds(r.headers.get('Retry-After'),1))
                        data=bytearray();oversize=False;began=time.monotonic()
                        for chunk in r.iter_content(65536):
                            data.extend(chunk)
                            if len(data)>MAX_BODY or time.monotonic()-began>40:oversize=True;break
                        completed=True
                        return r.status_code,requests.structures.CaseInsensitiveDict(r.headers),bytes(data[:MAX_BODY]),oversize
            finally:
                duration=max(0.,time.monotonic()-started)
                with self.lock:
                    self.network['http_in_flight']-=1
                    self.network['http_completed' if completed else 'http_errors']+=1
                    self.network['http_seconds']+=duration
                lease=getattr(self.shared_leases,'active',{}).get(p.hostname)
                observe=getattr(lease,'observe',None)
                if callable(observe):observe(status,duration,transport_error=not completed)
    def policy(self,url):
        p=public_url(url);origin=p.scheme+'://'+p.netloc
        with self.queued_host_lock('robots:'+origin):
            if origin in self.robots:return self.robots[origin]
            robot_url=origin+'/robots.txt';original=robot_url
            for _ in range(4):
                code,headers,body,large=self.one(robot_url)
                if code in (301,302,303,307,308) and headers.get('Location'):
                    robot_url=urljoin(robot_url,headers['Location']);continue
                break
            uri=self.put('runs/'+self.run_id+'/robots/'+key(origin)+'.json',json.dumps({'url':original,'final_url':robot_url,'status':code,'checked_at':now(),'body':body.decode('utf-8','replace')[:1000000]}),'application/json')
            actual_delay=None
            if code in (404,410):result=(None,True,self.delay,uri);actual_delay=0.
            elif code!=200 or large:result=(None,False,self.delay,uri)
            else:
                parser=RobotFileParser();parser.parse(body.decode('utf-8','replace').splitlines())
                actual_delay=float(parser.crawl_delay(AGENT) or parser.crawl_delay('*') or 0)
                rate=parser.request_rate(AGENT) or parser.request_rate('*')
                if rate:actual_delay=max(actual_delay,rate.seconds/rate.requests)
                delay=max(self.delay,actual_delay)
                result=(parser,True,delay,uri)
            if actual_delay is not None:
                with self.lock:self.robots_delays[p.hostname]=max(actual_delay,self.robots_delays.get(p.hostname,0))
            self.robots[origin]=result;return result
    def fetch(self,item,run,country):
        url=item['url'];article_id=key(url)
        result={**item,'article_id':article_id,'run_id':run,'country':country,'updated_at':now(),
            'status':'failed','attempts':[],'http_status':None,'final_url':url,'raw_uri':None,'text_uri':None,
            'content_sha256':None,'response_bytes':0,'stored_bytes':0,'error':None,'reused':False,'robots_uri':None}
        cached=self.bucket.blob('latest/'+article_id+'.json')
        if cached.exists():
            previous=json.loads(cached.download_as_text())
            if previous['status']=='saved' or (not self.parse_initial and previous['status']=='retrieved'):
                for field in ('http_status','final_url','raw_uri','text_uri','content_sha256','robots_uri','fetched_at'):
                    result[field]=previous.get(field)
                result.update(status=previous['status'],reused=True);return result
        for attempt in range(1,self.max_attempts+1):
            event={'attempt':attempt,'started_at':now()};current=url
            try:
                for redirect in range(6):
                    parser,allowed,delay,robots_uri=self.policy(current);result['robots_uri']=robots_uri
                    if not allowed or (parser and not parser.can_fetch(AGENT,current)):
                        result['status']='robots_denied' if allowed else 'robots_unavailable';event.update(status=result['status'],url=current);break
                    code,headers,body,large=self.one(current,delay)
                    result['response_bytes']+=len(body)
                    digest=hashlib.sha256(body).hexdigest();compressed=gzip.compress(body,mtime=0)
                    raw=self.put('raw/'+article_id+'/'+digest+'.body.gz',compressed,'application/gzip')
                    meta=self.put('runs/'+run+'/responses/'+article_id+f'-{attempt}-{redirect}-{uuid.uuid4().hex}.json',json.dumps({'requested_url':current,'status':code,'headers':dict(headers),'fetched_at':now(),'body_uri':raw,'truncated':large}),'application/json')
                    result.update(http_status=code,final_url=current,raw_uri=raw,content_sha256=digest,fetched_at=now())
                    result['stored_bytes']+=len(compressed)
                    event.update(http_status=code,response_metadata_uri=meta,raw_uri=raw)
                    if code in (301,302,303,307,308) and headers.get('Location'):
                        current=urljoin(current,headers['Location']);continue
                    content_type=headers.get('Content-Type','').lower()
                    if large:result['status']='needs_inspection';result['error']='Response exceeded size/time limit; stored body is partial'
                    elif code==429:result['status']='rate_limited'
                    elif code in (401,403):result['status']='blocked'
                    elif code in (404,410):result['status']='unavailable'
                    elif code>=500:result['status']='temporary_error'
                    elif code!=200:result['status']='needs_inspection'
                    elif 'html' not in content_type:result['status']='needs_inspection';result['error']='Non-HTML response'
                    else:
                        html=body.decode('utf-8','replace')
                        import re
                        title=re.search(r'<title[^>]*>(.*?)</title>',html,re.I|re.S)
                        title=title.group(1).lower() if title else ''
                        if any(x in title for x in ('just a moment','access denied','attention required','verify you are human','captcha')):result['status']='blocked'
                        elif any(x in title for x in ('page not found','404 not found','page cannot be found')):result['status']='needs_inspection';result['error']='Possible soft 404'
                        elif urlsplit(current).path in ('','/') and urlsplit(url).path not in ('','/'):
                            result['status']='needs_inspection';result['error']='Redirected to homepage'
                        elif not self.parse_initial:
                            result['status']='retrieved'
                        else:
                            import trafilatura
                            text=trafilatura.extract(body,url=current,include_comments=False) or ''
                            if text:
                                data=gzip.compress(text.encode(),mtime=0)
                                result['text_uri']=self.put('text/'+article_id+'/'+digest+'.txt.gz',data,'application/gzip');result['stored_bytes']+=len(data)
                            result['status']='saved' if len(text)>=400 else 'needs_inspection'
                            if len(text)<400:result['error']='Too little extracted article text'
                    break
                else:result['status']='needs_inspection';result['error']='Too many redirects'
                event['status']=result['status']
                retry=result['status'] in ('rate_limited','temporary_error')
                wait=retry_seconds(headers.get('Retry-After'),attempt) if retry else 0
            except (requests.RequestException,socket.gaierror,TimeoutError) as e:
                result.update(status='temporary_error',error=type(e).__name__+': '+str(e)[:250]);event['status']=result['status'];retry=True;wait=getattr(e,'retry_after_seconds',retry_seconds(None,attempt))
            except ValueError as e:
                result.update(status='needs_inspection',error=str(e));event['status']=result['status'];retry=False;wait=0
            event['finished_at']=now();result['attempts'].append(event)
            if retry and attempt<self.max_attempts and wait<=120:
                event['retry_after_seconds']=wait;time.sleep(wait);continue
            if retry:
                result['retry_after_seconds']=wait
                if attempt==self.max_attempts:result['error']=(result.get('error') or '')+'; maximum attempts reached'
            break
        result['updated_at']=now()
        # This also permits reuse after a crash before the run checkpoint is flushed.
        self.put('latest/'+article_id+'.json',json.dumps(result),'application/json')
        return result
