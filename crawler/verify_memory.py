"""Bounded cloud verification: saved originals, local JS fixture, compact index."""
import gc,gzip,hashlib,json,os,threading,time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from google.cloud import storage
from isolation import extract_isolated,render_isolated,IsolationError
from result_store import ResultIndex,memory_usage

def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True).encode()).hexdigest()

def main():
    bucket=storage.Client().bucket(os.environ['CRAWL_BUCKET']);prefix='verification/memory-v1/'
    samples=[];done=threading.Event()
    def sample():
        while not done.wait(.1):samples.append(memory_usage().get('current_bytes',0))
    thread=threading.Thread(target=sample,daemon=True);thread.start()
    report={'started_memory':memory_usage()}
    try:
        inputs=json.loads(bucket.blob(prefix+'inputs.json').download_as_text())
        for row in inputs:
            raw=gzip.decompress(bucket.blob(row['raw_uri'].split('/'+bucket.name+'/',1)[1]).download_as_bytes())
            result=extract_isolated(raw,row['url']);assert digest(result)==row['expected'],row['url']
        report['identical_saved_page_outputs']=len(inputs)
        index=ResultIndex()
        for i in range(100000):
            index.record({'article_id':str(i),'updated_at':'2026-10-04T00:00:00+00:00','status':'saved','outlet':'example.org','attempts':[{'http_attempts':[{'body':'x'*100000}]}]})
        assert len(index)==100000
        report['compact_index_entries']=len(index);report['memory_with_100k_results']=memory_usage()
        class H(BaseHTTPRequestHandler):
            def do_GET(self):
                body=b'<html><article id="a"></article><script>document.getElementById("a").innerHTML="<p>"+"Reporting about investment and cooperation. ".repeat(30)+"</p>";</script></html>'
                self.send_response(200);self.send_header('Content-Type','text/html');self.end_headers();self.wfile.write(body)
            def log_message(self,*args):pass
        server=ThreadingHTTPServer(('127.0.0.1',0),H);threading.Thread(target=server.serve_forever,daemon=True).start();url='http://127.0.0.1:'+str(server.server_port)+'/article'
        try:
            for _ in range(3):
                body,source=render_isolated(url,lambda u,d:u==url)
                result=extract_isolated(body,source);assert result['quality']=='candidate';assert result['text'].count('Reporting about investment and cooperation.')==30
            report['javascript_render_repetitions']=3
            try:render_isolated(url,lambda u,d:False)
            except IsolationError:report['request_denial_preserved']=True
            else:raise AssertionError('Browser bypassed request approval')
        finally:server.shutdown()
        del index;gc.collect();report['final_memory']=memory_usage();report['passed']=True
    except Exception as e:
        report['passed']=False;report['error']=repr(e);raise
    finally:
        done.set();thread.join(timeout=2);report['sampled_peak_bytes']=max(samples,default=0)
        bucket.blob(prefix+'result.json').upload_from_string(json.dumps(report),content_type='application/json');print(json.dumps(report),flush=True)
