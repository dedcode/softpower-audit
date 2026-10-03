"""Read-only end-to-end verification of a completed toolbox pilot."""
import argparse,gzip,hashlib,json
from collections import Counter
from google.cloud import bigquery,storage
p=argparse.ArgumentParser();p.add_argument('--run-id',required=True);a=p.parse_args()
b=storage.Client(project='citygraph').bucket('citygraph-softpower-crawl');prefix='runs/'+a.run_id+'/'
cfg=json.loads(b.blob(prefix+'config.json').download_as_text());assert cfg['phase']=='pilot'
inputs=gzip.decompress(b.blob(prefix+'inputs.json.gz').download_as_bytes())
parent=gzip.decompress(b.blob('runs/'+cfg['parent_run']+'/inputs.json.gz').download_as_bytes());assert inputs==parent,'Pilot input changed'
rows=list(bigquery.Client(project='citygraph').query('SELECT * FROM `citygraph.softpower_crawl.crawl_results` WHERE run_id=@run',job_config=bigquery.QueryJobConfig(maximum_bytes_billed=100*2**20,query_parameters=[bigquery.ScalarQueryParameter('run','STRING',a.run_id)])).result())
assert len(rows)==len(json.loads(inputs)), 'Run not complete or results missing'
for r in rows:
 events=json.loads(r.attempts_json);assert events[-1]['stage']=='final';assert events[-1]['status']==r.status
 stages={e['stage'] for e in events}
 if r.status in ('partial','exhausted'):assert {'http','publisher_url_discovery','browser','archive_lookup'}<=stages,'Premature terminal result'
 if r.status=='saved':
  assert r.text_uri and r.raw_uri
  def read(uri):return gzip.decompress(b.blob(uri.split('/'+b.name+'/')[1]).download_as_bytes())
  text=read(r.text_uri);raw=read(r.raw_uri);assert len(text.decode())>=400;assert hashlib.sha256(raw).hexdigest()==r.content_sha256
print(json.dumps({'run_id':a.run_id,'identical_pilot_inputs':True,'rows':len(rows),'outcomes':dict(Counter(r.status for r in rows)),'saved_text_and_original_hashes_verified':True,'terminal_stage_coverage_verified':True},indent=2))
