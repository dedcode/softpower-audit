"""Restart exactly the existing pilot inputs; never resample or scan source corpus."""
import argparse,gzip,json,hashlib
from datetime import datetime,timezone
from google.cloud import storage,bigquery
p=argparse.ArgumentParser();p.add_argument('--from-run',required=True);p.add_argument('--apply',action='store_true');a=p.parse_args()
b=storage.Client(project='citygraph').bucket('citygraph-softpower-crawl');old=json.loads(b.blob('runs/'+a.from_run+'/config.json').download_as_text())
if old['phase']!='pilot':raise SystemExit('Only pilot runs can be restarted by this command')
inputs=b.blob('runs/'+a.from_run+'/inputs.json.gz').download_as_bytes();rows=json.loads(gzip.decompress(inputs));assert 0<len(rows)<=24
run=old['country'].lower()+'-toolbox-'+datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
cfg={**old,'run_id':run,'parent_run':a.from_run,'workers':3,'max_runtime_seconds':1200,'max_total_attempts':1000,'max_response_bytes':100*2**20,'toolbox_version':'toolbox-2'}
print(json.dumps(cfg,indent=2))
if a.apply:
 b.blob('runs/'+run+'/inputs.json.gz').upload_from_string(inputs,content_type='application/gzip');b.blob('runs/'+run+'/config.json').upload_from_string(json.dumps(cfg),content_type='application/json')
 client=bigquery.Client(project='citygraph');items=[{**r,'run_id':run,'country':cfg['country'],'article_id':hashlib.sha256(r['url'].encode()).hexdigest()} for r in rows]
 client.load_table_from_json(items,cfg['dataset']+'.crawl_inputs').result()
 progress={'country':cfg['country'],'run_id':run,'phase':'pilot','state':'queued','updated_at':datetime.now(timezone.utc).isoformat(),'total':len(rows),'processed':0,'pending':len(rows),'downloading':0,'counts':{},'limits':cfg,'toolbox_version':'toolbox-2'}
 b.blob('progress/'+cfg['country']+'.json').upload_from_string(json.dumps(progress),content_type='application/json')
 print('RUN_ID='+run)
