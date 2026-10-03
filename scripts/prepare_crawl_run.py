"""Prepare a generic country run. No crawling; --apply uploads inputs after approval."""
import argparse,gzip,hashlib,json,re
from collections import defaultdict
from datetime import date,datetime,timezone
from pathlib import Path
from google.cloud import storage,bigquery
ROOT=Path(__file__).resolve().parents[1]
def arguments():
 p=argparse.ArgumentParser();p.add_argument('--country',required=True);p.add_argument('--start',default='2015-01-01');p.add_argument('--end',default='2025-12-31');p.add_argument('--source-table',default='citygraph.softpower.china_articles');p.add_argument('--pilot',action='store_true');p.add_argument('--apply',action='store_true');return p.parse_args()
def main():
 args=arguments();assert re.fullmatch('[A-Z]{2}',args.country);assert re.fullmatch(r'[a-zA-Z0-9_-]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+',args.source_table);assert date.fromisoformat(args.start)<=date.fromisoformat(args.end)
 phase='pilot' if args.pilot else 'full';run_id=args.country.lower()+'-'+phase+'-'+datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
 cfg={'project':'citygraph','dataset':'citygraph.softpower_crawl','source_table':args.source_table,'run_id':run_id,'country':args.country,'phase':phase,'start':args.start,'end':args.end,'workers':6,'delay_seconds':3,'max_attempts':3,'max_runtime_seconds':1200 if args.pilot else 82800,'max_response_bytes':100*2**20 if args.pilot else 40*2**30,'max_total_attempts':100 if args.pilot else 150000}
 print(json.dumps(cfg,indent=2),flush=True)
 if not args.apply:return
 bq=bigquery.Client(project='citygraph')
 sql=f'''SELECT url,outlet,MIN(day) first_observed,MAX(day) last_observed FROM `{args.source_table}`
 WHERE estimated_country=@country AND day>=@start AND day<=@end GROUP BY url,outlet'''
 job=bq.query(sql,job_config=bigquery.QueryJobConfig(maximum_bytes_billed=16*2**30,query_parameters=[bigquery.ScalarQueryParameter('country','STRING',args.country),bigquery.ScalarQueryParameter('start','DATE',args.start),bigquery.ScalarQueryParameter('end','DATE',args.end)]),location='US')
 rows=[{**dict(r),'first_observed':str(r.first_observed),'last_observed':str(r.last_observed),'source_table':args.source_table} for r in job.result()]
 if args.pilot:
  domains=defaultdict(list)
  for r in rows:domains[r['outlet']].append(r)
  selected=sorted(domains,key=lambda d:(-len(domains[d]),d))[:12]
  rows=[]
  for d in selected:
   candidates=sorted(domains[d],key=lambda r:(r['first_observed'],r['url']));rows.append(candidates[0])
   if len(candidates)>1:rows.append(candidates[-1])
 assert len(rows)==len({r['url'] for r in rows})
 cfg['input_count']=len(rows);cfg['input_query_job']=job.job_id
 bucket=storage.Client(project=cfg['project']).bucket('citygraph-softpower-crawl');prefix='runs/'+run_id+'/'
 bucket.blob(prefix+'inputs.json.gz').upload_from_string(gzip.compress(json.dumps(rows).encode(),mtime=0),content_type='application/gzip')
 bucket.blob(prefix+'config.json').upload_from_string(json.dumps(cfg),content_type='application/json')
 inputs=[{**r,'run_id':run_id,'country':args.country,'article_id':hashlib.sha256(r['url'].encode()).hexdigest()} for r in rows]
 bq.load_table_from_json(inputs,cfg['dataset']+'.crawl_inputs').result()
 progress={'country':args.country,'run_id':run_id,'phase':phase,'state':'queued','updated_at':datetime.now(timezone.utc).isoformat(),'total':len(rows),'processed':0,'pending':len(rows),'downloading':0,'counts':{},'domains':[],'attempts':0,'retries':0,'limits':cfg}
 bucket.blob('progress/'+args.country+'.json').upload_from_string(json.dumps(progress),content_type='application/json')
 bq.load_table_from_json([{'run_id':run_id,'country':args.country,'updated_at':progress['updated_at'],'state':'queued','config_json':json.dumps(cfg),'summary_json':json.dumps(progress)}],cfg['dataset']+'.crawl_run_events').result()
 private=ROOT/'deployment-private';private.mkdir(exist_ok=True);(private/'latest-crawl-run.json').write_text(json.dumps(cfg,indent=2))
 print('Prepared',run_id,len(rows),'URLs',flush=True)
if __name__=='__main__':main()
