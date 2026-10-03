"""Cloud provisioning plan. Only --apply makes changes; requires approval."""
import argparse,json
import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage,bigquery
PROJECT='citygraph';BUCKET=PROJECT+'-softpower-crawl';DATASET=PROJECT+'.softpower_crawl'
SA='softpower-crawler@'+PROJECT+'.iam.gserviceaccount.com'
PLAN={'bucket':BUCKET,'bucket_location':'US','dataset':DATASET,'service_account':SA,'worker_permissions':{'project':'roles/bigquery.jobUser','new_bucket':'roles/storage.objectAdmin','new_dataset':'WRITER'},'existing_api_permission':{'identity':'softpower-audit-reader@citygraph.iam.gserviceaccount.com','role':'roles/storage.objectViewer','scope':'progress/ objects only in the new bucket'},'public_access':'No public bucket access; only derived progress is served through the existing API'}
def apply():
 credentials,_=google.auth.default();session=AuthorizedSession(credentials)
 r=session.get(f'https://iam.googleapis.com/v1/projects/{PROJECT}/serviceAccounts/{SA}')
 if r.status_code==404:r=session.post(f'https://iam.googleapis.com/v1/projects/{PROJECT}/serviceAccounts',json={'accountId':'softpower-crawler','serviceAccount':{'displayName':'Article collection worker'}})
 r.raise_for_status()
 url=f'https://cloudresourcemanager.googleapis.com/v1/projects/{PROJECT}'
 r=session.post(url+':getIamPolicy',json={});r.raise_for_status();policy=r.json();member='serviceAccount:'+SA
 b=next((b for b in policy['bindings'] if b['role']=='roles/bigquery.jobUser' and not b.get('condition')),None)
 if b is None:b={'role':'roles/bigquery.jobUser','members':[]};policy['bindings'].append(b)
 if member not in b['members']:b['members'].append(member)
 r=session.post(url+':setIamPolicy',json={'policy':policy});r.raise_for_status()
 sc=storage.Client(project=PROJECT);bucket=sc.lookup_bucket(BUCKET)
 if bucket is None:
  bucket=sc.bucket(BUCKET);bucket.iam_configuration.uniform_bucket_level_access_enabled=True;bucket.iam_configuration.public_access_prevention='enforced';bucket=sc.create_bucket(bucket,location='US')
 policy=bucket.get_iam_policy(requested_policy_version=3);policy.version=3
 bindings=[{'role':'roles/storage.objectAdmin','members':{member}},{'role':'roles/storage.objectViewer','members':{'serviceAccount:softpower-audit-reader@citygraph.iam.gserviceaccount.com'},'condition':{'title':'Progress snapshots only','expression':f'resource.name.startsWith("projects/_/buckets/{BUCKET}/objects/progress/")'}}]
 for binding in bindings:
  if not any(b['role']==binding['role'] and b.get('condition')==binding.get('condition') and set(binding['members'])<=set(b['members']) for b in policy.bindings):policy.bindings.append(binding)
 bucket.set_iam_policy(policy)
 bq=bigquery.Client(project=PROJECT);ds=bigquery.Dataset(DATASET);ds.location='US';ds=bq.create_dataset(ds,exists_ok=True)
 entries=list(ds.access_entries);entry=bigquery.AccessEntry('WRITER','userByEmail',SA)
 if entry not in entries:entries.append(entry);ds.access_entries=entries;bq.update_dataset(ds,['access_entries'])
 SF=bigquery.SchemaField
 fields=[SF(n,t) for n,t in [('run_id','STRING'),('country','STRING'),('article_id','STRING'),('url','STRING'),('outlet','STRING'),('source_table','STRING'),('first_observed','DATE'),('last_observed','DATE'),('updated_at','TIMESTAMP'),('fetched_at','TIMESTAMP'),('status','STRING'),('http_status','INTEGER'),('final_url','STRING'),('raw_uri','STRING'),('text_uri','STRING'),('robots_uri','STRING'),('content_sha256','STRING'),('response_bytes','INTEGER'),('stored_bytes','INTEGER'),('error','STRING'),('reused','BOOLEAN'),('attempts_json','STRING'),('retry_after_seconds','FLOAT')]]
 for name,schema in [('crawl_result_events',fields),('crawl_run_events',[SF('run_id','STRING'),SF('country','STRING'),SF('updated_at','TIMESTAMP'),SF('state','STRING'),SF('config_json','STRING'),SF('summary_json','STRING')]),('crawl_inputs',[SF(n,t) for n,t in [('run_id','STRING'),('country','STRING'),('article_id','STRING'),('url','STRING'),('outlet','STRING'),('source_table','STRING'),('first_observed','DATE'),('last_observed','DATE')]])]:
  table=bigquery.Table(DATASET+'.'+name,schema=schema)
  if name!='crawl_inputs':table.time_partitioning=bigquery.TimePartitioning(field='updated_at')
  table.clustering_fields=['country','run_id'];bq.create_table(table,exists_ok=True)
 for name,query in [('crawl_results',f'SELECT * FROM `{DATASET}.crawl_result_events` QUALIFY ROW_NUMBER() OVER(PARTITION BY run_id,article_id ORDER BY updated_at DESC)=1'),('crawl_runs',f'SELECT * FROM `{DATASET}.crawl_run_events` QUALIFY ROW_NUMBER() OVER(PARTITION BY run_id ORDER BY updated_at DESC)=1')]:
  table=bigquery.Table(DATASET+'.'+name);table.view_query=query;bq.create_table(table,exists_ok=True)
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--apply',action='store_true');args=parser.parse_args();print(json.dumps(PLAN,indent=2))
 if args.apply:apply()
