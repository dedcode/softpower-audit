"""Export complete country caches in bounded, content-addressed static chunks."""
import argparse,gzip,hashlib,json,math
from pathlib import Path
from google.cloud import bigquery
ROOT=Path(__file__).resolve().parents[1]
COUNTRIES={'CI':'chile','RP':'philippines'}
FIELDS=['day','url','outlet','estimated_country','domain_country','countries','specific_location_countries','places']
SQL="""SELECT source_country,day,url,outlet,estimated_country,domain_country,countries,specific_location_countries,
 ARRAY(SELECT DISTINCT SPLIT(loc,'#')[SAFE_OFFSET(1)]
 FROM UNNEST(raw_locations) raw,UNNEST(SPLIT(raw,';')) loc
 WHERE SPLIT(loc,'#')[SAFE_OFFSET(2)]=source_country AND SPLIT(loc,'#')[SAFE_OFFSET(0)] IN ('2','3','4','5')
 ORDER BY 1) places
 FROM `citygraph.softpower.china_articles`,UNNEST(@source_countries) source_country
 WHERE (estimated_country IN UNNEST(@source_countries) OR domain_country IN UNNEST(@source_countries))
 AND (estimated_country=source_country OR domain_country=source_country)
 ORDER BY source_country,day,url"""
def build(countries):
 client=bigquery.Client(project='citygraph');params=[bigquery.ArrayQueryParameter('source_countries','STRING',countries)]
 dry=client.query(SQL,job_config=bigquery.QueryJobConfig(dry_run=True,query_parameters=params),location='US')
 cap=math.ceil(dry.total_bytes_processed*1.1/2**30)*2**30
 print(json.dumps({'dry_run_bytes':dry.total_bytes_processed,'maximum_bytes_billed':cap}),flush=True)
 job=client.query(SQL,job_config=bigquery.QueryJobConfig(query_parameters=params,maximum_bytes_billed=cap),location='US')
 results=job.result();states={}
 for code in countries:
  out=ROOT/'docs/data'/COUNTRIES[code];out.mkdir(parents=True,exist_ok=True)
  states[code]={'out':out,'rows':0,'gdelt_rows':0,'urls':set(),'gdelt_urls':set(),'outlets':set(),'parts':[],'chunk':[],'bytes':0,'previous':None}
 def flush(code):
  s=states[code]
  if not s['chunk']:return
  payload=json.dumps({'country':code,'fields':FIELDS,'rows':s['chunk']},ensure_ascii=False,separators=(',',':')).encode()
  digest=hashlib.sha256(payload).hexdigest();name='articles-'+digest[:12]+'.json';compressed=gzip.compress(payload,compresslevel=9,mtime=0)
  (s['out']/name).write_bytes(payload);(s['out']/(name+'.gz')).write_bytes(compressed)
  s['parts'].append({'json':name,'gzip':name+'.gz','rows':len(s['chunk']),'sha256':digest,'json_bytes':len(payload),'gzip_bytes':len(compressed)})
  s['chunk']=[];s['bytes']=0
 for row in results:
  code=row['source_country'];s=states[code];key=(str(row['day']),row['url'])
  assert s['previous'] is None or key>s['previous'],'Duplicate or unordered daily URL';s['previous']=key
  values=[str(row[k]) if k=='day' else row[k] for k in FIELDS];size=len(json.dumps(values,ensure_ascii=False,separators=(',',':')).encode())+1
  if s['bytes']+size>4*1024*1024:flush(code)
  s['chunk'].append(values);s['bytes']+=size;s['rows']+=1;s['urls'].add(row['url']);s['outlets'].add(row['outlet'])
  if row['estimated_country']==code:s['gdelt_rows']+=1;s['gdelt_urls'].add(row['url'])
 assert sum(s['rows'] for s in states.values())==results.total_rows
 for code,s in states.items():
  flush(code);assert sum(p['rows'] for p in s['parts'])==s['rows']
  m={'schema_version':2,'country':code,'table':'citygraph.softpower.china_articles','start':'2015-01-01','end':'2025-12-31','rows':s['rows'],'unique_urls':len(s['urls']),'websites':len(s['outlets']),'gdelt_rows':s['gdelt_rows'],'gdelt_unique_urls':len(s['gdelt_urls']),'parts':s['parts'],'job_id':job.job_id,'bytes_billed':job.total_bytes_billed}
  (s['out']/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');print(json.dumps(m),flush=True)
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--countries',nargs='+',choices=COUNTRIES,default=['CI','RP']);build(p.parse_args().countries)
