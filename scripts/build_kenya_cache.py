"""Export all Kenya-origin candidates, without limits, preserving daily evidence."""
import gzip,hashlib,json,math
from pathlib import Path
from google.cloud import bigquery
ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'docs/data/kenya'
OUT.mkdir(parents=True,exist_ok=True)
SQL="""SELECT day,url,outlet,estimated_country,domain_country,countries,specific_location_countries,
 ARRAY(SELECT DISTINCT SPLIT(loc,'#')[SAFE_OFFSET(1)]
 FROM UNNEST(raw_locations) raw,UNNEST(SPLIT(raw,';')) loc
 WHERE SPLIT(loc,'#')[SAFE_OFFSET(2)]='KE' AND SPLIT(loc,'#')[SAFE_OFFSET(0)] IN ('2','3','4','5')
 ORDER BY 1) places
 FROM `citygraph.softpower.china_articles`
 WHERE estimated_country='KE' OR domain_country='KE'
 ORDER BY day,url"""
client=bigquery.Client(project='citygraph')
dry=client.query(SQL,job_config=bigquery.QueryJobConfig(dry_run=True),location='US')
print('Dry run bytes:',dry.total_bytes_processed,flush=True)
cfg=bigquery.QueryJobConfig(maximum_bytes_billed=math.ceil(dry.total_bytes_processed*1.1/2**30)*2**30)
job=client.query(SQL,job_config=cfg,location='US');rows=[dict(r) for r in job.result()]
assert len(rows)==len({(r['day'],r['url']) for r in rows})
ke=[r for r in rows if r['estimated_country']=='KE']
assert len(ke)==86226 and len({r['url'] for r in ke})==85993
fields=['day','url','outlet','estimated_country','domain_country','countries','specific_location_countries','places']
payload=json.dumps({'fields':fields,'rows':[[str(r[k]) if k=='day' else r[k] for k in fields] for r in rows]},ensure_ascii=False,separators=(',',':')).encode()
digest=hashlib.sha256(payload).hexdigest();name='articles-'+digest[:12]+'.json'
(OUT/name).write_bytes(payload);(OUT/(name+'.gz')).write_bytes(gzip.compress(payload,compresslevel=9,mtime=0))
manifest={'schema_version':1,'table':'citygraph.softpower.china_articles','start':'2015-01-01','end':'2025-12-31','rows':len(rows),'gdelt_kenya_rows':len(ke),'gdelt_kenya_unique_urls':85993,'json':name,'gzip':name+'.gz','sha256':digest,'json_bytes':len(payload),'gzip_bytes':(OUT/(name+'.gz')).stat().st_size,'job_id':job.job_id,'bytes_billed':job.total_bytes_billed}
(OUT/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print(json.dumps(manifest,indent=2),flush=True)
