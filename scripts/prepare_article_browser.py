"""Grant the existing API identity read access to the article table; validate queries."""
import sys,json
from pathlib import Path
from datetime import date
from google.cloud import bigquery
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'backend'))
import articles
client=bigquery.Client(project='citygraph')
policy=client.get_iam_policy(articles.TABLE)
member='serviceAccount:softpower-audit-reader@citygraph.iam.gserviceaccount.com'
if not any(b['role']=='roles/bigquery.dataViewer' and member in b['members'] for b in policy.bindings):
 policy.bindings.append({'role':'roles/bigquery.dataViewer','members':{member}});client.set_iam_policy(articles.TABLE,policy)
print('Article table read permission ready.',flush=True)
q=f"SELECT DISTINCT code FROM `{articles.TABLE}`, UNNEST([estimated_country,domain_country]) code WHERE code IS NOT NULL AND code!='CH' ORDER BY code"
job=client.query(q,job_config=bigquery.QueryJobConfig(maximum_bytes_billed=4*2**30),location='US');codes=[r.code for r in job.result()]
catalog={**articles.CATALOG,'countries':codes}
for p in [ROOT/'backend/article_catalog.json',ROOT/'docs/article-catalog.json']:p.write_text(json.dumps(catalog,ensure_ascii=False))
print('Catalog countries:',len(codes),flush=True)
for label,q in [('overview',articles.overview_sql('broad','gdelt')),('stories',articles.stories_sql('broad','gdelt'))]:
 cfg=bigquery.QueryJobConfig(maximum_bytes_billed=16*2**30,query_parameters=articles.params('KE',date(2025,1,1),date(2025,1,31),False,''))
 job=client.query(q,job_config=cfg,location='US');rows=[dict(r) for r in job.result()]
 (ROOT/'deployment-private'/('article_'+label+'.json')).write_text(json.dumps(rows,default=str))
 if label=='overview':assert sum(r['count'] for r in rows if r['kind']=='outlet')==429
 else:assert len(rows)==20 and all(r['latest']['estimated_country']=='KE' for r in rows)
 print(label,'passed; billed bytes',job.total_bytes_billed,flush=True)
