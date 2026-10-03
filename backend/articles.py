"""Country-selectable article browser over the completed metadata extraction."""
import json
import logging
import time
from datetime import date, timedelta
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from google.cloud import bigquery

router=APIRouter()
TABLE='citygraph.softpower.china_articles'
CATALOG=json.loads(Path(__file__).with_name('article_catalog.json').read_text())
MODES={'broad':'TRUE','both':"@country IN UNNEST(countries)",'only_pair':"@country IN UNNEST(countries) AND ARRAY_LENGTH(countries)=2"}
ORIGINS={'gdelt':'estimated_country=@country','cctld':'domain_country=@country','both':'estimated_country=@country AND domain_country=@country'}

def selection(country,start,end,mode,origin):
    if country not in CATALOG['countries'] or country=='CH':raise HTTPException(422,'Choose a listed source country.')
    if not date(2015,1,1)<=start<=end<=date(2025,12,31):raise HTTPException(422,'Choose ordered dates within 2015–2025.')
    if mode not in MODES or origin not in ORIGINS:raise HTTPException(422,'Invalid geographic or country-source filter.')

def base_sql(mode,origin,dated=False):
    return f"""SELECT day,url,outlet,estimated_country,domain_country,countries,specific_location_countries
    FROM `{TABLE}` WHERE {ORIGINS[origin]} AND ({MODES[mode]})
    AND (NOT @place OR @country IN UNNEST(specific_location_countries))
    {'AND day>=@start AND day<@end' if dated else ''}"""

def overview_sql(mode,origin):
    return f"""WITH base AS ({base_sql(mode,origin)})
    SELECT 'outlet' kind,outlet,CAST(NULL AS DATE) bucket,COUNT(DISTINCT url) count
    FROM base WHERE day>=@start AND day<@end GROUP BY outlet
    UNION ALL
    SELECT 'timeline',CAST(NULL AS STRING),DATE_TRUNC(day,MONTH),COUNT(DISTINCT url)
    FROM base WHERE (@outlet='' OR outlet=@outlet) GROUP BY 3"""

def stories_sql(mode,origin):
    return f"""WITH base AS ({base_sql(mode,origin,True)})
    SELECT outlet,url,MIN(day) first_observed,MAX(day) last_observed,
      ARRAY_AGG(STRUCT(countries,specific_location_countries,estimated_country,domain_country) ORDER BY day DESC LIMIT 1)[OFFSET(0)] latest
    FROM base WHERE (@outlet='' OR outlet=@outlet)
    GROUP BY outlet,url ORDER BY last_observed DESC,url LIMIT 20 OFFSET @offset"""

def params(country,start,end,place,outlet,offset=0):
    return [bigquery.ScalarQueryParameter(n,t,v) for n,t,v in (
      ('country','STRING',country),('start','DATE',start),('end','DATE',end+timedelta(days=1)),
      ('place','BOOL',place),('outlet','STRING',outlet),('offset','INT64',offset))]

def run(key,sql,parameters,shape):
    # Share the established public-query concurrency, caching and spend controls.
    import main as service
    with service.lock:
        if key in service.cache:
            payload=service.cache[key];service.cache.move_to_end(key)
            return Response(payload,media_type='application/json',headers={'Cache-Control':'public, max-age=86400'})
    if not service.query_slot.acquire(blocking=False):raise HTTPException(429,'Another selection is loading. Please retry shortly.',headers={'Retry-After':'3'})
    try:
        with service.lock:
            now=time.monotonic()
            while service.recent_queries and now-service.recent_queries[0]>3600:service.recent_queries.popleft()
            if len(service.recent_queries)>=30:raise HTTPException(429,'The public query allowance is busy. Please retry later.',headers={'Retry-After':'120'})
            service.recent_queries.append(now)
        cfg=bigquery.QueryJobConfig(maximum_bytes_billed=service.MAX_BYTES,use_query_cache=True,query_parameters=parameters,labels={'app':'softpower-audit','purpose':'article-browser'})
        job=service.client().query(sql,job_config=cfg,location='US',timeout=30)
        try:result=shape([dict(r) for r in job.result(timeout=100)])
        except TimeoutError:
            job.cancel();raise HTTPException(504,'This selection took too long. Try a shorter date range.')
        payload=json.dumps(result,default=str,separators=(',',':')).encode()
        with service.lock:
            service.cache[key]=payload
            while sum(map(len,service.cache.values()))>service.CACHE_BYTES and len(service.cache)>1:service.cache.popitem(last=False)
        return Response(payload,media_type='application/json',headers={'Cache-Control':'public, max-age=86400'})
    except HTTPException:raise
    except Exception:
        logging.exception('Article query failed')
        raise HTTPException(503,'This selection could not be loaded. Try a shorter date range or retry.')
    finally:service.query_slot.release()

@router.get('/article-catalog')
def catalog():return CATALOG

@router.get('/browse')
def browse(country:str='KE',start:date=date(2015,1,1),end:date=date(2025,12,31),mode:str='broad',origin:str='gdelt',place:bool=False,outlet:str=Query('',max_length=253)):
    selection(country,start,end,mode,origin)
    def shape(rows):
        outlets=sorted([{'outlet':r['outlet'],'count':r['count']} for r in rows if r['kind']=='outlet'],key=lambda r:(-r['count'],r['outlet']))
        selected=[r for r in outlets if not outlet or r['outlet']==outlet]
        return {'outlets':outlets,'count':sum(r['count'] for r in selected),'websites':len(selected),
          'timeline':sorted([{'month':str(r['bucket']),'count':r['count']} for r in rows if r['kind']=='timeline'],key=lambda r:r['month'])}
    return run(('browse',country,str(start),str(end),mode,origin,place,outlet),overview_sql(mode,origin),params(country,start,end,place,outlet),shape)

@router.get('/stories')
def stories(country:str='KE',start:date=date(2015,1,1),end:date=date(2025,12,31),mode:str='broad',origin:str='gdelt',place:bool=False,outlet:str=Query('',max_length=253),page:int=Query(0,ge=0,le=5000000)):
    selection(country,start,end,mode,origin)
    return run(('stories',country,str(start),str(end),mode,origin,place,outlet,page),stories_sql(mode,origin),params(country,start,end,place,outlet,page*20),lambda rows:{'articles':[{**r.pop('latest'),**r} for r in rows]})

@router.get('/article-places')
def places(country:str,day:date,url:str=Query(...,max_length=20000),origin:str='gdelt'):
    selection(country,day,day,'broad',origin)
    sql=f"SELECT raw_locations FROM `{TABLE}` WHERE day=@day AND {ORIGINS[origin]} AND url=@url"
    parameters=[bigquery.ScalarQueryParameter(n,t,v) for n,t,v in [('country','STRING',country),('day','DATE',day),('url','STRING',url)]]
    def shape(rows):
        names=set()
        for row in rows:
            for raw in row['raw_locations']:
                for loc in raw.split(';'):
                    p=loc.split('#')
                    if len(p)>2 and p[2]==country and p[0] in ('2','3','4','5'):names.add(p[1])
        return {'places':sorted(names)}
    return run(('places',country,str(day),url,origin),sql,parameters,shape)
