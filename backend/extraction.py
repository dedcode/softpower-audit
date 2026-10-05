"""Public progress only. Does not query BigQuery or expose stored originals."""
import json,logging,threading,time
from fastapi import APIRouter,HTTPException,Query
from google.cloud import storage
from google.api_core.exceptions import NotFound
router=APIRouter();cache={};lock=threading.Lock()
@router.get('/extraction-status')
def status(country:str=Query('KE',pattern='^[A-Z]{2}$')):
 with lock:
  if country in cache and time.monotonic()-cache[country][0]<15:return cache[country][1]
  try:result=json.loads(storage.Client().bucket('citygraph-softpower-crawl').blob('progress/'+country+'.json').download_as_text(timeout=15))
  except NotFound:return {'country':country,'state':'not_started','updated_at':None}
  except Exception:
   logging.exception('Progress snapshot unavailable');raise HTTPException(503,'Progress is temporarily unavailable; this does not mean the crawler stopped.')
  result={k:v for k,v in result.items() if k in ('country','run_id','phase','state','updated_at','total','processed','pending','downloading','counts','domains','active_stages','error','instances','active_instances','article_slots')}
  cache[country]=(time.monotonic(),result);return result
