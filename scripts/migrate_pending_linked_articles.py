"""Reanalyse pending stored teasers under an exclusive crawler handover.

Uses ordinary fenced claims/handoffs; never completes or resets an article.
The current publisher task and continuation must already be stopped.
"""
import argparse,gzip,json,sys,uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from google.cloud import storage,firestore
from google.cloud.firestore_v1.base_query import FieldFilter
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
from retrying import RetryingPipeline
from shared_queue import SharedQueue
from extractor_version import VERSION
from crawl_handover import validate_reservation
RID='ke-full-20261003-192939'
def eligible(checkpoint):
 p=checkpoint.get('pipeline') or {};best=p.get('best') or {}
 return (checkpoint.get('next_phase')=='publisher' and type(checkpoint.get('completed_passes')) is int
         and checkpoint['completed_passes']<2 and p.get('extractor_version')!=VERSION
         and best.get('quality')=='partial' and bool(best.get('raw_uri')))
def migrate(bucket,queue,handover_id,limit=2000):
 reservation=validate_reservation(bucket,RID,handover_id)
 snapshots=list(queue.articles.where(filter=FieldFilter('state','==','ready')).where(filter=FieldFilter('phase','==','publisher')).limit(limit+1).stream(timeout=30))
 if len(snapshots)>limit:raise RuntimeError('Pending scan exceeded bound; increase limit explicitly')
 def read(snapshot):
  row=snapshot.to_dict();uri=row.get('checkpoint_uri')
  if not uri:return None
  prefix='gs://'+bucket.name+'/runs/'+RID+'/distributed/checkpoints/'
  if not uri.startswith(prefix):raise RuntimeError('Unexpected checkpoint URI')
  envelope=json.loads(gzip.decompress(bucket.blob(uri.split('/'+bucket.name+'/',1)[1]).download_as_bytes(timeout=30)))
  if envelope.get('article_id')!=snapshot.id or envelope.get('run_id')!=RID:raise RuntimeError('Checkpoint identity mismatch')
  state=envelope['checkpoint'];return (snapshot,state) if eligible(state) else None
 selected=[]
 with ThreadPoolExecutor(max_workers=8) as pool:
  for offset in range(0,len(snapshots),32):
   current=validate_reservation(bucket,RID,handover_id)
   if current['generation']!=reservation['generation']:raise RuntimeError('Handover reservation changed')
   selected.extend(x for x in pool.map(read,snapshots[offset:offset+32]) if x is not None)
 worker=RetryingPipeline(bucket,RID,max_attempts=1);moved=[]
 for snapshot,state in selected:
  validate_reservation(bucket,RID,handover_id)
  row=snapshot.to_dict();result=worker.archive_first_handoff(row['item'],RID,'KE',checkpoint=state)
  if result is None:continue
  claims=queue._claim_batch([(snapshot.reference,uuid.uuid4().hex)],'publisher',Counter(),1)
  if not claims:continue
  claim=claims[0];path='runs/'+RID+'/distributed/checkpoints/'+claim.article_id+'/'+claim.token+'.json.gz'
  envelope={'article_id':claim.article_id,'run_id':RID,'phase':'archive','checkpoint':result['_checkpoint']}
  bucket.blob(path).upload_from_string(gzip.compress(json.dumps(envelope).encode(),mtime=0),content_type='application/gzip',if_generation_match=0,timeout=30)
  if not queue.handoff(claim,'gs://'+bucket.name+'/'+path,next_phase='archive',retry_at=result['retry_at'],result=result):raise RuntimeError('Fenced recovery handoff rejected')
  moved.append(claim.article_id)
 proof={'handover_id':handover_id,'scanned':len(snapshots),'eligible':len(selected),'moved':moved,'completion_unchanged':True}
 bucket.blob('runs/'+RID+'/diagnostics/linked-original/'+handover_id+'/migration.json').upload_from_string(json.dumps(proof),content_type='application/json')
 return proof
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--handover-id',required=True);args=parser.parse_args();b=storage.Client(project='citygraph').bucket('citygraph-softpower-crawl');q=SharedQueue(firestore.Client(project='citygraph',database='softpower-crawl'),RID,'operator-'+args.handover_id);print(json.dumps(migrate(b,q,args.handover_id)),flush=True)
