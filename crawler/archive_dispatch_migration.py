"""Index unfinished archive work only under a verified stopped-cohort handover."""
import gzip,json,os,re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
from urllib.parse import urlsplit
from google.cloud import firestore,storage
from archive_dispatch import archive_service
from shared_queue import SharedQueue

def route_fields(doc,envelope,run_id,old_execution):
    if (envelope.get('run_id')!=run_id or envelope.get('article_id')!=doc['article_id']
            or envelope.get('phase')!='archive' or doc.get('phase')!='archive'
            or doc.get('state') not in ('ready','leased')):
        raise ValueError('Archive checkpoint identity or phase does not match queue')
    fields={'archive_service':archive_service(envelope['checkpoint'],doc['item'])}
    if doc['state']=='leased':
        if not str(doc.get('owner','')).startswith(old_execution+'-'):
            raise ValueError('A different cohort owns an article; refusing migration')
        fields.update(state='ready',archive_due_at=0.,owner=firestore.DELETE_FIELD,
                      claim_token=firestore.DELETE_FIELD)
    return fields

def main():
    run_id=os.environ['RUN_ID'];token=os.environ['HANDOVER_ID'];old=os.environ['OLD_EXECUTION']
    for value in (run_id,token,old):
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*',value):raise ValueError('Invalid migration identity')
    bucket=storage.Client(project='citygraph').bucket(os.environ['CRAWL_BUCKET']);prefix='runs/'+run_id+'/'
    proof=json.loads(bucket.blob(prefix+'diagnostics/archive-dispatch/handover.json').download_as_text(timeout=30))
    if (proof.get('run_id')!=run_id or proof.get('handover_id')!=token or proof.get('old_execution')!=old
            or not proof.get('old_completion_time') or proof.get('old_running_count')!=0
            or proof.get('coordinator_state') not in ('CANCELLED','SUCCEEDED','FAILED')):
        raise RuntimeError('Confirmed stopped execution and coordinator proof required')
    def guard():
        blob=bucket.blob('control/worker-lease.json');blob.reload(timeout=30)
        lease=json.loads(blob.download_as_text(if_generation_match=blob.generation,timeout=30))
        if (lease.get('run_id')!=run_id or lease.get('handover_id')!=token
                or lease.get('previous_execution')!=old or lease.get('owner')!='handover-'+token
                or lease.get('execution')!='handover-'+token or lease.get('distributed') is not True
                or datetime.fromisoformat(lease['expires_at'])<=datetime.now(timezone.utc)):
            raise RuntimeError('Migration handover ownership was lost')
    guard();client=firestore.Client(project='citygraph',database='softpower-crawl')
    q=SharedQueue(client,run_id,'archive-migration-'+token);before=q.summary()
    snapshots=list(q.articles.where(filter=firestore.FieldFilter('archive_due_at','>=',0.)).stream(timeout=300))
    if len(snapshots)!=before['archive_remaining']:raise RuntimeError('Archive counter/input count mismatch')
    def prepare(snapshot):
        doc=snapshot.to_dict();u=urlsplit(doc['checkpoint_uri'])
        if (u.scheme!='gs' or u.netloc!=bucket.name
                or not u.path.startswith('/'+prefix+'distributed/checkpoints/'+doc['article_id']+'/')):
            raise ValueError('Unexpected checkpoint evidence location')
        envelope=json.loads(gzip.decompress(bucket.blob(u.path.lstrip('/')).download_as_bytes(timeout=30)))
        return snapshot,route_fields(doc,envelope,run_id,old)
    counts=Counter();released=0;batch=client.batch();pending=0
    with ThreadPoolExecutor(max_workers=12) as pool:
        for snapshot,fields in pool.map(prepare,snapshots):
            counts[fields['archive_service']]+=1;released+=int(fields.get('state')=='ready')
            batch.update(snapshot.reference,fields,option=client.write_option(last_update_time=snapshot.update_time))
            pending+=1
            if pending==250:
                guard();batch.commit(timeout=60);batch=client.batch();pending=0
    if pending:guard();batch.commit(timeout=60)
    # Publisher claims held by the now-terminated cohort are also immediately
    # reusable. Unclaimed URLs and all terminal results remain unchanged.
    for snapshot in q.articles.where(filter=firestore.FieldFilter('due_at','>=',0.)).stream(timeout=60):
        doc=snapshot.to_dict()
        if doc['state']=='leased':
            if not str(doc.get('owner','')).startswith(old+'-'):raise RuntimeError('Different publisher owner')
            guard();snapshot.reference.update({'state':'ready','due_at':0.,'owner':firestore.DELETE_FIELD,
                'claim_token':firestore.DELETE_FIELD},option=client.write_option(last_update_time=snapshot.update_time),timeout=30)
    guard();after=q.summary()
    for field in ('total','processed','counts','archive_remaining','browser_remaining',
                  'attempts','retries','response_bytes','stored_bytes'):
        if after[field]!=before[field]:raise RuntimeError('Migration changed result accounting: '+field)
    report={'run_id':run_id,'handover_id':token,'archive_rows':len(snapshots),'routes':dict(counts),
            'released_archive_claims':released,'processed_before':before['processed'],'processed_after':after['processed'],
            'counts':after['counts'],'execution':os.environ.get('CLOUD_RUN_EXECUTION')}
    bucket.blob(prefix+'diagnostics/archive-dispatch/migration.json').upload_from_string(json.dumps(report),
        content_type='application/json',if_generation_match=0,timeout=60)
    print(json.dumps(report),flush=True)

if __name__=='__main__':main()
