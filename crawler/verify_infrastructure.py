"""Isolated cloud fixture: one confirmed failure, then successful completion.

No article requests, Firestore writes or production progress/lease changes.
"""
import json,os,re
from datetime import datetime,timezone
from google.cloud import storage
from google.api_core.exceptions import NotFound


def main():
    run_id=os.environ['RUN_ID']
    if not re.fullmatch(r'verify-infrastructure-[A-Za-z0-9._-]+',run_id):
        raise ValueError('Infrastructure fixture requires an isolated verification run ID')
    mode=os.environ.get('VERIFY_INFRASTRUCTURE_MODE','temporary')
    if mode not in ('temporary','permanent','always'):
        raise ValueError('Unknown infrastructure verification mode')
    bucket=storage.Client().bucket(os.environ['CRAWL_BUCKET'])
    prefix='runs/'+run_id+'/'
    blob=bucket.blob(prefix+'verification.json')
    try:
        blob.reload();generation=blob.generation
        executions=json.loads(blob.download_as_text(if_generation_match=generation))
    except NotFound:
        generation=0;executions=[]
    execution=os.environ['CLOUD_RUN_EXECUTION']
    if execution in executions:raise RuntimeError('Unexpected native fixture retry')
    executions.append(execution)
    if len(executions)>4:raise RuntimeError('Coordinator exceeded the infrastructure restart limit')
    complete=len(executions)>1 and mode=='temporary'
    summary={'country':'ZZ','phase':'verification','run_id':run_id,'execution':execution,
        'state':'completed' if complete else 'failed','infrastructure_recoverable':mode!='permanent',
        'total':1,'processed':int(complete),'pending':int(not complete),'downloading':0,
        'updated_at':datetime.now(timezone.utc).isoformat(),
        'error':None if complete else 'Simulated infrastructure verification failure'}
    blob.upload_from_string(json.dumps(executions),content_type='application/json',if_generation_match=generation)
    bucket.blob(prefix+'progress.json').upload_from_string(json.dumps(summary),content_type='application/json')
    print(json.dumps(summary),flush=True)
    if not complete:raise SystemExit(1)


if __name__=='__main__':main()
