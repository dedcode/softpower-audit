"""Private no-network fixture for the cloud continuation workflow."""
import json,os
from datetime import datetime,timezone
from google.cloud import storage


def main():
    bucket=storage.Client().bucket(os.environ['CRAWL_BUCKET'])
    run_id=os.environ['RUN_ID']
    if not run_id.startswith('verify-continuation-'):
        raise ValueError('Continuation fixture requires a dedicated verification run ID')
    prefix='runs/'+run_id+'/'
    blob=bucket.blob(prefix+'verification.json')
    executions=json.loads(blob.download_as_text()) if blob.exists() else []
    execution=os.environ['CLOUD_RUN_EXECUTION']
    if execution in executions:raise RuntimeError('Unexpected fixture task retry')
    executions.append(execution)
    if len(executions)>2:raise RuntimeError('Coordinator relaunched a completed fixture')
    state='continuing' if len(executions)==1 else 'completed'
    state=os.environ.get('VERIFY_CONTINUATION_STATE',state)
    complete=state=='completed'
    summary={'country':'ZZ','phase':'verification','run_id':run_id,'execution':execution,
        'state':state,'total':1,'processed':int(complete),'pending':int(not complete),
        'downloading':0,'counts':{'saved':int(complete)},'domains':[],
        'updated_at':datetime.now(timezone.utc).isoformat(),'error':None}
    blob.upload_from_string(json.dumps(executions),content_type='application/json')
    bucket.blob(prefix+'progress.json').upload_from_string(json.dumps(summary),content_type='application/json')
    print(json.dumps(summary),flush=True)


if __name__=='__main__':main()
