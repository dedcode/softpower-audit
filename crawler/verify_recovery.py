"""Cloud-only fault injection using a private test lease; no publisher requests."""
import json,os,signal,time,uuid
from google.cloud import storage
from isolation import extract_isolated,IsolationError
from memory_recovery import Recovery

def main():
    from worker import Run
    bucket=storage.Client().bucket(os.environ['CRAWL_BUCKET'])
    execution=os.environ['CLOUD_RUN_EXECUTION'];attempt=int(os.environ['CLOUD_RUN_TASK_ATTEMPT'])
    prefix='verification/recovery-v1/'+execution+'/'
    run=Run.__new__(Run);run.run_id='verification';run.owner=uuid.uuid4().hex;run.execution=execution
    run.task_index=os.environ.get('CLOUD_RUN_TASK_INDEX','0');run.task_attempt=attempt
    run.lease=bucket.blob(prefix+'lease.json')
    run.lease_update(initial=True)
    checkpoint=bucket.blob(prefix+'checkpoint.json')
    if attempt==0:
        checkpoint.upload_from_string(json.dumps({'completed':['saved-original-1','saved-original-2']}),content_type='application/json')
        print('Fault injection: persisted checkpoint and lease; killing test task.',flush=True)
        os.kill(os.getpid(),signal.SIGKILL)
    assert json.loads(checkpoint.download_as_text())['completed']==['saved-original-1','saved-original-2']
    # Force the stage watchdog to interrupt one real parser subprocess, then
    # verify another page can still be extracted in the same worker process.
    import isolation
    previous=isolation.memory_pressure;isolation.memory_pressure=lambda:True
    try:
        try:extract_isolated(b'<article>test</article>','https://example.org/test')
        except IsolationError:pass
        else:raise AssertionError('Memory guard did not interrupt stage')
    finally:isolation.memory_pressure=previous
    result=extract_isolated(('<article><p>'+('A real article. '*100)+'</p></article>').encode(),'https://example.org/test')
    assert result['quality']=='candidate'
    recovery=Recovery(bucket,prefix,execution,1000)
    prior=recovery.state['memory_restarts']
    if prior==0:assert recovery.reserve_restart()
    restored=Recovery(bucket,prefix,execution,1000)
    assert restored.state['memory_restarts']>=1 and restored.reduced_concurrency
    run.lease.delete(if_generation_match=run.lease.generation)
    report={'passed':True,'execution':execution,'task_attempt':attempt,'abrupt_kill_retried':True,'checkpoint_preserved':True,'stale_test_lease_reclaimed':True,'stage_memory_interrupt_then_success':True,'restart_budget_persisted':True}
    bucket.blob('verification/recovery-v1/result.json').upload_from_string(json.dumps(report),content_type='application/json');print(json.dumps(report),flush=True)
