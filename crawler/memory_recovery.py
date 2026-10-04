"""Bounded automatic worker recycling; all task attempts share the time budget."""
import ctypes,gc,json,os,time
from google.api_core.exceptions import NotFound

MAX_MEMORY_RESTARTS=3

def reclaim_memory():
    gc.collect()
    try:ctypes.CDLL(None).malloc_trim(0)
    except (AttributeError,OSError):pass


def retry_owns_lease(previous,execution,task_index,attempt):
    # Cloud Run starts a retry only after the preceding task attempt terminates.
    # Never steal another execution's (or another task's) live lease.
    return (execution!='local' and previous.get('execution')==execution
            and previous.get('task_index')==task_index
            and int(previous.get('task_attempt',-1))<attempt)

class Recovery:
    def __init__(self,bucket,prefix,execution,max_runtime,clock=time.time):
        self.blob=bucket.blob(prefix+'recovery/'+execution+'.json')
        self.clock=clock;self.max_runtime=max_runtime
        try:self.state=json.loads(self.blob.download_as_text())
        except NotFound:
            self.state={'started_at':clock(),'memory_restarts':0}
            self.save()
    def save(self):
        self.blob.upload_from_string(json.dumps(self.state),content_type='application/json',timeout=30)
    @property
    def elapsed(self):return max(0,self.clock()-self.state['started_at'])
    @property
    def reduced_concurrency(self):
        return self.state['memory_restarts']>0 or int(os.environ.get('CLOUD_RUN_TASK_ATTEMPT','0'))>0
    def reserve_restart(self):
        if self.elapsed>=self.max_runtime or self.state['memory_restarts']>=MAX_MEMORY_RESTARTS:return False
        self.state['memory_restarts']+=1;self.save();return True
