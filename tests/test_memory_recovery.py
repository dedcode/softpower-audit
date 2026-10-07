import sys,json,unittest
from pathlib import Path
from unittest.mock import patch
from google.api_core.exceptions import NotFound
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
from memory_recovery import Recovery,retry_owns_lease
class Bucket:
 def __init__(self):self.data={}
 def blob(self,key):
  data=self.data
  class Blob:
   def download_as_text(self):
    if key not in data:raise NotFound(key)
    return data[key]
   def upload_from_string(self,value,**kwargs):data[key]=value
  return Blob()
class RecoveryTests(unittest.TestCase):
 def test_restarts_preserve_budget_and_stop_at_limit(self):
  b=Bucket();now=[100.];r=Recovery(b,'run/','execution',1000,clock=lambda:now[0])
  for i in range(3):
   self.assertTrue(r.reserve_restart());now[0]+=20
   r=Recovery(b,'run/','execution',1000,clock=lambda:now[0]);self.assertTrue(r.reduced_concurrency)
  self.assertEqual(r.elapsed,60);self.assertFalse(r.reserve_restart())
 def test_no_runtime_limit_still_caps_memory_restarts(self):
  b=Bucket();now=[100.];r=Recovery(b,'run/','execution',None,clock=lambda:now[0])
  for i in range(3):
   now[0]+=30*24*3600
   r=Recovery(b,'run/','execution',None,clock=lambda:now[0])
   self.assertTrue(r.reserve_restart())
  self.assertEqual(r.elapsed,90*24*3600)
  self.assertFalse(r.reserve_restart())
  self.assertEqual(r.state['memory_restarts'],3)
 def test_time_limit_never_resets_on_restart(self):
  b=Bucket();Recovery(b,'run/','execution',30,clock=lambda:100)
  r=Recovery(b,'run/','execution',30,clock=lambda:131)
  self.assertFalse(r.reserve_restart())
 def test_attempt_age_survives_memory_exec_and_resets_on_cloud_retry(self):
  b=Bucket();now=[100.]
  with patch.dict('os.environ',{'CLOUD_RUN_TASK_ATTEMPT':'0'}):
   r=Recovery(b,'run/','execution',None,clock=lambda:now[0]);self.assertTrue(r.reserve_restart())
   now[0]=150
   restored=Recovery(b,'run/','execution',None,clock=lambda:now[0])
   self.assertEqual(restored.attempt_elapsed,50)
  with patch.dict('os.environ',{'CLOUD_RUN_TASK_ATTEMPT':'1'}):
   now[0]=200
   retried=Recovery(b,'run/','execution',None,clock=lambda:now[0])
   self.assertEqual(retried.attempt_elapsed,0);self.assertEqual(retried.elapsed,100)
   self.assertEqual(retried.state['memory_restarts'],1)
   now[0]=230
   self.assertEqual(Recovery(b,'run/','execution',None,clock=lambda:now[0]).attempt_elapsed,30)
 def test_legacy_recovery_state_preserves_conservative_attempt_age(self):
  b=Bucket();b.data['run/recovery/execution.json']=json.dumps({'started_at':100,'memory_restarts':1})
  with patch.dict('os.environ',{'CLOUD_RUN_TASK_ATTEMPT':'0'}):
   r=Recovery(b,'run/','execution',None,clock=lambda:150)
  self.assertEqual(r.attempt_elapsed,50);self.assertEqual(r.state['memory_restarts'],1)
 def test_cloud_retry_without_memory_pressure_preserves_concurrency(self):
  r=Recovery(Bucket(),'run/','execution',100)
  with patch.dict('os.environ',{'CLOUD_RUN_TASK_ATTEMPT':'1'}):self.assertFalse(r.reduced_concurrency)
 def test_only_later_attempt_of_same_task_can_reclaim_lease(self):
  p={'execution':'execution','task_index':'0','task_attempt':0}
  self.assertTrue(retry_owns_lease(p,'execution','0',1))
  for execution,index,attempt in [('other','0',1),('execution','1',1),('execution','0',0),('local','0',1)]:
   self.assertFalse(retry_owns_lease(p,execution,index,attempt))
if __name__=='__main__':unittest.main()
