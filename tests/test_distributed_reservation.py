"""New cohorts wait for atomic assignment without spending native retries."""
import copy
import unittest
from unittest.mock import patch

from test_distributed_retirement import fixture,terminal
from test_distributed_worker import worker_record
from test_host_queue import Clock
import distributed_worker as distributed


class ReservationStartupTests(unittest.TestCase):
 def setUp(self):
  self.run,self.guard=fixture();self.clock=Clock()
  self.reservation={**self.guard,'owner':'handover-token','execution':'handover-token','handover_id':'token',
                    'expires_at':(distributed.datetime.now(distributed.timezone.utc)+distributed.timedelta(minutes=30)).isoformat()}
  self.run.bucket.seed(self.run.lease.name,self.reservation)

 def test_new_execution_waits_past_twelve_polls_without_consuming_cas_retries(self):
  sleeps=[]
  def wait(seconds):
   sleeps.append(seconds);self.clock.advance(seconds)
   if len(sleeps)==15:self.run.bucket.seed(self.run.lease.name,self.guard)
  with patch('distributed_worker.time.monotonic',side_effect=self.clock.now),patch('distributed_worker.time.sleep',side_effect=wait):
   self.run.lease_update(initial=True)
  self.assertEqual(len(sleeps),15);self.assertEqual(self.run.bucket.writes,[])
  self.run.queue.claim.assert_not_called();self.run.queue.heartbeat.assert_not_called()
  self.assertFalse(self.run.fetcher.abort_event.is_set())

 def test_untransferred_reservation_times_out_after_180_seconds(self):
  with patch('distributed_worker.time.monotonic',side_effect=self.clock.now), \
       patch('distributed_worker.time.sleep',side_effect=self.clock.advance):
   with self.assertRaisesRegex(RuntimeError,'180 seconds'):self.run.lease_update(initial=True)
  self.assertEqual(self.clock.now(),280);self.assertEqual(self.run.bucket.writes,[])
  self.assertTrue(self.run.fetcher.abort_event.is_set());self.run.queue.claim.assert_not_called()

 def test_stop_or_retirement_while_waiting_prevents_assignment_or_publication(self):
  for action in ('STOP','retired'):
   with self.subTest(action=action):
    self.setUp()
    def wait(seconds):
     self.clock.advance(seconds)
     path=self.run.prefix+('STOP' if action=='STOP' else 'retired-executions/'+self.run.execution+'.json')
     self.run.bucket.seed(path,{})
    with patch('distributed_worker.time.monotonic',side_effect=self.clock.now),patch('distributed_worker.time.sleep',side_effect=wait):
     with self.assertRaisesRegex(RuntimeError,'paused|retired'):self.run.lease_update(initial=True)
    self.assertEqual(self.run.bucket.writes,[]);self.run.queue.claim.assert_not_called()
    self.run.queue.heartbeat.assert_not_called();self.assertTrue(self.run.fetcher.abort_event.is_set())

 def test_other_owner_or_malformed_reservation_is_rejected_without_wait(self):
  for change in ({'execution':'other','owner':'other'},{'run_id':'other'},
                 {'handover_id':''},{'handover_id':'invalid token'},{'task_count':0}):
   with self.subTest(change=change):
    self.setUp();self.run.bucket.seed(self.run.lease.name,{**self.reservation,**change})
    with patch('distributed_worker.time.sleep') as sleep:
     with self.assertRaisesRegex(RuntimeError,'Another crawl execution'):self.run.lease_update(initial=True)
    sleep.assert_not_called();self.assertEqual(self.run.bucket.writes,[])


class SafeTerminalReleaseTests(unittest.TestCase):
 def test_newer_active_attempt_prevents_stale_terminal_release(self):
  run,_=fixture();summary=terminal(run)
  run.queue.list_workers.return_value=[worker_record(0,'completed'),worker_record(1,'starting',task_attempt=1)]
  run.release_cohort_lease(summary)
  self.assertIn(run.lease.name,run.bucket.objects);self.assertEqual(run.bucket.writes,[])

 def test_failed_cohort_keeps_guard_for_native_task_retries(self):
  for state in ('failed','recovery_failed'):
   with self.subTest(state=state):
    run,_=fixture();summary=terminal(run);summary['state']=state
    summary['workers'][1]['state']=state
    run.queue.list_workers.return_value=summary['workers']
    run.release_cohort_lease(summary)
    self.assertIn(run.lease.name,run.bucket.objects);self.assertEqual(run.bucket.writes,[])

 def test_newer_failed_attempt_also_prevents_release_of_old_success_snapshot(self):
  run,_=fixture();summary=terminal(run)
  run.queue.list_workers.return_value=[worker_record(0,'completed'),worker_record(1,'failed',task_attempt=1)]
  run.release_cohort_lease(summary)
  self.assertIn(run.lease.name,run.bucket.objects)

 def test_verified_terminal_cohort_still_releases_normally(self):
  for state in ('completed','continuing','paused_by_operator'):
   with self.subTest(state=state):
    run,_=fixture();summary=terminal(run);summary['state']=state
    for record in summary['workers']:record['state']=state
    run.queue.list_workers.return_value=copy.deepcopy(summary['workers'])
    run.release_cohort_lease(summary)
    self.assertNotIn(run.lease.name,run.bucket.objects)
    self.assertEqual(run.bucket.writes,['delete:'+run.lease.name])


if __name__=='__main__':unittest.main()
