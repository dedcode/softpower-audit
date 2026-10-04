"""Article concurrency must not lose work or multiply the host request rate."""
import sys
import unittest
from collections import Counter, deque
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
from worker import Run


class SchedulingTests(unittest.TestCase):
    def make_run(self,workers=48,per_outlet=4):
        run=Run.__new__(Run)
        run.config={'workers':workers,'per_outlet_workers':per_outlet}
        run.queues={domain:deque({'url':domain+'/'+str(i),'outlet':domain} for i in range(count))
                    for domain,count in [('small.ke',2),('large.ke',100),('medium.ke',20)]}
        run.active={};run.inflight_items={};run.paused={};run.fetcher=Mock();run.run_id='run';run.country='KE'
        pool=Mock();pool.submit.side_effect=lambda *args:object()
        return run,pool

    def test_bounded_overlap_and_large_backlog_starts_first(self):
        run,pool=self.make_run(workers=7)
        run.schedule(pool)
        self.assertEqual(len(run.active),7)
        self.assertEqual(pool.submit.call_args_list[0].args[1]['outlet'],'large.ke')
        self.assertLessEqual(max(Counter(run.active.values()).values()),4)
        self.assertEqual(sum(map(len,run.queues.values()))+len(run.active),122)
        urls=[call.args[1]['url'] for call in pool.submit.call_args_list]
        self.assertEqual(len(urls),len(set(urls)))

    def test_existing_active_work_and_paused_outlets_respected(self):
        run,pool=self.make_run()
        run.active={object():'large.ke' for _ in range(4)}
        run.paused={'medium.ke':'blocked'}
        run.schedule(pool)
        self.assertEqual(Counter(run.active.values()),{'large.ke':4,'small.ke':2})
        self.assertEqual(len(run.queues['medium.ke']),20)
        self.assertEqual(len(run.queues['large.ke']),100)

    def test_default_remains_one_article_per_outlet(self):
        run,pool=self.make_run()
        run.config.pop('per_outlet_workers')
        run.schedule(pool)
        self.assertEqual(Counter(run.active.values()),{'large.ke':1,'medium.ke':1,'small.ke':1})

    def test_completion_frees_capacity_without_rescheduling_active_url(self):
        run,pool=self.make_run(workers=2,per_outlet=2)
        run.schedule(pool)
        completed=next(iter(run.active));run.active.pop(completed)
        run.schedule(pool)
        self.assertEqual(len(run.active),2)
        urls=[call.args[1]['url'] for call in pool.submit.call_args_list]
        self.assertEqual(len(urls),len(set(urls)))


if __name__=='__main__':unittest.main()
