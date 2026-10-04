import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
from isolated_task import navigate


class BrowserTimeout(Exception):pass


class BrowserNavigationTests(unittest.TestCase):
    def setUp(self):
        self.clock=100.
        self.queued=0.
        self.page=Mock()
        self.page.main_frame=object()
        self.response=SimpleNamespace(request=SimpleNamespace(is_navigation_request=lambda:True),
                                      frame=self.page.main_frame,status=200)
        self.callbacks={}
        self.page.on.side_effect=lambda event,callback:self.callbacks.update({event:callback})
        self.time_patch=patch('isolated_task.time.monotonic',side_effect=lambda:self.clock)
        self.time_patch.start();self.addCleanup(self.time_patch.stop)
        self.browser_patch=patch.dict(sys.modules,{'playwright.sync_api':SimpleNamespace(TimeoutError=BrowserTimeout)})
        self.browser_patch.start();self.addCleanup(self.browser_patch.stop)

    def run_navigation(self):
        return navigate(self.page,'https://example.org/story',lambda:self.queued)

    def fail_goto(self,total,queued):
        def fail(*_,**__):
            self.clock+=total;self.queued+=queued
            raise BrowserTimeout('navigation timed out')
        self.page.goto.side_effect=fail

    def complete(self,predicate,**kwargs):
        self.assertFalse(predicate('about:blank'))
        self.assertTrue(predicate('https://example.org/final'))
        self.callbacks['response'](self.response)
        self.clock+=2

    def test_queued_navigation_receives_only_remaining_actual_work_budget(self):
        self.fail_goto(total=80,queued=75)
        self.page.wait_for_url.side_effect=self.complete
        self.assertIs(self.run_navigation(),self.response)
        self.assertEqual(self.page.wait_for_url.call_args.kwargs,
                         {'wait_until':'domcontentloaded','timeout':30000})
        self.page.remove_listener.assert_called_once()

    def test_no_queue_does_not_extend_timeout(self):
        self.fail_goto(total=36,queued=0)
        with self.assertRaises(BrowserTimeout):self.run_navigation()
        self.page.wait_for_url.assert_not_called()

    def test_queue_cannot_hide_exhausted_actual_work_budget(self):
        self.fail_goto(total=80,queued=40)
        with self.assertRaises(BrowserTimeout):self.run_navigation()
        self.page.wait_for_url.assert_not_called()

    def test_new_queue_during_followup_wait_gets_measured_extension(self):
        self.fail_goto(total=80,queued=75)
        def wait(predicate,**kwargs):
            if self.page.wait_for_url.call_count==1:
                self.clock+=45;self.queued+=40
                raise BrowserTimeout('redirect queued')
            self.complete(predicate,**kwargs)
        self.page.wait_for_url.side_effect=wait
        self.assertIs(self.run_navigation(),self.response)
        self.assertEqual([call.kwargs['timeout'] for call in self.page.wait_for_url.call_args_list],
                         [30000,25000])

    def test_followup_without_new_queue_does_not_extend_again(self):
        self.fail_goto(total=80,queued=75)
        self.page.wait_for_url.side_effect=BrowserTimeout('still not loaded')
        with self.assertRaises(BrowserTimeout):self.run_navigation()
        self.assertEqual(self.page.wait_for_url.call_count,1)

    def test_normal_navigation_is_unchanged(self):
        self.page.goto.return_value=self.response
        self.assertIs(self.run_navigation(),self.response)
        self.page.goto.assert_called_once_with('https://example.org/story',
            wait_until='domcontentloaded',timeout=35000)
        self.page.wait_for_url.assert_not_called()


if __name__=='__main__':unittest.main()
