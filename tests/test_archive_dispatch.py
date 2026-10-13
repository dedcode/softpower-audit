import copy,sys,time,unittest
from pathlib import Path
from unittest.mock import Mock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
from archive_dispatch import archive_service
from extractor_version import VERSION
from crawl import Fetcher
from shared_hosts import Admission
from shared_queue import Claim,article_key
from test_crawler import Bucket
from test_distributed_worker import bare_run
from test_shared_queue import Database,transactional,article,result
from shared_queue import SharedQueue
from result_store import ResultIndex

ITEM={'first_observed':'2020-01-02'}
SNAP='https://web.archive.org/web/20200102/https://news.test/a'
def cache(available=False):
    return {'payload':{'archived_snapshots':{'closest':{'available':True,'url':SNAP}} if available else {}}}

class ArchiveRouteTests(unittest.TestCase):
    def test_uncached_lookup_and_available_snapshot_have_different_services(self):
        self.assertEqual(archive_service(None,ITEM),'archive.org')
        c={'archive_lookups':{'20200102':cache(True)}}
        self.assertEqual(archive_service(c,ITEM),'web.archive.org')
        c['archive_seen']=[SNAP]
        self.assertEqual(archive_service(c,ITEM),'archive.org')
    def test_complete_cached_misses_need_no_archive_server(self):
        c={'archive_lookups':{'20200102':cache(),'':cache()}}
        before=copy.deepcopy(c)
        self.assertEqual(archive_service(c,ITEM),'local');self.assertEqual(c,before)
    def test_latest_snapshot_and_seen_snapshots(self):
        c={'archive_lookups':{'20200102':cache(),'':cache(True)}}
        self.assertEqual(archive_service(c,ITEM),'web.archive.org')
        c['archive_seen']=[SNAP]
        self.assertEqual(archive_service(c,ITEM),'local')
    def test_retry_envelope_and_explicit_service_wait(self):
        for host in ('archive.org','web.archive.org'):
            c={'kind':'phased-toolbox','pipeline':{'archive_service':host}}
            self.assertEqual(archive_service(c,ITEM),host)
    def test_reanalysis_and_linked_original_remain_runnable_during_archive_outage(self):
        for best,version in (({'raw_uri':'gs://test/raw'},'old'),
                             ({'raw_uri':'gs://test/raw','continuations':['https://news.test/full']},VERSION)):
            c={'best':best,'extractor_version':version,'archive_service':'web.archive.org'}
            self.assertEqual(archive_service(c,ITEM),'local')
    def test_malformed_route_is_rejected(self):
        for c in ({'archive_service':'untrusted.test'},{'archive_lookups':[]},
                  {'archive_lookups':{'20200102':{'payload':{'archived_snapshots':[]}}}}):
            with self.assertRaises(ValueError):archive_service(c,ITEM)
    def test_unknown_snapshot_host_reaches_pipeline_validation_without_its_network_gate(self):
        c=cache(True);c['payload']['archived_snapshots']['closest']['url']='https://unexpected.test/a'
        self.assertEqual(archive_service({'archive_lookups':{'20200102':c}},ITEM),'local')

class ArchiveHealthTests(unittest.TestCase):
    def test_service_health_does_not_make_requests_and_spacing_is_not_outage(self):
        f=Fetcher(Bucket(),'run');f.host_coordinator=Mock()
        f.host_coordinator.availability.return_value={'archive.org':Admission(False,1),
                                                     'web.archive.org':Admission(False,100)}
        with patch.object(f,'one') as network:
            states=f.archive_dispatch_availability()
        network.assert_not_called();self.assertTrue(all(s.cooldown_seconds==0 for s in states.values()))
    def test_local_robots_failure_and_shared_cooldown_are_both_used(self):
        f=Fetcher(Bucket(),'run');f.host_coordinator=Mock()
        f.host_coordinator.availability.return_value={'archive.org':Admission(True),
                                                     'web.archive.org':Admission(False,30,30)}
        f.robots_failures['https://archive.org']={'expires_at':120}
        with patch('crawl.time.monotonic',return_value=100):states=f.archive_dispatch_availability()
        self.assertEqual(states['archive.org'].cooldown_seconds,20)
        self.assertEqual(states['web.archive.org'].cooldown_seconds,30)
    def test_missing_shared_health_fails_closed(self):
        f=Fetcher(Bucket(),'run');f.host_coordinator=Mock();f.host_coordinator.availability.return_value={}
        with self.assertRaisesRegex(RuntimeError,'incomplete'):f.archive_dispatch_availability()

class ArchiveBorrowingTests(unittest.TestCase):
    def make_run(self,states,publishers=0):
        run=bare_run();run.verification=False;run.config['workers']=48
        run.fetcher.archive_dispatch_availability=Mock(return_value=states)
        def claim(**kw):
            if kw['phase']=='publisher':count=min(publishers,kw['limit'])
            elif kw['phase']=='archive':count=kw['limit']
            else:count=0
            return [Claim(kw['phase']+str(i),'token',{'url':f'https://news.test/{i}','outlet':'news.test'},
                          time.time()+600,phase=kw['phase']) for i in range(count)]
        run.queue.claim.side_effect=claim;return run
    def test_archive_uses_44_spare_slots_and_preserves_publisher_capacity(self):
        run=self.make_run({'archive.org':Admission(True),'web.archive.org':Admission(True)},1)
        with patch.dict('os.environ',{'CRAWL_ARCHIVE_SLOTS':'44'}):claims=run.claim_available()
        self.assertEqual(len(claims),45)
        self.assertEqual(sum(c.phase=='archive' for c in claims),44)
    def test_download_outage_does_not_stop_lookup_or_local_work(self):
        run=self.make_run({'archive.org':Admission(True),'web.archive.org':Admission(False,30,30)})
        with patch.dict('os.environ',{'CRAWL_ARCHIVE_SLOTS':'44'}):run.claim_available()
        self.assertEqual(run.queue.claim.call_args.kwargs['archive_services'],['local','archive.org'])
        self.assertEqual(run.archive_scheduler['pauses_seconds'],{'web.archive.org':30})
    def test_lookup_outage_does_not_stop_downloads(self):
        run=self.make_run({'archive.org':Admission(False,30,30),'web.archive.org':Admission(True)})
        run.claim_available()
        self.assertEqual(run.queue.claim.call_args.kwargs['archive_services'],['local','web.archive.org'])
    def test_both_outages_only_admit_local_work(self):
        run=self.make_run({'archive.org':Admission(False,30,30),'web.archive.org':Admission(False,10,10)})
        run.claim_available()
        self.assertEqual(run.queue.claim.call_args.kwargs['archive_services'],['local'])
        self.assertEqual(run.dispatch_retry_seconds,10)

class ArchiveFilteredQueueTests(unittest.TestCase):
    def setUp(self):
        p=patch('shared_queue.firestore.transactional',transactional);p.start();self.addCleanup(p.stop)
        self.db=Database();self.q=SharedQueue(self.db,'run','owner',clock=lambda:1000)
        self.items=[article(i) for i in range(120)]
        self.q.initialize(self.items,ResultIndex(),{'input_count':120,'country':'CI','source_table':'p.d.t'})
        for item in self.items:
            ref=self.q.articles.document(article_key(item['url']));ref.update({'phase':'archive','archive_due_at':0,
                'archive_service':'web.archive.org'})
        self.target=self.q.articles.document(article_key(self.items[-1]['url']))
        self.target.update({'archive_service':'archive.org','archive_due_at':1})
    def test_filtered_query_skips_full_blocked_backlog_without_leasing_or_rewriting_it(self):
        before=copy.deepcopy(self.db.data)
        claimed=self.q.claim(44,per_outlet=44,phase='archive',archive_services=['archive.org','local'])
        self.assertEqual([c.article_id for c in claimed],[self.target.id])
        for path,value in before.items():
            if path!=self.target.path:self.assertEqual(self.db.data[path],value)
        self.assertLessEqual(self.db.query_results,2)
    def test_transaction_rechecks_route_when_candidate_changes(self):
        claims=self.q._claim_batch([(self.target,'token')],'archive',{},44,archive_services={'web.archive.org'})
        self.assertEqual(claims,[]);self.assertEqual(self.target.get().to_dict()['state'],'ready')
    def test_invalid_service_and_non_archive_filter_are_rejected(self):
        for kw in ({'phase':'archive','archive_services':['bad']},{'phase':'publisher','archive_services':['local']}):
            with self.assertRaises(ValueError):self.q.claim(**kw)

class ArchiveMigrationTests(unittest.TestCase):
    def test_route_update_keeps_result_and_source_fields_intact(self):
        from archive_dispatch_migration import route_fields
        doc={'article_id':'aid','phase':'archive','state':'ready','item':ITEM,'checkpoint_uri':'gs://test/checkpoint'}
        envelope={'article_id':'aid','run_id':'run','phase':'archive','checkpoint':{}}
        before=copy.deepcopy(doc);fields=route_fields(doc,envelope,'run','old')
        self.assertEqual(fields,{'archive_service':'archive.org'});self.assertEqual(doc,before)
    def test_only_stopped_cohort_claims_can_be_released(self):
        from archive_dispatch_migration import route_fields
        doc={'article_id':'aid','phase':'archive','state':'leased','owner':'old-0-1-random','item':ITEM}
        envelope={'article_id':'aid','run_id':'run','phase':'archive','checkpoint':{}}
        self.assertEqual(route_fields(doc,envelope,'run','old')['state'],'ready')
        doc['owner']='other-0-0-random'
        with self.assertRaises(ValueError):route_fields(doc,envelope,'run','old')
    def test_wrong_run_and_terminal_documents_are_rejected(self):
        from archive_dispatch_migration import route_fields
        doc={'article_id':'aid','phase':'archive','state':'ready','item':ITEM}
        envelope={'article_id':'aid','run_id':'other','phase':'archive','checkpoint':{}}
        with self.assertRaises(ValueError):route_fields(doc,envelope,'run','old')
        envelope['run_id']='run';doc['state']='done'
        with self.assertRaises(ValueError):route_fields(doc,envelope,'run','old')

if __name__=='__main__':unittest.main()
