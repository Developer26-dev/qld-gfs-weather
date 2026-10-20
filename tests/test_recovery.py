"""Offline failure/recovery contracts. No live downloads or Git writes."""
import copy,json,tempfile,shutil,unittest
from pathlib import Path
from datetime import timedelta
import recovery as r
import pipeline as p
from validate import validate,site_check
from test_pipeline import Source,ROOT
from test_refresh import RUN

class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        shutil.copytree(ROOT/'assets',self.root/'assets')
        p.prepare_site(self.root,None,{'schema':1,'result':{'status':'not-started'}})
        r.save(self.root,{'schema':1,'attempts':[],'pending':None,'bootstrapped':True})
        # Existing validated forecast, representing a prior scheduled model.
        self.t=RUN+timedelta(hours=6,minutes=40)
        p.write(self.root/'state/control.json',{'schema':1,'attempts':[],'history':[]})
        p.reserve(self.root,self.t-timedelta(hours=1),'seed')
        p.run(self.root,self.t-timedelta(hours=1),'seed',Source(RUN),clock=lambda:self.t-timedelta(minutes=30))
    def tearDown(self):self.temp.cleanup()
    def acquire(self,t,job,source):
        self.assertEqual(r.plan(self.root,t,job)['action'],'acquire')
        events=[]
        result=r.acquire(self.root,t,job,source,clock=lambda:t+timedelta(minutes=12),persist=lambda:events.append('persist'))
        return result,events
    def ack(self,t,job,success=True):r.verified(self.root,t+timedelta(minutes=15),job,success)
    def test_hourly_utc_checks_preserve_brisbane_primary_opportunities(self):
        for h in [4,10,16,22]:
            now=RUN.replace(hour=h,minute=39)
            self.assertEqual(r.next_check(now).minute,40)
            self.assertEqual((r.next_check(now)+timedelta(hours=10)).hour,(h+10)%24)
            self.assertEqual(r.slot(now+timedelta(minutes=1)),p.iso(now.replace(minute=40)))
    def test_missed_primary_run_catches_up_and_two_models_advance(self):
        t=self.t+timedelta(hours=5) # delayed to 11:40; missed earlier primary.
        first,_=self.acquire(t,'first',Source(RUN+timedelta(hours=6)));self.ack(t,'first')
        self.assertEqual(first['result']['status'],'published')
        before=p.read(self.root/'public/manifest.json')
        self.assertEqual(r.plan(self.root,t+timedelta(hours=1),'noop')['action'],'none')
        second,_=self.acquire(t+timedelta(hours=6),'second',Source(RUN+timedelta(hours=12)))
        self.ack(t+timedelta(hours=6),'second')
        after=p.read(self.root/'public/manifest.json')
        self.assertNotEqual(before['path'],after['path']);self.assertEqual(second['result']['status'],'published')
        self.assertTrue((self.root/'public'/before['path']).exists());site_check(self.root/'public')
    def test_incomplete_model_only_probes_then_later_recovers(self):
        t=self.t+timedelta(hours=4);old=(self.root/'public/current.json').read_bytes()
        class Missing(Source):
            def get(self,url):
                self.requests.append(url)
                if not url.endswith('/atmos/'):raise AssertionError('No incomplete GRIB download')
                return 200,b'incomplete listing'
        missing=Missing();failed,_=self.acquire(t,'missing',missing)
        self.assertEqual(failed['result']['status'],'failed');self.assertEqual(len(missing.requests),3)
        self.assertEqual(old,(self.root/'public/current.json').read_bytes());self.ack(t,'missing')
        good,_=self.acquire(t+timedelta(hours=1),'recovery',Source(RUN+timedelta(hours=6)))
        self.assertEqual(good['result']['status'],'published')
    def test_partial_download_has_one_delayed_retry_not_unbounded_retries(self):
        t=self.t+timedelta(hours=4);old=(self.root/'public/current.json').read_bytes()
        for i in range(2):
            res,_=self.acquire(t+timedelta(hours=i),str(i),Source(RUN+timedelta(hours=6),True))
            self.assertEqual(res['result']['status'],'failed');self.ack(t+timedelta(hours=i),str(i))
            self.assertEqual(old,(self.root/'public/current.json').read_bytes())
        source=Source(RUN+timedelta(hours=6));res,_=self.acquire(t+timedelta(hours=2),'exhausted',source)
        self.assertEqual(res['result']['status'],'failed');self.assertIn('retry budget',res['result']['reason'])
        self.assertTrue(all(url.endswith('/atmos/') for url in source.requests))
    def test_model_reservation_is_persisted_before_grib_and_push_failure_stops_download(self):
        t=self.t+timedelta(hours=4);source=Source(RUN+timedelta(hours=6))
        r.plan(self.root,t,'1')
        def broken_push():raise RuntimeError('Git unavailable')
        result=r.acquire(self.root,t,'1',source,clock=lambda:t+timedelta(minutes=12),persist=broken_push)
        self.assertEqual(result['result']['status'],'failed')
        self.assertTrue(all(url.endswith('/atmos/') for url in source.requests))
        self.assertEqual(r.load(self.root)['attempts'][-1]['modelRun'],p.iso(RUN+timedelta(hours=6)))
    def test_live_lease_blocks_overlap_but_interrupted_job_recovers(self):
        t=self.t+timedelta(hours=4)
        self.assertEqual(r.plan(self.root,t,'crashed')['action'],'acquire')
        self.assertEqual(r.plan(self.root,t+timedelta(minutes=30),'early')['action'],'none')
        self.assertEqual(r.plan(self.root,t+timedelta(hours=1),'later')['action'],'acquire')
        self.assertEqual(r.load(self.root)['attempts'][0]['status'],'interrupted')
        self.assertEqual(r.plan(self.root,t+timedelta(hours=2),'later')['action'],'none')
    def test_failed_publication_reuses_bytes_without_download_then_marks_original_verified(self):
        t=self.t+timedelta(hours=4);self.acquire(t,'1',Source(RUN+timedelta(hours=6)));self.ack(t,'1',False)
        old=(self.root/'public/current.json').read_bytes()
        self.assertEqual(r.plan(self.root,t+timedelta(hours=1),'retry')['action'],'republish')
        self.ack(t+timedelta(hours=1),'retry')
        state=r.load(self.root);self.assertIsNone(state['pending']);self.assertTrue(state['attempts'][0]['verified'])
        self.assertEqual(old,(self.root/'public/current.json').read_bytes())
        self.assertEqual(r.plan(self.root,t+timedelta(hours=2),'noop')['action'],'none')
    def test_bootstrap_verifies_existing_bytes_without_noaa_or_pages_build(self):
        r.save(self.root,{'schema':1,'attempts':[],'pending':None})
        self.assertEqual(r.plan(self.root,self.t,'bootstrap')['action'],'verify')
        self.assertFalse(r.load(self.root)['attempts'][0]['publicationReserved'])
        self.ack(self.t,'bootstrap');self.assertTrue(r.load(self.root)['bootstrapped'])
    def test_publication_retries_are_bounded_and_resume_next_window(self):
        t=self.t+timedelta(hours=4);self.acquire(t,'1',Source(RUN+timedelta(hours=6)));self.ack(t,'1',False)
        for i in [1,2]:
            self.assertEqual(r.plan(self.root,t+timedelta(hours=i),str(i+1))['action'],'republish')
            self.ack(t+timedelta(hours=i),str(i+1),False)
        self.assertEqual(r.plan(self.root,t+timedelta(hours=3),'blocked')['action'],'none')
        self.assertEqual(r.plan(self.root,t+timedelta(hours=6),'next')['action'],'republish')
    def test_unchanged_forecast_never_redownloads_or_builds_pages(self):
        source=Source(RUN);result,events=self.acquire(self.t,'same',source)
        self.assertEqual(result['result']['status'],'unchanged');self.assertFalse(result['publish'])
        self.assertEqual(events,[]);self.assertTrue(all(url.endswith('/atmos/') for url in source.requests))
    def test_discovery_cap_then_next_window(self):
        for i in range(3):self.acquire(self.t+timedelta(hours=i),str(i),Source(RUN))
        self.assertEqual(r.plan(self.root,self.t+timedelta(hours=3),'fourth')['action'],'none')
        self.assertEqual(r.plan(self.root,self.t+timedelta(hours=4),'next')['action'],'acquire')
    def test_pending_corruption_is_not_published_and_stale_forecast_not_current(self):
        t=self.t+timedelta(hours=4);self.acquire(t,'1',Source(RUN+timedelta(hours=6)));self.ack(t,'1',False)
        old=(self.root/'public/current.json').read_bytes()
        with self.assertRaises(ValueError):validate(old,t+timedelta(hours=20),True)
        self.assertEqual(r.plan(self.root,t+timedelta(hours=20),'fresh-needed')['action'],'acquire')
        recovered=r.acquire(self.root,t+timedelta(hours=20),'fresh-needed',Source(RUN+timedelta(hours=24)),clock=lambda:t+timedelta(hours=20,minutes=12))
        self.assertEqual(recovered['result']['status'],'published')
        validate((self.root/'public/current.json').read_bytes(),t+timedelta(hours=20,minutes=15),True)
        (self.root/'public/status.json').write_text('{}')
        # Separate fresh pending candidate must not survive external metadata alteration.
        state=r.load(self.root);state['pending']={'fingerprint':'wrong','expiresAt':p.iso(t+timedelta(hours=30)),'job':'1'};r.save(self.root,state)
        with self.assertRaises(ValueError):r.plan(self.root,t+timedelta(hours=22),'tampered')
    def test_rolling_full_download_budget_includes_crashed_attempts(self):
        t=self.t+timedelta(hours=4);s=r.load(self.root)
        s['attempts']=[{'job':str(i),'slot':'older','startedAt':p.iso(t-timedelta(hours=2,minutes=i)), 'status':'interrupted','action':'acquire','modelRun':str(i)} for i in range(8)]
        r.save(self.root,s)
        self.assertEqual(r.plan(self.root,t,'capped')['action'],'none')
    def test_partial_model_recovers_on_one_delayed_retry_and_is_never_acquired_again(self):
        t=self.t+timedelta(hours=4)
        failed,_=self.acquire(t,'failed',Source(RUN+timedelta(hours=6),True));self.ack(t,'failed')
        self.assertEqual(failed['result']['status'],'failed')
        source=Source(RUN+timedelta(hours=6))
        good,events=self.acquire(t+timedelta(hours=1),'retry',source);self.ack(t+timedelta(hours=1),'retry')
        self.assertEqual(good['result']['status'],'published');self.assertEqual(events,['persist'])
        self.assertEqual(sum(a.get('modelRun')==p.iso(RUN+timedelta(hours=6)) for a in r.load(self.root)['attempts']),2)
        self.assertEqual(r.plan(self.root,t+timedelta(hours=2),'duplicate')['action'],'none')
    def test_guard_persists_before_source_receives_first_grib_request(self):
        t=self.t+timedelta(hours=4);root=self.root;events=[]
        class CheckSource(Source):
            def get(self,url):
                if '/cgi-bin/' in url:
                    self_test.assertEqual(events,['persisted'])
                    self_test.assertEqual(r.load(root)['attempts'][-1]['modelRun'],p.iso(RUN+timedelta(hours=6)))
                return super().get(url)
        self_test=self;r.plan(root,t,'ordered')
        result=r.acquire(root,t,'ordered',CheckSource(RUN+timedelta(hours=6)),clock=lambda:t+timedelta(minutes=12),persist=lambda:events.append('persisted'))
        self.assertTrue(result['publish'])
if __name__=='__main__':unittest.main()
