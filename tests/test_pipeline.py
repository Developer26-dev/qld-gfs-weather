import copy, hashlib, json, shutil, tempfile, unittest
from pathlib import Path
from datetime import timedelta
from urllib.parse import parse_qs, urlparse
import eccodes as ec
import pipeline as p
from validate import validate, site_check
from test_refresh import RAW, RUN
ROOT=Path(__file__).resolve().parents[1]

def grib(run,step):
    out=[]
    with tempfile.TemporaryFile() as f:
        f.write(RAW);f.seek(0)
        while (g:=ec.codes_grib_new_from_file(f)) is not None:
            ec.codes_set(g,'dataDate',int(run.strftime('%Y%m%d')))
            ec.codes_set(g,'dataTime',run.hour*100)
            ec.codes_set(g,'forecastTime',step)
            out.append(ec.codes_get_message(g));ec.codes_release(g)
    return b''.join(out)
class Source:
    def __init__(self,run=RUN,fail=False):self.run=run;self.fail=fail;self.requests=[];self.bytes=0
    def get(self,url):
        self.requests.append(url)
        if url.endswith('/atmos/'):
            if f'/{self.run:%H}/atmos/' not in url:return 404,b'incomplete'
            return 200,(' '.join(f'gfs.t{self.run:%H}z.pgrb2.0p25.f{x:03}"' for x in range(385))).encode()
        if self.fail:raise RuntimeError('NOAA outage fixture')
        step=int(parse_qs(urlparse(url).query)['file'][0][-3:]);raw=grib(self.run,step);self.bytes+=len(raw);return 200,raw
class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        shutil.copytree(ROOT/'assets',self.root/'assets');p.write(self.root/'state/control.json',{'schema':1,'attempts':[],'history':[]})
        p.prepare_site(self.root,None,{'schema':1,'result':{'status':'not-started'}})
    def tearDown(self):self.temp.cleanup()
    def cycle(self,at,job,source):
        self.assertTrue(p.reserve(self.root,at,job))
        return p.run(self.root,at,job,source,clock=lambda:at+timedelta(minutes=12))
    def test_successive_native_runs_and_version_integrity(self):
        self.cycle(RUN+timedelta(hours=6),'1',Source())
        first=p.read(self.root/'public/manifest.json')
        result=self.cycle(RUN+timedelta(hours=12),'2',Source(RUN+timedelta(hours=6)))
        second=p.read(self.root/'public/manifest.json')
        self.assertNotEqual(first['path'],second['path']);self.assertEqual(second['runAt'],p.iso(RUN+timedelta(hours=6)))
        self.assertTrue((self.root/'public'/first['path']).exists());self.assertLessEqual(len(list((self.root/'public/runs').glob('*'))),2)
        self.assertLess(result['publication']['bytes'],500000);site_check(self.root/'public')
    def test_partial_download_preserves_last_good_and_recovers(self):
        self.cycle(RUN+timedelta(hours=6),'1',Source())
        before=(self.root/'public/current.json').read_bytes()
        failed=self.cycle(RUN+timedelta(hours=12),'2',Source(RUN+timedelta(hours=6),True))
        self.assertEqual(failed['result']['status'],'failed');self.assertEqual(before,(self.root/'public/current.json').read_bytes())
        recovered=self.cycle(RUN+timedelta(hours=18),'3',Source(RUN+timedelta(hours=12)))
        self.assertEqual(recovered['result']['status'],'published');self.assertNotEqual(before,(self.root/'public/current.json').read_bytes())
    def test_reserved_crash_and_repeat_use_no_downloads(self):
        now=RUN+timedelta(hours=6);self.assertTrue(p.reserve(self.root,now,'a'))
        self.assertFalse(p.reserve(self.root,now+timedelta(hours=1),'b'))
        self.assertTrue(p.reserve(self.root,now+timedelta(hours=6),'c'))
    def test_manual_setup_does_not_consume_next_scheduled_opportunity(self):
        self.assertTrue(p.reserve(self.root,RUN+timedelta(hours=7,minutes=50),'setup'))
        self.assertFalse(p.reserve(self.root,RUN+timedelta(hours=10,minutes=39),'duplicate'))
        self.assertTrue(p.reserve(self.root,RUN+timedelta(hours=10,minutes=40),'scheduled'))
        self.assertFalse(p.reserve(self.root,RUN+timedelta(hours=12,minutes=1),'delayed-duplicate'))
        self.assertTrue(p.reserve(self.root,RUN+timedelta(hours=16,minutes=40),'next'))
    def test_corrupt_or_incomplete_publication_is_rejected(self):
        self.cycle(RUN+timedelta(hours=6),'1',Source())
        m=p.read(self.root/'public/manifest.json');(self.root/'public'/m['path']).write_text('{}')
        with self.assertRaises((ValueError,KeyError)):site_check(self.root/'public')
    def test_stale_future_and_fabricated_times_rejected(self):
        self.cycle(RUN+timedelta(hours=6),'1',Source());raw=(self.root/'public/current.json').read_bytes()
        with self.assertRaises(ValueError):validate(raw,RUN+timedelta(hours=19),True)
        with self.assertRaises(ValueError):validate(raw,RUN-timedelta(hours=1),True)
        d=json.loads(raw);d['frames'][0]['validAt']=p.iso(RUN)
        with self.assertRaises(ValueError):validate(json.dumps(d).encode())
    def test_no_model_rollback_and_next_scheduled_time(self):
        self.cycle(RUN+timedelta(hours=12),'1',Source(RUN+timedelta(hours=6)))
        before=(self.root/'public/current.json').read_bytes()
        outcome=self.cycle(RUN+timedelta(hours=18),'2',Source())
        self.assertEqual(before,(self.root/'public/current.json').read_bytes())
        self.assertEqual(outcome['result']['status'],'failed') # requested older run is outside the bounded discovery window
        self.assertEqual(p.next_due(RUN+timedelta(hours=4,minutes=40)),RUN+timedelta(hours=10,minutes=40))
    def test_missing_forecast_frame_rejected(self):
        self.cycle(RUN+timedelta(hours=6),'1',Source())
        d=p.read(self.root/'public/current.json');del d['frames'][10]
        with self.assertRaises(ValueError):validate(json.dumps(d).encode())
    def test_only_static_weather_files_can_publish(self):
        (self.root/'public/_worker.js').write_text('export default {}')
        with self.assertRaises(ValueError):site_check(self.root/'public')
if __name__=='__main__':unittest.main()
