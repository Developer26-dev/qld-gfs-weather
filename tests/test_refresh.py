import unittest,tempfile,json,fcntl
from pathlib import Path
from datetime import datetime,timedelta,timezone
from unittest.mock import patch
from urllib.parse import urlparse,parse_qs
import eccodes as ec
import refresh as r
RUN=datetime(2026,10,19,tzinfo=timezone.utc)
RAW=(Path(__file__).parent / 'fixtures' / 'gfs-20261019-00-f116.grib2').read_bytes()
def fixture(step,wrong=False):
    out=[]
    with tempfile.TemporaryFile() as f:
        f.write(RAW);f.seek(0)
        while (g:=ec.codes_grib_new_from_file(f)) is not None:
            ec.codes_set(g,'forecastTime',step+int(wrong));out.append(ec.codes_get_message(g));ec.codes_release(g)
    return b''.join(out)
class Fake:
    def __init__(self,fail=None):self.urls=[];self.fail=fail
    def get(self,url):
        self.urls.append(url)
        if '/06/atmos/' in url:return 403,b'not published'
        if url.endswith('/atmos/'):
            return 200,(' '.join(f'gfs.t00z.pgrb2.0p25.f{x:03}"' for x in range(385))).encode()
        step=int(parse_qs(urlparse(url).query)['file'][0][-3:])
        if self.fail=='http':raise RuntimeError('HTTP 503')
        if self.fail=='html':return 200,b'<html>not GRIB</html>'
        return 200,fixture(step,self.fail=='wrong-time')
class RefreshTests(unittest.TestCase):
    def test_native_times_and_briefing_launch(self):
        steps=r.steps(RUN,RUN+timedelta(hours=6),168)
        self.assertLessEqual(len(steps),62);self.assertIn(116,steps);self.assertNotIn(140,steps)
        self.assertTrue(all(x%3==0 or x<=120 for x in steps))
    def test_listing_requires_every_frame(self):
        self.assertFalse(r.complete_listing(b'gfs.t00z.pgrb2.0p25.f116.idx', '00',[116]))
        self.assertTrue(r.complete_listing(b'gfs.t00z.pgrb2.0p25.f116"','00',[116]))
        self.assertFalse(r.complete_listing(b'gfs.t00z.pgrb2.0p25.f116"','00',[116,117]))
    def test_atomic_publish_fallback_and_idempotency(self):
        with tempfile.TemporaryDirectory() as temp:
            source=Fake();result=r.refresh(temp,RUN+timedelta(hours=6),source,6,clock=lambda:RUN+timedelta(hours=7))
            self.assertEqual(result['status'],'published');self.assertEqual(result['runAt'],r.iso(RUN))
            before=(Path(temp)/'current.json').read_bytes();again=Fake()
            self.assertEqual(r.refresh(temp,RUN+timedelta(hours=7),again,6)['status'],'already-attempted')
            self.assertEqual(again.urls,[]);self.assertEqual((Path(temp)/'current.json').read_bytes(),before)
    def test_failure_never_overwrites_last_good(self):
        for mode in ['http','html','wrong-time']:
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as temp:
                previous={'runAt':r.iso(RUN-timedelta(hours=6)),'marker':'last good'}
                path=Path(temp)/'current.json';path.write_text(json.dumps(previous));before=path.read_bytes()
                with self.assertRaises((ValueError,RuntimeError)):r.refresh(temp,RUN+timedelta(hours=6),Fake(mode),6)
                self.assertEqual(path.read_bytes(),before)
    def test_lock_and_bounded_schedule(self):
        with tempfile.TemporaryDirectory() as temp:
            with (Path(temp)/'refresh.lock').open('w') as lock:
                fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                self.assertEqual(r.refresh(temp,RUN,Fake())['status'],'locked')
        for h in range(24):self.assertLessEqual(len(r.steps(RUN,RUN+timedelta(hours=h),168)),62)
    def test_complete_decode_validates_identity(self):
        frame,meta=r.decode(RAW,RUN,116);self.assertEqual(len(frame['u']),63);self.assertEqual(len(meta),3)
        with self.assertRaises(ValueError):r.decode(RAW,RUN,117)
        with self.assertRaises(ValueError):r.decode(RAW[:-4],RUN,116)
    def test_publication_clock_rejects_aged_run(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(RuntimeError,'aged out'):
                r.refresh(temp,RUN+timedelta(hours=6),Fake(),6,clock=lambda:RUN+timedelta(hours=19))
            self.assertFalse((Path(temp)/'current.json').exists())
    def test_missing_run_failure(self):
        s=Fake()
        with patch.object(s,'get',return_value=(403,b'not found')) as req,tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(RuntimeError):r.refresh(temp,RUN+timedelta(hours=6),s,6)
            self.assertEqual(req.call_count,3);self.assertFalse((Path(temp)/'current.json').exists())
if __name__=='__main__':unittest.main()
