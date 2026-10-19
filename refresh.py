"""Isolated bounded GFS cache publisher. No Cloudflare writes or private data access.
Run under an external scheduler; this file does not install or activate one.
Requires eccodes (pinned in requirements.txt). All publication is local/atomic.
"""
import argparse, fcntl, hashlib, json, math, os, re, subprocess, tempfile, time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlencode
import eccodes as ec

UTC=timezone.utc
BASE='https://nomads.ncep.noaa.gov'
GRID={'west':152.5,'south':-28,'dx':.25,'dy':.25,'nx':7,'ny':9}
MAX_REQUESTS=70
MAX_SECONDS=750
MAX_BYTES=2_000_000

def iso(t): return t.isoformat(timespec='seconds').replace('+00:00','Z')
def parse(t): return datetime.fromisoformat(t.replace('Z','+00:00'))
def candidates(now):
    latest=now.replace(hour=now.hour//6*6,minute=0,second=0,microsecond=0)
    return [latest-timedelta(hours=6*i) for i in range(3)]
def steps(run,now,horizon=168):
    first=math.ceil((now-run).total_seconds()/3600)
    last=math.floor((now+timedelta(hours=horizon)-run).total_seconds()/3600)
    # Real 3-hour frames, plus preferred 06:00 Brisbane only while native hourly exists.
    return [h for h in range(first,min(last,384)+1) if h%3==0 or (h<=120 and (run.hour+h+10)%24==6)]
def complete_listing(body,cycle,wanted):
    names=set(re.findall(r'gfs\.t'+cycle+r'z\.pgrb2\.0p25\.f(\d{3})(?![\w.])',body.decode('utf8','replace')))
    return all(f'{s:03}' in names for s in wanted)
def atomic(path,data):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix(path.suffix+'.new')
    with tmp.open('wb') as f:f.write(data);f.flush();os.fsync(f.fileno())
    os.replace(tmp,path)
def decode(raw,run,step):
    if len(raw)>100_000 or not raw.startswith(b'GRIB') or not raw.endswith(b'7777'):raise ValueError('Not a complete bounded GRIB file')
    frame={};meta=[]
    with tempfile.TemporaryFile() as f:
        f.write(raw);f.seek(0)
        while (g:=ec.codes_grib_new_from_file(f)) is not None:
            try:
                keys=['shortName','units','dataDate','dataTime','validityDate','validityTime','Ni','Nj','latitudeOfFirstGridPointInDegrees','longitudeOfFirstGridPointInDegrees','iDirectionIncrementInDegrees','jDirectionIncrementInDegrees','jScansPositively','iScansNegatively','jPointsAreConsecutive','uvRelativeToGrid','typeOfLevel','level']
                m={k:ec.codes_get(g,k) for k in keys}
                valid=run+timedelta(hours=step)
                if (m['dataDate'],m['dataTime'],m['validityDate'],m['validityTime'])!=(int(run.strftime('%Y%m%d')),run.hour*100,int(valid.strftime('%Y%m%d')),valid.hour*100):raise ValueError('Mixed run or wrong valid time')
                if (m['Ni'],m['Nj'],m['latitudeOfFirstGridPointInDegrees'],m['longitudeOfFirstGridPointInDegrees'],m['iDirectionIncrementInDegrees'],m['jDirectionIncrementInDegrees'],m['jScansPositively'],m['iScansNegatively'],m['jPointsAreConsecutive'],m['uvRelativeToGrid'])!=(7,9,-28,152.5,.25,.25,1,0,0,0):raise ValueError('Unsupported grid or vector orientation')
                key={'10u':'u','10v':'v','gust':'gust'}.get(m['shortName'])
                if not key or key in frame or m['units']!='m s**-1':raise ValueError('Unexpected/duplicate parameter or unit')
                if key in ('u','v') and (m['typeOfLevel'],m['level'])!=('heightAboveGround',10):raise ValueError('Wrong wind height')
                if key=='gust' and m['typeOfLevel']!='surface':raise ValueError('Wrong gust level')
                values=[round(float(v),4) for v in ec.codes_get_values(g)]
                if len(values)!=63 or not all(math.isfinite(v) and abs(v)<200 for v in values):raise ValueError('Missing or implausible field values')
                frame[key]=values;meta.append(m)
            finally:ec.codes_release(g)
    if set(frame)!= {'u','v','gust'}:raise ValueError('Incomplete vectors')
    frame.update(validAt=iso(run+timedelta(hours=step)),leadHours=step)
    return frame,meta

class NOAA:
    def __init__(self):self.requests=[];self.start=time.monotonic();self.last=0;self.bytes=0
    def get(self,url):
        if len(self.requests)>=MAX_REQUESTS:raise RuntimeError('Request budget exhausted')
        wait=max(0,10-(time.monotonic()-self.last)) if self.last else 0
        if time.monotonic()-self.start+wait+40>MAX_SECONDS:raise RuntimeError('Job deadline reached')
        time.sleep(wait);self.last=time.monotonic();request_started=self.last
        with tempfile.TemporaryDirectory() as temp:
            target=Path(temp)/'response'
            p=subprocess.run(['curl','--http1.1','-sS','--proto','=https','--max-time','40','--max-filesize','500000','-w','%{http_code}','-o',str(target),url],capture_output=True,text=True)
            data=target.read_bytes() if target.exists() else b''
        self.last=time.monotonic() # A full ten-second quiet gap after each completed fetch.
        status=int(p.stdout) if p.stdout.isdigit() else 0
        self.bytes+=len(data);self.requests.append({'url':url,'status':status,'bytes':len(data),'offsetSeconds':round(request_started-self.start,2)})
        if self.bytes>MAX_BYTES:raise RuntimeError('Transfer budget exceeded')
        if p.returncode or status not in (200,403,404):raise RuntimeError(f'NOAA HTTP {status}; no immediate retries')
        return status,data

def refresh(root,now,source,horizon=168,clock=lambda: datetime.now(UTC)):
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    with (root/'refresh.lock').open('w') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return {'status':'locked'}
        current_path=root/'current.json'
        current=json.loads(current_path.read_text()) if current_path.exists() else None
        slot=iso(now.replace(hour=now.hour//6*6,minute=0,second=0,microsecond=0))
        ledger_path=root/'attempts.json'
        ledger=json.loads(ledger_path.read_text()) if ledger_path.exists() else []
        ledger=[s for s in ledger if parse(s)>now-timedelta(hours=30)]
        if slot in ledger:return {'status':'already-attempted','slot':slot}
        ledger.append(slot);atomic(ledger_path,json.dumps(ledger).encode())
        run=None;wanted=[]
        for candidate in candidates(now):
            wanted=steps(candidate,now,horizon)
            if not wanted or len(wanted)>62:raise RuntimeError('Forecast package exceeds frame bound')
            cycle=candidate.strftime('%H');day=candidate.strftime('%Y%m%d')
            status,body=source.get(f'{BASE}/pub/data/nccf/com/gfs/prod/gfs.{day}/{cycle}/atmos/')
            if status==200 and complete_listing(body,cycle,wanted):run=candidate;break
        if run is None:raise RuntimeError('No complete sufficiently recent NOAA run')
        if current and parse(current['runAt'])>=run:return {'status':'unchanged','runAt':current['runAt']}
        frames=[];evidence=[];cpu=time.process_time()
        for step in wanted:
            params={'file':f'gfs.t{run:%H}z.pgrb2.0p25.f{step:03}','lev_10_m_above_ground':'on','lev_surface':'on','var_UGRD':'on','var_VGRD':'on','var_GUST':'on','subregion':'','leftlon':152.5,'rightlon':154,'toplat':-26,'bottomlat':-28,'dir':f'/gfs.{run:%Y%m%d}/{run:%H}/atmos'}
            url=BASE+'/cgi-bin/filter_gfs_0p25.pl?'+urlencode(params)
            status,raw=source.get(url)
            if status!=200:raise RuntimeError('Forecast disappeared during acquisition')
            frame,metadata=decode(raw,run,step);frames.append(frame)
            evidence.append({'leadHours':step,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),'metadata':metadata})
        published=clock()
        if published-run>timedelta(hours=18):raise RuntimeError('Run aged out before publication')
        data={'schema':1,'provider':'NOAA/NCEP','model':'GFS 0.25°','runAt':iso(run),'retrievedAt':iso(published),'expiresAt':iso(run+timedelta(hours=18)),'grid':GRID,'units':'m/s','vectorConvention':'earth-relative eastward u, northward v','temporalPolicy':'Exact native times only; no temporal interpolation','frames':frames}
        raw=(json.dumps(data,separators=(',',':'))+'\n').encode()
        if len(raw)>120_000:raise RuntimeError('JSON publication budget exceeded')
        if current:atomic(root/'previous.json',current_path.read_bytes())
        atomic(current_path,raw) # Readers see either complete old or complete new package.
        atomic(root/'provenance.json',json.dumps(evidence,indent=2).encode())
        return {'status':'published','runAt':data['runAt'],'frames':len(frames),'firstValid':frames[0]['validAt'],'lastValid':frames[-1]['validAt'],'jsonBytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'processCpuSeconds':round(time.process_time()-cpu,3)}

def main():
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);args=p.parse_args()
    root=Path(args.output).resolve()
    # Never target production or the checked-in snapshot automatically.
    if 'test-results' not in root.parts:raise SystemExit('Research mode only: output must be under test-results')
    source=NOAA();now=datetime.now(UTC)
    try:result=refresh(root,now,source)
    except Exception as e:result={'status':'failed','reason':str(e),'lastGoodPreserved':(root/'current.json').exists()}
    result.update(startedAt=iso(now),finishedAt=iso(datetime.now(UTC)),wallSeconds=round(time.monotonic()-source.start,2),requests=source.requests,requestCount=len(source.requests),downloadBytes=source.bytes)
    atomic(root/'last-attempt.json',json.dumps(result,indent=2).encode());print(json.dumps(result,indent=2))
    return 0 if result['status']!='failed' else 1
if __name__=='__main__':raise SystemExit(main())
