"""Bounded hourly catch-up candidate. Not active until the workflow is approved.
Reuse the existing decoder, validator, atomic publisher and remote verifier.
No NOAA call on a no-op; reserve model attempts durably BEFORE any GRIB download.
"""
import argparse, hashlib, json, os, subprocess, tempfile, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from pipeline import read, write, iso, prepare_site, next_due as cycle_due
from validate import validate, site_check, parse

UTC = timezone.utc
LEASE = timedelta(minutes=45) # Longer than the existing 30-minute job limit.
GAP = timedelta(minutes=55)
MAX_PROBES = 3               # Per original six-hour opportunity.
MAX_MODEL_ATTEMPTS = 2       # Includes interrupted reservations, not just successes.
MAX_FULL_DAY = 8             # Rolling 24-hour cap, including failed/interrupted downloads.
MAX_PUBLICATIONS = 3         # Per six-hour window: <=375 builds across any 31-day span.

def next_check(now):
    t = now.replace(minute=40, second=0, microsecond=0)
    return t if t > now else t + timedelta(hours=1)

def slot(now): return iso(cycle_due(now) - timedelta(hours=6))
def path(root): return Path(root) / 'state/recovery.json'
def load(root):
    p = path(root)
    state = read(p) if p.exists() else {'schema': 1, 'attempts': [], 'pending': None}
    if state['schema'] != 1: raise ValueError('Unknown recovery schema')
    return state

def save(root, state): write(path(root), state)
def fingerprint(root):
    return hashlib.sha256(b''.join((Path(root)/'public'/p).read_bytes() for p in ['manifest.json','status.json'])).hexdigest()

def plan(root, now, job):
    """Pure local preflight. Workflow commits any reservation before executing it."""
    state = load(root)
    state['attempts'] = [a for a in state['attempts'] if parse(a['startedAt']) > now-timedelta(hours=48)]
    attempts = state['attempts']
    if any(a['job'] == job for a in attempts): return {'action':'none','reason':'Job already reserved'}
    if any(a['status'] == 'reserved' and now-parse(a['startedAt']) < LEASE for a in attempts):
        return {'action':'none','reason':'Reservation lease active'}
    if attempts and now-parse(attempts[-1]['startedAt']) < GAP:
        return {'action':'none','reason':'Hourly backoff'}
    for a in attempts:
        if a['status'] == 'reserved': a['status']='interrupted'; a['finishedAt']=iso(now)
    same = [a for a in attempts if a['slot'] == slot(now)]
    if sum(a.get('publicationReserved',False) for a in same) >= MAX_PUBLICATIONS:
        save(root,state);return {'action':'none','reason':'Publication budget; next six-hour window'}
    pending = state.get('pending')
    if pending and pending['fingerprint'] != fingerprint(root): raise ValueError('Pending site changed outside recovery ledger')
    # Bootstrap verifies the existing public bytes without NOAA or a Pages build.
    action = 'verify' if not state.get('bootstrapped') else 'acquire'
    if pending:
        if now < parse(pending['expiresAt']): action='republish'
        else: state['pending']=None # Expired bytes are not republished as recovery.
    if action == 'acquire':
        if any(a.get('advanced') and a.get('verified') for a in same):
            save(root,state);return {'action':'none','reason':'This six-hour window already advanced'}
        if sum(a['action']=='acquire' for a in same)>=MAX_PROBES:
            save(root,state);return {'action':'none','reason':'Discovery budget; next six-hour window'}
        if sum(bool(a.get('modelRun')) for a in attempts if parse(a['startedAt'])>now-timedelta(hours=24))>=MAX_FULL_DAY:
            save(root,state);return {'action':'none','reason':'Rolling acquisition budget'}
    a = {'job':job,'slot':slot(now),'startedAt':iso(now),'status':'reserved','action':action,
         'publicationReserved':action=='republish'}
    attempts.append(a);save(root,state)
    return {'action':action,'reason':'Reserved before external work'}

def model_reservation(root, job, run, now, persist):
    state=load(root);a=next(a for a in state['attempts'] if a['job']==job)
    if a['status']!='reserved' or a['action']!='acquire' or a.get('modelRun'):
        raise ValueError('No unique acquisition reservation')
    if sum(x.get('modelRun')==run for x in state['attempts'])>=MAX_MODEL_ATTEMPTS:
        raise ValueError('Model acquisition retry budget exhausted')
    if sum(bool(x.get('modelRun')) for x in state['attempts'] if parse(x['startedAt'])>now-timedelta(hours=24))>=MAX_FULL_DAY:
        raise ValueError('Rolling acquisition budget exhausted')
    a['modelRun']=run;a['modelReservedAt']=iso(now);save(root,state)
    persist() # Fail closed: a failed Git push means NO GRIB request follows.

class GuardedSource:
    def __init__(self,source,root,job,clock,persist):
        self.source=source;self.root=root;self.job=job;self.clock=clock;self.persist=persist;self.selected=False
    @property
    def requests(self): return self.source.requests
    @property
    def bytes(self): return self.source.bytes
    def get(self,url):
        if '/cgi-bin/filter_gfs_0p25.pl?' in url and not self.selected:
            d=parse_qs(urlparse(url).query)['dir'][0].split('/')
            run=datetime.strptime(d[1]+d[2],'gfs.%Y%m%d%H').replace(tzinfo=UTC)
            model_reservation(self.root,self.job,iso(run),self.clock(),self.persist);self.selected=True
        return self.source.get(url)

def acquire(root, now, job, source=None, clock=None, persist=lambda:None):
    import refresh
    root=Path(root);clock=clock or (lambda:datetime.now(UTC));started=time.monotonic();cpu=time.process_time()
    state=load(root);a=next(a for a in state['attempts'] if a['job']==job)
    if a['status']!='reserved' or a['action']!='acquire':raise ValueError('Missing acquisition reservation')
    old=(root/'public/current.json').read_bytes() if (root/'public/current.json').exists() else None
    if old:validate(old)
    source=GuardedSource(source or refresh.NOAA(),root,job,clock,persist);raw=old
    try:
        with tempfile.TemporaryDirectory() as temp:
            if old:(Path(temp)/'current.json').write_bytes(old)
            result=refresh.refresh(temp,now,source,clock=clock)
            if result['status']=='published':
                candidate=(Path(temp)/'current.json').read_bytes();d=validate(candidate,clock(),True)
                if old and parse(d['runAt'])<=parse(validate(old)['runAt']):raise ValueError('Model rollback rejected')
                raw=candidate
            elif result['status']!='unchanged':raise ValueError('Unexpected refresh result')
            if raw is None:raise ValueError('No verified data')
            validate(raw,clock(),True)
    except Exception as e:result={'status':'failed','reason':str(e),'lastGoodPreserved':old is not None};raw=old
    finished=clock();result.update(requestCount=len(source.requests),downloadBytes=source.bytes,
                                  wallSeconds=round(time.monotonic()-started,2),
                                  attemptCpuSeconds=round(time.process_time()-cpu,3))
    # Reload because the pre-download reservation was persisted separately.
    state=load(root);a=next(a for a in state['attempts'] if a['job']==job)
    a.update(status=result['status'],finishedAt=iso(finished),result=result,advanced=result['status']=='published')
    if result['status']=='unchanged':save(root,state);return {'publish':False,'result':result}
    current=validate(raw) if raw else None
    status={'schema':1,'lastAttemptAt':iso(now),'finishedAt':iso(finished),
            'nextCheckAt':iso(next_check(finished)),
            'nextExpectedAttemptAt':iso(cycle_due(finished)),'missedAfter':iso(cycle_due(finished)+timedelta(hours=1)),
            'result':result,'runAt':current['runAt'] if current else None,'expiresAt':current['expiresAt'] if current else None,
            'job':job,'source':'NOAA/NCEP','validator':'qld-gfs-1','schedule':'40 * * * * (UTC); bounded six-hour acquisition windows',
            'attemptLedger':'https://github.com/Developer26-dev/qld-gfs-weather/blob/main/state/recovery.json'}
    prepare_site(root,raw,status) # Existing complete-site validation/atomic staging.
    a['publicationReserved']=True
    state['pending']={'fingerprint':fingerprint(root),'expiresAt':status['expiresAt'] or iso(finished),'job':job}
    save(root,state)
    return {'publish':True,'result':result}

def verified(root, now, job, success):
    state=load(root);a=next(a for a in state['attempts'] if a['job']==job)
    a.update(verified=success,verificationAt=iso(now))
    if success:
        if state.get('pending'):
            original=next(x for x in state['attempts'] if x['job']==state['pending']['job'])
            original['verified']=True
        state['pending']=None;state['bootstrapped']=True
        if a['status']=='reserved':a['status']='verified';a['finishedAt']=iso(now)
    else:
        if a['action']=='verify': # Existing bytes not hosted: retry without any NOAA acquisition.
            m=read(Path(root)/'public/manifest.json')
            state['pending']={'fingerprint':fingerprint(root),'expiresAt':m.get('expiresAt') or iso(now),'job':job}
        a['status']='publication-failed';a['finishedAt']=iso(now)
    save(root,state)

def persist_git(root):
    if os.environ.get('GITHUB_REPOSITORY')!='Developer26-dev/qld-gfs-weather' or os.environ.get('GITHUB_ACTIONS')!='true':
        raise RuntimeError('Remote ledger persistence only in the approved weather workflow')
    for args in [('add','state/recovery.json'),('commit','-m','[CF-Pages-Skip] Reserve bounded NOAA model attempt'),('push','origin','HEAD:main')]:
        subprocess.run(['git',*args],cwd=root,check=True)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['plan','acquire','verified','verification-failed','result']);args=parser.parse_args()
    root=Path.cwd();now=datetime.now(UTC);job=os.environ.get('GITHUB_RUN_ID','local')
    if args.action=='plan':r=plan(root,now,job)
    elif args.action=='acquire':r=acquire(root,now,job,persist=lambda:persist_git(root))
    elif args.action in ('verified','verification-failed'):
        verified(root,now,job,args.action=='verified');r={'verified':args.action=='verified'}
    else:
        a=next(a for a in load(root)['attempts'] if a['job']==job)
        raise SystemExit(1 if a['status'] in ('failed','publication-failed') else 0)
    print(json.dumps(r,indent=2))
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'],'a') as f:
            if 'action' in r:f.write('action='+r['action']+'\n')
            if 'publish' in r:f.write('publish='+str(r['publish']).lower()+'\n')
