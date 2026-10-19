"""Generic public-weather runner. External writes are confined to the reviewed workflow."""
import argparse, hashlib, json, os, shutil, tempfile, time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from validate import validate, site_check, VALIDATOR, parse
UTC=timezone.utc

def iso(t): return t.isoformat(timespec='seconds').replace('+00:00','Z')
def read(path): return json.loads(Path(path).read_text())
def write(path,data):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(data,indent=2)+'\n')
def next_due(now):
    return min(t for day in range(2) for h in (4,10,16,22) if (t:=(now+timedelta(days=day)).replace(hour=h,minute=40,second=0,microsecond=0))>now)
def reserve(root,now,job):
    root=Path(root);c=read(root/'state/control.json')
    if c['schema']!=1: raise ValueError('Invalid control state')
    # Reserve the most recent scheduled opportunity, including delayed/manual runs.
    # UTC model-cycle boundaries would let a setup run suppress the next scheduled job.
    slot=iso(next_due(now)-timedelta(hours=6))
    c['attempts']=[a for a in c['attempts'] if parse(a['startedAt'])>now-timedelta(days=32)]
    if any(iso(next_due(parse(a['startedAt']))-timedelta(hours=6))==slot for a in c['attempts']): return False
    c['attempts'].append({'slot':slot,'job':job,'startedAt':iso(now),'status':'reserved'})
    write(root/'state/control.json',c)
    return True

def prepare_site(root,raw,status):
    root=Path(root)
    if (root/'public/manifest.json').exists(): site_check(root/'public')
    # Build a complete directory before Git/Pages can see it; never mix staged files.
    with tempfile.TemporaryDirectory(dir=root) as temp:
        out=Path(temp)/'public';out.mkdir()
        for name in ('index.html','404.html','_headers'): shutil.copy(root/'assets'/name,out/name)
        write(out/'status.json',status)
        if raw is None:
            write(out/'manifest.json',{'schema':1,'available':False,'reason':'No verified forecast published yet'})
        else:
            d=validate(raw);digest=hashlib.sha256(raw).hexdigest()
            name=f"runs/gfs-{parse(d['runAt']):%Y%m%d%H}-{digest[:16]}.json"
            (out/'runs').mkdir();(out/name).write_bytes(raw);(out/'current.json').write_bytes(raw)
            # Keep one prior version so an older cached manifest remains usable.
            old_manifest=root/'public/manifest.json'
            if old_manifest.exists():
                old=read(old_manifest)
                if old.get('available') and old['path']!=name:
                    previous=(root/'public'/old['path']).read_bytes();validate(previous)
                    if hashlib.sha256(previous).hexdigest()!=old['sha256']: raise ValueError('Corrupt previous version')
                    (out/old['path']).write_bytes(previous)
            write(out/'manifest.json',{'schema':1,'available':True,'path':name,'sha256':digest,'bytes':len(raw),'validator':VALIDATOR,'model':d['model'],'provider':d['provider'],'runAt':d['runAt'],'retrievedAt':d['retrievedAt'],'expiresAt':d['expiresAt'],'firstValid':d['frames'][0]['validAt'],'lastValid':d['frames'][-1]['validAt'],'frames':len(d['frames'])})
        measured=site_check(out)
        # This remains a local worktree. One Git commit / Pages deployment publishes all files.
        existing=root/'public'
        if existing.exists(): shutil.rmtree(existing)
        shutil.copytree(out,existing)
        return measured

def run(root,now,job,source=None,clock=None):
    import refresh
    clock=clock or (lambda:datetime.now(UTC));root=Path(root)
    control=read(root/'state/control.json')
    matches=[a for a in control['attempts'] if a['job']==job]
    if len(matches)!=1 or matches[0]['status']!='reserved': raise ValueError('Missing unique reserved attempt')
    source=source or refresh.NOAA();start=time.monotonic()
    old=(root/'public/current.json').read_bytes() if (root/'public/current.json').exists() else None
    if old: validate(old)
    raw=old
    try:
        with tempfile.TemporaryDirectory() as temp:
            if old: (Path(temp)/'current.json').write_bytes(old)
            result=refresh.refresh(temp,now,source,clock=clock)
            if result['status']=='published':
                candidate=(Path(temp)/'current.json').read_bytes();d=validate(candidate,clock(),True)
                if old and parse(d['runAt'])<=parse(validate(old)['runAt']): raise ValueError('Model rollback rejected')
                raw=candidate
            elif result['status']!='unchanged': raise ValueError('Unexpected refresh result')
            if raw is None: raise ValueError('No verified data')
            validate(raw,clock(),True)
    except Exception as e:
        result={'status':'failed','reason':str(e),'lastGoodPreserved':old is not None};raw=old
    finished=clock()
    result.update(wallSeconds=round(time.monotonic()-start,2),requestCount=len(getattr(source,'requests',[])),downloadBytes=getattr(source,'bytes',0))
    attempt=matches[0];attempt.update(status=result['status'],finishedAt=iso(finished),result=result)
    current=validate(raw) if raw else None
    status={'schema':1,'lastAttemptAt':iso(now),'finishedAt':iso(finished),'nextExpectedAttemptAt':iso(next_due(now)),'missedAfter':iso(next_due(now)+timedelta(hours=1)),'result':result,'runAt':current['runAt'] if current else None,'expiresAt':current['expiresAt'] if current else None,'job':job,'source':'NOAA/NCEP','validator':VALIDATOR,'schedule':'40 4,10,16,22 * * * (UTC)'}
    measured=prepare_site(root,raw,status)
    control['history']=(control.get('history',[])+[status])[-128:]
    write(root/'state/control.json',control)
    report={'result':result,'publication':measured,'status':status}
    write(root/'test-results/run.json',report)
    return report

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['reserve','run','bootstrap']);p.add_argument('--root',default='.');p.add_argument('--job',default=os.getenv('GITHUB_RUN_ID','local'));a=p.parse_args();now=datetime.now(UTC)
    if a.action=='reserve':
        ok=reserve(a.root,now,a.job);print('reserved' if ok else 'already-attempted')
        if os.getenv('GITHUB_OUTPUT'):
            with open(os.environ['GITHUB_OUTPUT'],'a') as f:f.write(f'run={str(ok).lower()}\n')
    elif a.action=='run':
        report=run(a.root,now,a.job);print(json.dumps(report,indent=2))
        if os.getenv('GITHUB_STEP_SUMMARY'):
            with open(os.environ['GITHUB_STEP_SUMMARY'],'a') as f:f.write('## Public NOAA refresh\n```json\n'+json.dumps(report,indent=2)+'\n```\n')
        # Workflow persists failure status before failing visibly.
    else:
        prepare_site(a.root,None,{'schema':1,'result':{'status':'not-started'},'nextExpectedAttemptAt':None,'missedAfter':None,'expiresAt':None})
