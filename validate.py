"""Standard-library validation used on the runner AND before Pages publication."""
import hashlib, json, math, sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

GRID = {'west':152.5,'south':-28,'dx':.25,'dy':.25,'nx':7,'ny':9}
VALIDATOR = 'qld-gfs-1'
def parse(value):
    d=datetime.fromisoformat(value.replace('Z','+00:00'))
    if d.tzinfo is None: raise ValueError('Timestamp needs timezone')
    return d

def validate(raw, now=None, require_fresh=False):
    if len(raw)>120_000: raise ValueError('Dataset too large')
    d=json.loads(raw)
    if (d.get('schema'),d.get('provider'),d.get('model'),d.get('units'),d.get('vectorConvention')) != (1,'NOAA/NCEP','GFS 0.25°','m/s','earth-relative eastward u, northward v'):
        raise ValueError('Wrong schema, model, units or vectors')
    if d.get('grid')!=GRID: raise ValueError('Wrong grid')
    run,received=parse(d['runAt']),parse(d['retrievedAt'])
    if run.minute or run.second or run.hour%6: raise ValueError('Invalid model cycle')
    if received<run or parse(d['expiresAt'])!=run+timedelta(hours=18): raise ValueError('Invalid freshness metadata')
    if now is not None and (received>now+timedelta(minutes=5) or run>now): raise ValueError('Future dataset')
    if require_fresh and (now is None or now>parse(d['expiresAt'])): raise ValueError('Stale dataset')
    frames=d['frames']
    if not 56<=len(frames)<=62: raise ValueError('Incomplete forecast coverage')
    previous=-1
    for f in frames:
        lead=f['leadHours']
        if not isinstance(lead,int) or isinstance(lead,bool) or not previous<lead<=384: raise ValueError('Duplicate/unordered lead')
        if lead%3 and not (lead<=120 and (run.hour+lead+10)%24==6): raise ValueError('Non-native requested time')
        if parse(f['validAt'])!=run+timedelta(hours=lead): raise ValueError('Mixed valid time')
        previous=lead
        for k in ('u','v','gust'):
            a=f[k]
            if len(a)!=63 or not all(type(v) in (int,float) and math.isfinite(v) and abs(v)<200 for v in a): raise ValueError('Invalid field values')
    first,last=frames[0]['leadHours'],frames[-1]['leadHours']
    if last-first<165: raise ValueError('Short forecast coverage')
    expected=[h for h in range(first,last+1) if h%3==0 or (h<=120 and (run.hour+h+10)%24==6)]
    if [f['leadHours'] for f in frames]!=expected: raise ValueError('Missing native forecast frame')
    return d

def site_check(site):
    root=Path(site)
    files=[p for p in root.rglob('*') if p.is_file()]
    if any(p.is_symlink() for p in root.rglob('*')) or len(files)>12 or sum(p.stat().st_size for p in files)>500_000: raise ValueError('Unsafe publication size')
    allowed={'index.html','404.html','_headers','current.json','manifest.json','status.json'}
    for p in files:
        rel=p.relative_to(root).as_posix()
        if rel not in allowed and not (rel.startswith('runs/gfs-') and rel.endswith('.json')): raise ValueError('Non-weather publication file')
    status=json.loads((root/'status.json').read_text())
    if status['schema']!=1: raise ValueError('Invalid status')
    manifest=json.loads((root/'manifest.json').read_text())
    if manifest.get('available') is False:
        if (root/'current.json').exists(): raise ValueError('Hidden unreferenced current dataset')
        return {'files':len(files),'bytes':sum(p.stat().st_size for p in files),'available':False}
    path=manifest['path']
    if not path.startswith('runs/gfs-') or '/' in path[5:] or '..' in path: raise ValueError('Invalid version path')
    raw=(root/path).read_bytes()
    d=validate(raw) # Old last-good data may remain published; clients enforce expiresAt.
    if hashlib.sha256(raw).hexdigest()!=manifest['sha256'] or len(raw)!=manifest['bytes']: raise ValueError('Hash/size mismatch')
    if d['runAt']!=manifest['runAt'] or d['expiresAt']!=manifest['expiresAt'] or manifest['validator']!=VALIDATOR: raise ValueError('Manifest metadata mismatch')
    if raw!=(root/'current.json').read_bytes(): raise ValueError('Current alias mismatch')
    for p in (root/'runs').glob('*.json'): validate(p.read_bytes())
    return {'files':len(files),'bytes':sum(p.stat().st_size for p in files),'available':True,'runAt':d['runAt']}

if __name__=='__main__': print(json.dumps(site_check(sys.argv[1])))
