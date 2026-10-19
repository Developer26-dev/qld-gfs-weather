"""Bounded, read-only publication verification. No credentials sent to the public feed."""
import argparse, hashlib, json, time, urllib.request, urllib.error
from pathlib import Path
from urllib.parse import urlparse
from validate import validate

def get(url,headers=None):
    req=urllib.request.Request(url,headers={'User-Agent':'qld-gfs-weather-verifier/1.0 (+https://github.com/Developer26-dev/qld-gfs-weather)',**(headers or {})})
    try:
        with urllib.request.urlopen(req,timeout=20) as r:
            body=r.read(120001)
            if len(body)>120000: raise ValueError('Oversize public response')
            return r.status,r.headers,body
    except urllib.error.HTTPError as e:
        if e.code==304:return e.code,e.headers,b''
        raise

def verify(base,site,wait=0,local=False):
    u=urlparse(base)
    if u.scheme!='https' and not(local and u.scheme=='http' and u.hostname in ('127.0.0.1','localhost')):raise ValueError('HTTPS required')
    if u.username or u.password or u.query or u.fragment:raise ValueError('Public base URL only')
    expected=json.loads((Path(site)/'manifest.json').read_text());status_expected=json.loads((Path(site)/'status.json').read_text())
    deadline=time.monotonic()+min(wait,600);attempts=0;transferred=0
    while True:
        attempts+=1
        try:
            code,h,body=get(base.rstrip('/')+'/manifest.json');transferred+=len(body);m=json.loads(body)
            if code!=200 or m!=expected:raise ValueError('Publication not visible yet')
            if h.get('Access-Control-Allow-Origin')!='*' or 'must-revalidate' not in h.get('Cache-Control','') or not h.get('ETag'):raise ValueError('Manifest cache/CORS contract')
            code,_,status_body=get(base.rstrip('/')+'/status.json');transferred+=len(status_body)
            if code!=200 or json.loads(status_body)!=status_expected:raise ValueError('Status publication mismatch')
            if m.get('available'):
                code,h,raw=get(base.rstrip('/')+'/'+m['path']);transferred+=len(raw)
                if code!=200 or len(raw)!=m['bytes'] or hashlib.sha256(raw).hexdigest()!=m['sha256']:raise ValueError('Remote grid hash mismatch')
                validate(raw)
                if h.get('Access-Control-Allow-Origin')!='*' or h.get('Cache-Control','').strip()!='public, max-age=31536000, immutable' or not h.get('ETag'):raise ValueError('Grid cache/CORS contract')
                code,_,_=get(base.rstrip('/')+'/'+m['path'],{'If-None-Match':h['ETag']})
                if code!=304:raise ValueError('ETag conditional retrieval failed')
            return {'verified':True,'attempts':attempts,'bytesReceived':transferred,'runAt':m.get('runAt'),'url':base,'https':u.scheme=='https','localSimulation':local}
        except (OSError,ValueError,KeyError) as e:
            if time.monotonic()+30>deadline:raise RuntimeError(f'Publication unverified after {attempts} attempts: {e}') from e
            time.sleep(30)
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('url');p.add_argument('--site',default='public');p.add_argument('--wait',type=int,default=0);p.add_argument('--local',action='store_true');a=p.parse_args();print(json.dumps(verify(a.url,a.site,a.wait,a.local),indent=2))
