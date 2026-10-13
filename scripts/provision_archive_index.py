"""Provision the service-filtered archive queue index; no crawler execution."""
import argparse,json,time
import google.auth
from google.auth.transport.requests import AuthorizedSession

FIELDS=[{'fieldPath':'archive_service','order':'ASCENDING'},
        {'fieldPath':'archive_due_at','order':'ASCENDING'},
        {'fieldPath':'__name__','order':'ASCENDING'}]
PARENT='projects/citygraph/databases/softpower-crawl/collectionGroups/articles/indexes'

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--apply',action='store_true');args=parser.parse_args()
    if not args.apply:print(json.dumps({'index':PARENT,'fields':FIELDS}));return
    s=AuthorizedSession(google.auth.default()[0]);url='https://firestore.googleapis.com/v1/'+PARENT
    r=s.get(url,timeout=30);r.raise_for_status()
    index=next((v for v in r.json().get('indexes',[]) if v.get('queryScope')=='COLLECTION' and v.get('fields')==FIELDS),None)
    if index is None:
        r=s.post(url,json={'queryScope':'COLLECTION','fields':FIELDS},timeout=30);r.raise_for_status()
        operation=r.json();print(json.dumps({'operation':operation['name']}),flush=True)
        for _ in range(180):
            r=s.get('https://firestore.googleapis.com/v1/'+operation['name'],timeout=30);r.raise_for_status();operation=r.json()
            if operation.get('done'):
                if operation.get('error'):raise RuntimeError(json.dumps(operation['error']))
                index=operation['response'];break
            time.sleep(5)
        else:raise TimeoutError('Index build has not finished; inspect existing operation')
    if index.get('state')!='READY':raise RuntimeError('Archive dispatch index is not ready')
    print(json.dumps({'index':index['name'],'state':index['state']}),flush=True)

if __name__=='__main__':main()
