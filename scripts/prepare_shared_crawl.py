"""Import the existing crawl manifest and final checkpoints into the shared queue."""
import argparse,gzip,io,json,sys,uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from google.cloud import storage,firestore
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'crawler'))
from result_store import ResultIndex,iter_input_rows
from shared_queue import SharedQueue


def main():
    p=argparse.ArgumentParser();p.add_argument('--run-id',required=True);p.add_argument('--apply',action='store_true');p.add_argument('--verification',action='store_true');args=p.parse_args()
    if '/' in args.run_id:raise ValueError('Invalid run ID')
    if args.verification and not args.run_id.startswith('verify-distributed-'):raise ValueError('Isolated verification run required')
    b=storage.Client(project='citygraph').bucket('citygraph-softpower-crawl');prefix='runs/'+args.run_id+'/'
    if not args.apply:print(json.dumps({'run_id':args.run_id,'database':'softpower-crawl','apply':False}));return
    done=ResultIndex();refs={}
    if args.verification:
        rows=[{'url':f'https://source-{i%16}.invalid/article/{i}','outlet':f'source-{i%16}.invalid','source_table':'citygraph.softpower.china_articles','first_observed':'2025-01-01','last_observed':'2025-01-01'} for i in range(96)]
        cfg={'project':'citygraph','dataset':'citygraph.softpower_crawl','source_table':'citygraph.softpower.china_articles','run_id':args.run_id,'country':'ZZ','phase':'verification','input_count':len(rows),'workers':48,'per_outlet_workers':4,'delay_seconds':3,'max_attempts':3,'max_runtime_seconds':None,'max_response_bytes':42949672960,'max_total_attempts':300000}
        b.blob(prefix+'config.json').upload_from_string(json.dumps(cfg),content_type='application/json',if_generation_match=0)
    else:
        if b.blob('control/worker-lease.json').exists():raise RuntimeError('Drain the existing worker and wait for its lease to be released before importing')
        cfg=json.loads(b.blob(prefix+'config.json').download_as_text());assert cfg['run_id']==args.run_id
        previous=json.loads(b.blob(prefix+'progress.json').download_as_text())
        if previous.get('downloading'):raise RuntimeError('The prior worker still has active articles')
        checkpoints=list(b.list_blobs(prefix=prefix+'checkpoints/'))
        def read(blob):return blob.name,gzip.decompress(blob.download_as_bytes(timeout=60))
        with ThreadPoolExecutor(max_workers=12) as pool:
            for name,body in pool.map(read,checkpoints):
                for line in body.splitlines():
                    result=json.loads(line)
                    if done.record(result):refs[result['article_id']]='gs://'+b.name+'/'+name
        if len(done)!=previous['processed']:raise RuntimeError(f'Checkpoint count {len(done)} does not match final progress {previous["processed"]}')
        with b.blob(prefix+'inputs.json.gz').open('rb') as f,gzip.GzipFile(fileobj=f) as decoded,io.TextIOWrapper(decoded,encoding='utf-8') as stream:
            rows=list(iter_input_rows(stream))
    client=firestore.Client(project='citygraph',database='softpower-crawl')
    queue=SharedQueue(client,args.run_id,'initializer-'+uuid.uuid4().hex)
    queue.initialize(rows,done,cfg,result_refs=refs)
    summary=queue.summary()
    if summary['processed']!=len(done) or summary['total']!=cfg['input_count']:raise RuntimeError('Imported queue count mismatch')
    report={k:summary[k] for k in ['run_id','total','processed','pending','counts']}
    b.blob(prefix+'distributed/import.json').upload_from_string(json.dumps(report),content_type='application/json')
    print(json.dumps(report),flush=True)


if __name__=='__main__':main()
