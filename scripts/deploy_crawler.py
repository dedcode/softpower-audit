"""Deploy the approved Cloud Run Job. Execution requires a separate --execute flag."""
import argparse,json,subprocess,tempfile
from pathlib import Path
import google.auth
from google.auth.transport.requests import Request
ROOT=Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser();p.add_argument('--run-id',required=True);p.add_argument('--deploy',action='store_true');p.add_argument('--execute',action='store_true');args=p.parse_args()
if not args.deploy and not args.execute:
 print('No changes requested. Use --deploy and/or --execute after approving cloud setup.');raise SystemExit(0)
credentials,_=google.auth.default();credentials.refresh(Request())
with tempfile.TemporaryDirectory(prefix='crawler-deploy-') as tmp:
 token=Path(tmp)/'token';token.write_text(credentials.token);token.chmod(0o600)
 base=['gcloud','--access-token-file='+str(token),'--project=citygraph']
 def command(parts):subprocess.run(base+parts,check=True)
 if args.deploy:
  image='us-central1-docker.pkg.dev/citygraph/cloud-run-source-deploy/softpower-crawler:'+args.run_id
  build=Path(tmp)/'build.json'
  build.write_text(json.dumps({'steps':[{'name':'gcr.io/cloud-builders/docker','args':['build','-t',image,'.']}],'images':[image],'options':{'logging':'CLOUD_LOGGING_ONLY'}}))
  command(['builds','submit',str(ROOT/'crawler'),'--config='+str(build),'--region=us-central1','--service-account=projects/citygraph/serviceAccounts/softpower-audit-builder@citygraph.iam.gserviceaccount.com','--quiet'])
  command(['run','jobs','deploy','softpower-crawler','--image='+image,'--region=us-central1','--service-account=softpower-crawler@citygraph.iam.gserviceaccount.com','--tasks=1','--parallelism=1','--max-retries=0','--task-timeout=86400','--cpu=1','--memory=1Gi','--set-env-vars=CRAWL_BUCKET=citygraph-softpower-crawl,RUN_ID='+args.run_id,'--quiet'])
 if args.execute:command(['run','jobs','execute','softpower-crawler','--region=us-central1','--update-env-vars=RUN_ID='+args.run_id,'--async','--format=json'])
