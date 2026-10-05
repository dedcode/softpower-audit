"""Deploy/start the Cloud Workflows continuation controller; no periodic trigger."""
import argparse
import json
import subprocess
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import google.auth
from google.auth.transport.requests import AuthorizedSession, Request
from google.cloud import storage
from google.api_core.exceptions import NotFound, PreconditionFailed

ROOT=Path(__file__).resolve().parents[1]
PROJECT='citygraph';REGION='us-central1';BUCKET=PROJECT+'-softpower-crawl'
PROJECT_NUMBER='1028219320071'
SA='softpower-crawl-coordinator@'+PROJECT+'.iam.gserviceaccount.com'
MEMBER='serviceAccount:'+SA


def checked(response):
    response.raise_for_status()
    return response.json()


def execution_belongs_to_workflow(name,workflow):
    # The executions API may canonicalize the same project ID to its immutable
    # project number. Accept these two identities, never an arbitrary project.
    if not isinstance(name,str):return False
    parts=name.split('/')
    return (len(parts)==8 and parts[0]=='projects' and parts[1] in (PROJECT,PROJECT_NUMBER)
        and parts[2:7]==['locations',REGION,'workflows',workflow,'executions'] and bool(parts[7]))


def add_binding(policy,role,member):
    bindings=policy.setdefault('bindings',[])
    binding=next((b for b in bindings if b['role']==role and not b.get('condition')),None)
    if binding is None:
        binding={'role':role,'members':[]};bindings.append(binding)
    if member not in binding['members']:binding['members'].append(member)


def provision(session,job):
    iam='https://iam.googleapis.com/v1/projects/'+PROJECT
    response=session.get(iam+'/serviceAccounts/'+SA,timeout=30)
    if response.status_code==404:
        response=session.post(iam+'/serviceAccounts',json={'accountId':'softpower-crawl-coordinator','serviceAccount':{'displayName':'Crawl checkpoint continuation'}},timeout=30)
    checked(response)
    for role_id,permissions in [
        ('softpowerCrawlJobRunner',['run.jobs.run','run.jobs.runWithOverrides','run.jobs.get','run.executions.get']),
        ('softpowerCrawlOperationReader',['run.operations.get']),
    ]:
        url=iam+'/roles/'+role_id
        response=session.get(url,timeout=30)
        role={'title':role_id,'description':'Least-privilege crawl continuation','includedPermissions':permissions,'stage':'GA'}
        if response.status_code==404:
            response=session.post(iam+'/roles',json={'roleId':role_id,'role':role},timeout=30)
        else:
            previous=checked(response)
            role['etag']=previous['etag']
            response=session.patch(url,json=role,timeout=30)
        checked(response)
    job_url=f'https://run.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/jobs/{job}'
    policy=checked(session.get(job_url+':getIamPolicy',params={'options.requestedPolicyVersion':3},timeout=30))
    policy['version']=3
    add_binding(policy,'projects/'+PROJECT+'/roles/softpowerCrawlJobRunner',MEMBER)
    checked(session.post(job_url+':setIamPolicy',json={'policy':policy},timeout=30))
    project_url='https://cloudresourcemanager.googleapis.com/v1/projects/'+PROJECT
    policy=checked(session.post(project_url+':getIamPolicy',json={'options':{'requestedPolicyVersion':3}},timeout=30))
    policy['version']=3
    add_binding(policy,'projects/'+PROJECT+'/roles/softpowerCrawlOperationReader',MEMBER)
    checked(session.post(project_url+':setIamPolicy',json={'policy':policy},timeout=30))
    bucket=storage.Client(project=PROJECT).bucket(BUCKET)
    policy=bucket.get_iam_policy(requested_policy_version=3);policy.version=3
    prefix=f'projects/_/buckets/{BUCKET}/objects/'
    condition={'title':'Crawl operational snapshots only','expression':f'(resource.name.startsWith("{prefix}runs/") && (resource.name.endsWith("/progress.json") || resource.name.endsWith("/STOP"))) || resource.name == "{prefix}control/worker-lease.json"'}
    binding={'role':'roles/storage.objectViewer','members':{MEMBER},'condition':condition}
    if not any(b['role']==binding['role'] and b.get('condition')==condition and MEMBER in b['members'] for b in policy.bindings):
        policy.bindings.append(binding);bucket.set_iam_policy(policy)


def start(session,workflow,run_id):
    # A launch is explicit, once per collection. The workflow loops only after
    # a checkpointed platform rollover, never on a wall-clock schedule.
    parent=f'projects/{PROJECT}/locations/{REGION}/workflows/{workflow}'
    url='https://workflowexecutions.googleapis.com/v1/'+parent+'/executions'
    params={'filter':'state="ACTIVE" OR state="QUEUED"','pageSize':100,'view':'FULL'}
    while True:
        page=checked(session.get(url,params=params,timeout=30))
        for execution in page.get('executions',[]):
            if execution.get('state') not in ('ACTIVE','QUEUED'):continue
            try:argument=json.loads(execution['argument'])
            except (KeyError,TypeError,ValueError) as exc:
                raise RuntimeError('Cannot verify an existing coordinator argument; refusing a duplicate launch') from exc
            if not isinstance(argument,dict):raise RuntimeError('Cannot verify an existing coordinator argument; refusing a duplicate launch')
            if argument.get('run_id')==run_id:
                print(json.dumps({'existing_execution':execution['name'],'state':execution['state']}));return execution
            raise RuntimeError('Another collection already has an active or queued coordinator: '+execution['name'])
        if not page.get('nextPageToken'):break
        params['pageToken']=page['nextPageToken']
    bucket=storage.Client(project=PROJECT).bucket(BUCKET)
    lease=bucket.blob('control/worker-lease.json')
    if lease.exists():raise RuntimeError('A crawler lease exists; inspect the current execution before starting another controller')
    # List + create alone is racy and a timed-out POST may already have created
    # an execution. Retain a generation-guarded launch record until its known
    # execution is terminal; an ambiguous request must never be blindly retried.
    claim=bucket.blob('control/coordinator-launch-'+workflow+'.json')
    generation=0
    try:
        claim.reload();generation=claim.generation
        previous=json.loads(claim.download_as_text(if_generation_match=generation))
        execution_name=previous.get('execution')
        if not execution_belongs_to_workflow(execution_name,workflow):
            raise RuntimeError('An earlier coordinator launch is pending or uncertain; inspect executions before retrying')
        execution=checked(session.get('https://workflowexecutions.googleapis.com/v1/'+execution_name,params={'view':'FULL'},timeout=30))
        if execution.get('state') in ('ACTIVE','QUEUED'):
            if previous.get('run_id')!=run_id:raise RuntimeError('Another collection already has a coordinator: '+execution_name)
            print(json.dumps({'existing_execution':execution_name,'state':execution['state']}));return execution
        if execution.get('state') not in ('SUCCEEDED','FAILED','CANCELLED'):
            raise RuntimeError('The earlier coordinator state is uncertain; inspect it before starting another')
    except NotFound:
        # Only a missing claim is safe. An HTTP failure while checking a known
        # execution propagates above, leaving the claim untouched.
        generation=0
    payload={'run_id':run_id,'workflow':workflow,'launch_id':uuid.uuid4().hex,'created_at':datetime.now(timezone.utc).isoformat(),'state':'creating'}
    try:claim.upload_from_string(json.dumps(payload),content_type='application/json',if_generation_match=generation,timeout=30)
    except PreconditionFailed as exc:raise RuntimeError('Another launcher claimed this coordinator; inspect executions instead of creating a duplicate') from exc
    generation=claim.generation
    response=checked(session.post(url,json={'argument':json.dumps({'run_id':run_id})},timeout=60))
    if not execution_belongs_to_workflow(response.get('name'),workflow):
        raise RuntimeError('Coordinator creation returned no verifiable execution; the launch claim was retained')
    payload.update(state='created',execution=response['name'])
    claim.upload_from_string(json.dumps(payload),content_type='application/json',if_generation_match=generation,timeout=30)
    print(json.dumps({k:response.get(k) for k in ['name','state','startTime']}))
    return response


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--deploy',action='store_true');parser.add_argument('--execute',action='store_true')
    parser.add_argument('--workflow',default='softpower-crawl-continuation')
    parser.add_argument('--job',default='softpower-crawler');parser.add_argument('--run-id')
    args=parser.parse_args()
    if args.execute and not args.run_id:parser.error('--execute requires --run-id')
    if not args.deploy and not args.execute:parser.error('Specify --deploy and/or --execute')
    credentials,_=google.auth.default();session=AuthorizedSession(credentials)
    if args.deploy:
        credentials.refresh(Request())
        with tempfile.TemporaryDirectory(prefix='crawl-coordinator-') as directory:
            token=Path(directory)/'token';token.write_text(credentials.token);token.chmod(0o600)
            base=['gcloud','--access-token-file='+str(token),'--project='+PROJECT]
            subprocess.run(base+['services','enable','workflows.googleapis.com','workflowexecutions.googleapis.com','--quiet'],check=True)
            provision(session,args.job)
            subprocess.run(base+['workflows','deploy',args.workflow,'--location='+REGION,'--service-account='+SA,
                '--source='+str(ROOT/'workflows/continue-crawl.yaml'),
                '--set-env-vars='+f'CRAWL_JOB_NAME={args.job},CRAWL_BUCKET={BUCKET},CRAWL_PROJECT={PROJECT},CRAWL_REGION={REGION}',
                '--format=json(name,revisionId,state)','--quiet'],check=True)
    if args.execute:start(session,args.workflow,args.run_id)


if __name__=='__main__':main()
