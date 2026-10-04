"""Private subprocess entry point. No persistent browser; request approval is delegated to the parent."""
import json,sys
from pathlib import Path


def render(root,url):
    from playwright.sync_api import sync_playwright
    from crawl import UA,MAX_BODY
    def authorize(url,document):
        print(json.dumps({'type':'authorize','url':url,'document':document}),flush=True)
        return json.loads(sys.stdin.readline()).get('allowed',False)
    if not authorize(url,True):raise RuntimeError('Robots policy does not permit rendering')
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True,args=['--disable-dev-shm-usage'])
        try:
            context=browser.new_context(user_agent=UA,service_workers='block',accept_downloads=False)
            page=context.new_page();count=[0]
            def guard(route):
                req=route.request;count[0]+=1
                if count[0]>80 or req.resource_type in ('image','media','font','websocket'):return route.abort()
                try:
                    if not authorize(req.url,req.resource_type=='document'):return route.abort()
                    route.continue_()
                except Exception:route.abort()
            page.route('**/*',guard)
            response=page.goto(url,wait_until='domcontentloaded',timeout=35000)
            if response is None or response.status!=200:raise RuntimeError('Browser did not receive HTTP 200')
            try:page.wait_for_load_state('networkidle',timeout=8000)
            except Exception:pass
            body=page.content().encode()
            if len(body)>MAX_BODY:raise RuntimeError('Rendered document exceeds size limit')
            (root/'rendered.html').write_bytes(body)
            return {'url':page.url}
        finally:browser.close()

if __name__=='__main__':
    kind=sys.argv[1];root=Path(sys.argv[2]);request=json.loads((root/'request.json').read_text())
    try:
        if kind=='extract':
            if sys.platform=='linux':
                import resource
                resource.setrlimit(resource.RLIMIT_AS,(384*1024**2,384*1024**2))
            from extract import extract
            output=extract((root/'input.html').read_bytes(),request['url'])
        elif kind=='render':output=render(root,request['url'])
        else:raise ValueError('Unknown task')
    except Exception as exc:output={'error':type(exc).__name__+': '+str(exc)[:400]}
    (root/'output.json').write_text(json.dumps(output))
