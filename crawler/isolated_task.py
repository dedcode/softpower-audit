"""Private subprocess entry point. No persistent browser; request approval is delegated to the parent."""
import json,sys,time
from pathlib import Path


def navigate(page,url,queue_wait_seconds,timeout_seconds=35):
    """Give navigation its normal work budget after measured host queue waits."""
    from playwright.sync_api import TimeoutError as BrowserTimeout
    responses=[]
    def capture(response):
        if response.request.is_navigation_request() and response.frame==page.main_frame:
            responses.append(response)
    page.on('response',capture)
    started=time.monotonic();queued_at_start=queue_wait_seconds();credited=0.
    try:
        try:return page.goto(url,wait_until='domcontentloaded',timeout=timeout_seconds*1000)
        except BrowserTimeout:
            while True:
                queued=max(0.,queue_wait_seconds()-queued_at_start)
                remaining=timeout_seconds-(time.monotonic()-started-queued)
                if queued<=credited or remaining<=0:raise
                credited=queued
                try:
                    # A timed-out goto can still be waiting for its first
                    # response. Waiting only for load state on about:blank
                    # would report success before navigation commits.
                    page.wait_for_url(lambda value:str(value)!='about:blank',
                        wait_until='domcontentloaded',timeout=remaining*1000)
                    return responses[-1] if responses else None
                except BrowserTimeout:continue
    finally:page.remove_listener('response',capture)


def intercept_requests(session,authorize,main_frame_id):
    """Pause each real request, including redirect hops skipped by page.route."""
    count=[0]
    def guard(event):
        count[0]+=1
        request_id=event['requestId']
        kind=event.get('resourceType','').lower()
        def deny():
            session.send('Fetch.failRequest',{'requestId':request_id,'errorReason':'BlockedByClient'})
        if (count[0]>80 or kind in ('image','media','font','websocket')
                or (kind=='document' and event.get('frameId')!=main_frame_id)):return deny()
        try:
            if not authorize(event['request']['url'],kind=='document'):return deny()
            session.send('Fetch.continueRequest',{'requestId':request_id})
        except Exception:deny()
    session.on('Fetch.requestPaused',guard)
    session.send('Fetch.enable',{'patterns':[{'urlPattern':'*','requestStage':'Request'}]})


def render(root,url):
    from playwright.sync_api import sync_playwright
    from crawl import UA,MAX_BODY
    queued=[0.]
    def authorize(url,document):
        print(json.dumps({'type':'authorize','url':url,'document':document}),flush=True)
        reply=json.loads(sys.stdin.readline())
        queued[0]+=max(0.,float(reply.get('queue_wait_seconds',0.)))
        return reply.get('allowed',False)
    # Request interception performs admission immediately before the
    # actual request. A separate preflight would consume a second host slot.
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True,args=['--disable-dev-shm-usage'],
                                  ignore_default_args=['--disable-popup-blocking'])
        try:
            context=browser.new_context(user_agent=UA,service_workers='block',accept_downloads=False)
            # The saved HTML is the main document only; auxiliary browsing
            # contexts add requests without contributing extracted article text.
            context.add_init_script("Object.defineProperty(window, 'open', {value: () => null, writable: false, configurable: false});")
            page=context.new_page()
            context.on('page',lambda popup:popup.close())
            def observe_response(response):
                if response.status not in (429,503):return
                print(json.dumps({'type':'response','url':response.url,'status':response.status,
                                  'retry_after':response.headers.get('retry-after')}),flush=True)
                if not json.loads(sys.stdin.readline()).get('received'):raise RuntimeError('Response coordination failed')
            page.on('response',observe_response)
            session=context.new_cdp_session(page)
            main_frame_id=session.send('Page.getFrameTree')['frameTree']['frame']['id']
            intercept_requests(session,authorize,main_frame_id)
            response=navigate(page,url,lambda:queued[0])
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
