"""Opt-in local Chromium checks; fixtures never contact publisher websites."""
import io
import json
import os
import sys
import tempfile
import threading
import unittest
from collections import Counter
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from isolated_task import render

CHROME = os.environ.get('CRAWL_TEST_CHROME')


@unittest.skipUnless(CHROME and Path(CHROME).is_file(), 'Set CRAWL_TEST_CHROME for local browser fixtures')
class LiveBrowserRedirectTests(unittest.TestCase):
    def setUp(self):
        self.served = []
        served = self.served
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                served.append(self.path)
                redirects = {'/start': '/hop', '/hop': '/article',
                             '/script-start': '/script', '/xhr-start': '/xhr',
                             '/denied-start': '/forbidden'}
                if self.path in redirects:
                    self.send_response(302)
                    self.send_header('Location', redirects[self.path])
                    self.end_headers()
                    return
                content_type = 'text/html'
                if self.path == '/article':
                    body = '''<html><body><h1>Local test article</h1><p id="status">loading</p>
                        <script src="/script-start"></script><iframe src="/iframe"></iframe>
                        <img src="/image"><script>
                        window.open('/popup');
                        const a=document.createElement('a');a.href='/anchor-popup';a.target='_blank';
                        document.body.appendChild(a);a.click();
                        </script></body></html>'''
                elif self.path == '/script':
                    content_type = 'application/javascript'
                    body = "fetch('/xhr-start').then(r=>r.json()).then(x=>document.querySelector('#status').textContent=x.message);"
                elif self.path == '/xhr':
                    content_type = 'application/json'
                    body = '{"message":"article-ready"}'
                elif self.path == '/rate-limit':
                    self.send_response(429)
                    self.send_header('Retry-After', '13')
                    self.end_headers()
                    return
                else:
                    body = '<html><body>Unexpected auxiliary request</body></html>'
                data = body.encode()
                self.send_response(200)
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.origin = 'http://127.0.0.1:' + str(self.server.server_port)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def run_render(self, path, denied=()):
        from playwright.sync_api import sync_playwright as real_playwright
        events = []
        output = io.StringIO()
        origin = self.origin

        class Replies:
            def readline(self):
                event = json.loads(output.getvalue().splitlines()[-1])
                events.append(event)
                if event['type'] == 'response':
                    return '{"received":true}\n'
                # Loopback is permitted only in this isolated fixture. Production
                # still delegates every admitted URL to Pipeline's public_url.
                allowed = event['url'].startswith(origin + '/') and urlsplit(event['url']).path not in denied
                return json.dumps({'allowed': allowed, 'queue_wait_seconds': 0}) + '\n'

        @contextmanager
        def local_playwright():
            with real_playwright() as runtime:
                def launch(**kwargs):
                    return runtime.chromium.launch(executable_path=CHROME, **kwargs)
                yield SimpleNamespace(chromium=SimpleNamespace(launch=launch))

        self.events = events
        with tempfile.TemporaryDirectory() as directory:
            with patch('playwright.sync_api.sync_playwright', local_playwright), \
                    patch('isolated_task.sys.stdout', output), \
                    patch('isolated_task.sys.stdin', Replies()):
                result = render(Path(directory), origin + path)
            return result, (Path(directory) / 'rendered.html').read_text(), events

    def test_redirect_hops_and_script_xhr_authorized_once_subframes_popups_denied(self):
        result, body, events = self.run_render('/start')
        self.assertEqual(result['url'], self.origin + '/article')
        self.assertIn('article-ready', body)
        authorized = Counter(urlsplit(e['url']).path for e in events if e['type'] == 'authorize')
        served = Counter(self.served)
        for path in ('/start', '/hop', '/article', '/script-start', '/script', '/xhr-start', '/xhr'):
            self.assertEqual(authorized[path], 1, (path, events))
            self.assertEqual(served[path], 1, (path, self.served))
        for path in ('/iframe', '/popup', '/anchor-popup', '/image'):
            self.assertEqual(served[path], 0, (path, self.served))
        for path, count in served.items():
            self.assertEqual(authorized[path], count, (path, events))
        documents = [urlsplit(e['url']).path for e in events if e.get('document')]
        self.assertEqual(documents, ['/start', '/hop', '/article'])

    def test_denied_redirect_target_never_reaches_server(self):
        with self.assertRaises(Exception):
            self.run_render('/denied-start', denied={'/forbidden'})
        self.assertEqual(self.served, ['/denied-start'])
        self.assertEqual([urlsplit(e['url']).path for e in self.events], ['/denied-start', '/forbidden'])

    def test_rate_limit_response_is_reported_to_parent(self):
        from playwright.sync_api import Error as BrowserError
        # Chromium may reject an empty 429 during goto before render checks its
        # status. The response must still reach the parent in either case.
        with self.assertRaisesRegex((RuntimeError, BrowserError), 'HTTP 200|ERR_HTTP_RESPONSE_CODE_FAILURE'):
            self.run_render('/rate-limit')
        responses = [event for event in self.events if event['type'] == 'response']
        self.assertEqual(responses, [{'type': 'response', 'url': self.origin + '/rate-limit',
                                      'status': 429, 'retry_after': '13'}])


if __name__ == '__main__':
    unittest.main()
