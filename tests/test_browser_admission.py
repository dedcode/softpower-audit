"""CDP admission must happen once per actual request, including redirects."""
import io
import json
import sys
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from isolated_task import render, intercept_requests


class BrowserAdmissionTests(unittest.TestCase):
    def run_render(self, requests, admit=lambda *_: True):
        events = []
        admissions = []
        denied = set()
        output = io.StringIO()
        page = Mock()
        page.main_frame = object()
        page.url = [url for url, kind in requests if kind == 'document'][-1]
        page.content.return_value = '<article>Rendered article</article>'
        handler = [None]
        session = Mock()
        session.on.side_effect = lambda event, callback: handler.__setitem__(0, callback)
        def command(name, params=None):
            if name == 'Page.getFrameTree':
                return {'frameTree': {'frame': {'id': 'main'}}}
            if name == 'Fetch.failRequest':
                denied.add(params['requestId'])
            if name == 'Fetch.continueRequest':
                admissions.append(params['requestId'])
        session.send.side_effect = command

        def goto(*_, **__):
            for index, (url, kind) in enumerate(requests):
                request_id = str(index)
                handler[0]({'requestId': request_id, 'frameId': 'main',
                            'resourceType': kind.title(), 'request': {'url': url}})
                if request_id in denied and kind == 'document':
                    raise RuntimeError('Navigation request was denied')
            return SimpleNamespace(status=200)

        page.goto.side_effect = goto
        browser = Mock()
        browser.new_context.return_value.new_page.return_value = page
        browser.new_context.return_value.new_cdp_session.return_value = session
        chromium = Mock()
        chromium.launch.return_value = browser
        api = SimpleNamespace(sync_playwright=lambda: nullcontext(SimpleNamespace(chromium=chromium)),
                              TimeoutError=TimeoutError)

        class Replies:
            def readline(self):
                event = json.loads(output.getvalue().splitlines()[-1])
                events.append(event)
                allowed = admit(event['url'], event['document'])
                return json.dumps({'allowed': allowed, 'queue_wait_seconds': 0}) + '\n'

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(sys.modules, {'playwright.sync_api': api}), \
                    patch('isolated_task.sys.stdout', output), \
                    patch('isolated_task.sys.stdin', Replies()):
                try:
                    result = render(Path(directory), requests[0][0])
                finally:
                    browser.close.assert_called_once()
            self.assertEqual((Path(directory) / 'rendered.html').read_text(), page.content.return_value)
        return result, events, admissions

    def test_one_document_consumes_one_admission_not_a_preflight_and_navigation(self):
        url = 'https://example.org/story'
        result, events, admissions = self.run_render([(url, 'document')])
        self.assertEqual(result, {'url': url})
        self.assertEqual(events, [{'type': 'authorize', 'url': url, 'document': True}])
        self.assertEqual(admissions, ['0'])

    def test_each_document_hop_is_authorized_and_subresources_are_not_document_starts(self):
        requests = [('https://example.org/story', 'document'),
                    ('https://example.org/script.js', 'script'),
                    ('https://example.org/next-story', 'document')]
        _, events, admissions = self.run_render(requests)
        self.assertEqual([(e['url'], e['document']) for e in events],
                         [(url, kind == 'document') for url, kind in requests])
        self.assertEqual(admissions, ['0', '1', '2'])

    def test_denied_first_navigation_is_never_sent(self):
        admitted = []
        def deny(url, document):
            admitted.append((url, document))
            return False
        with self.assertRaisesRegex(RuntimeError, 'denied'):
            self.run_render([('https://example.org/story', 'document')], deny)
        self.assertEqual(admitted, [('https://example.org/story', True)])

    def test_parent_guard_error_denies_request_before_network(self):
        def expired(*_):
            raise RuntimeError('Article ownership lost')
        with self.assertRaisesRegex(RuntimeError, 'denied'):
            self.run_render([('https://example.org/story', 'document')], expired)

    def test_redirect_to_denied_destination_is_never_sent(self):
        seen = []
        def admit(url, document):
            seen.append((url, document))
            return '127.0.0.1' not in url
        with self.assertRaisesRegex(RuntimeError, 'denied'):
            self.run_render([('https://example.org/start', 'document'),
                             ('http://127.0.0.1/private', 'document')], admit)
        self.assertEqual(len(seen), 2)

    def test_request_cap_exclusions_and_subframe_denial(self):
        session = Mock()
        admit = Mock(return_value=True)
        intercept_requests(session, admit, 'main')
        callback = session.on.call_args.args[1]
        kinds = [('Document', 'child'), ('Image', 'main'), ('Media', 'main'),
                 ('Font', 'main'), ('WebSocket', 'main')] + [('Script', 'main')] * 76
        for index, (kind, frame) in enumerate(kinds):
            callback({'requestId': str(index), 'frameId': frame, 'resourceType': kind,
                      'request': {'url': 'https://example.org/' + str(index)}})
        commands = session.send.call_args_list
        denied = [call.args[1]['requestId'] for call in commands if call.args[0] == 'Fetch.failRequest']
        continued = [call for call in commands if call.args[0] == 'Fetch.continueRequest']
        self.assertEqual(denied, ['0', '1', '2', '3', '4', '80'])
        self.assertEqual(len(continued), 75)
        self.assertEqual(admit.call_count, 75)
        session.send.assert_any_call('Fetch.enable', {'patterns': [{'urlPattern': '*', 'requestStage': 'Request'}]})


if __name__ == '__main__':
    unittest.main()
