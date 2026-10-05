"""Offline checks for conditional IAM and duplicate-safe coordinator launches."""
import copy
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import requests
from google.api_core.exceptions import NotFound, PreconditionFailed

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import deploy_crawl_coordinator as deploy


def response(value):
    result = Mock(status_code=200)
    result.json.return_value = value
    return result


class Blob:
    def __init__(self, bucket, path):
        self.bucket, self.path, self.generation = bucket, path, None

    def exists(self):
        return self.path in self.bucket.data

    def reload(self):
        if not self.exists():
            raise NotFound('missing')
        self.generation = self.bucket.data[self.path][0]

    def download_as_text(self, if_generation_match):
        generation, body = self.bucket.data[self.path]
        if generation != if_generation_match:
            raise PreconditionFailed('changed')
        return body

    def upload_from_string(self, body, if_generation_match, **kwargs):
        current = self.bucket.data.get(self.path, (0, None))[0]
        if self.bucket.claim_conflict or current != if_generation_match:
            raise PreconditionFailed('another launcher won')
        self.generation = current + 1
        self.bucket.data[self.path] = (self.generation, body)


class Bucket:
    def __init__(self):
        self.data = {}
        self.claim_conflict = False

    def blob(self, path):
        return Blob(self, path)


class CoordinatorLaunchTests(unittest.TestCase):
    def setUp(self):
        self.session = Mock()
        self.bucket = Bucket()
        self.parent = 'projects/citygraph/locations/us-central1/workflows/test-coordinator'
        self.execution = self.parent + '/executions/123'
        self.claim = 'control/coordinator-launch-test-coordinator.json'
        self.session.get.return_value = response({'executions': []})
        self.session.post.return_value = response({'name': self.execution, 'state': 'ACTIVE'})
        self.addCleanup(patch.stopall)
        patch.object(deploy.storage, 'Client', return_value=SimpleNamespace(bucket=lambda _: self.bucket)).start()
        patch('builtins.print').start()

    def start(self):
        return deploy.start(self.session, 'test-coordinator', 'run-1')

    def test_existing_active_execution_uses_full_view_and_does_not_launch(self):
        self.session.get.return_value = response({'executions': [
            {'name': self.execution, 'state': 'ACTIVE', 'argument': '{"run_id":"run-1"}'}]})
        self.assertEqual(self.start()['name'], self.execution)
        self.assertEqual(self.session.get.call_args.kwargs['params']['view'], 'FULL')
        self.session.post.assert_not_called()
        self.assertEqual(self.bucket.data, {})

    def test_queued_execution_on_second_page_is_not_duplicated(self):
        calls = []

        def get(url, **kwargs):
            calls.append(copy.deepcopy(kwargs['params']))
            if len(calls) == 1:
                return response({'executions': [], 'nextPageToken': 'next'})
            return response({'executions': [
                {'name': self.execution, 'state': 'QUEUED', 'argument': '{"run_id":"run-1"}'}]})

        self.session.get.side_effect = get
        self.start()
        self.assertEqual(calls[0]['view'], 'FULL')
        self.assertIn('QUEUED', calls[0]['filter'])
        self.assertEqual(calls[1], {**calls[0], 'pageToken': 'next'})
        self.session.post.assert_not_called()

    def test_unverifiable_or_different_active_collection_fails_closed(self):
        for argument in [None, '[]', '{', '{"run_id":"different"}']:
            with self.subTest(argument=argument):
                self.session.get.return_value = response({'executions': [
                    {'name': self.execution, 'state': 'ACTIVE', 'argument': argument}]})
                with self.assertRaises(RuntimeError):
                    self.start()
                self.session.post.assert_not_called()

    def test_success_persists_execution_and_known_active_claim_is_reused(self):
        self.start()
        stored = json.loads(self.bucket.data[self.claim][1])
        self.assertEqual(stored['execution'], self.execution)
        self.assertEqual(stored['run_id'], 'run-1')
        self.assertEqual(self.bucket.data[self.claim][0], 2)
        self.session.get.side_effect = [response({'executions': []}), response({'name': self.execution, 'state': 'ACTIVE'})]
        self.start()
        self.assertEqual(self.session.post.call_count, 1)

    def test_numeric_project_name_is_accepted_and_reused(self):
        canonical = self.execution.replace('/citygraph/', '/1028219320071/')
        self.session.post.return_value = response({'name': canonical, 'state': 'ACTIVE'})
        self.assertEqual(self.start()['name'], canonical)
        self.assertEqual(json.loads(self.bucket.data[self.claim][1])['execution'], canonical)
        self.session.get.side_effect = [response({'executions': []}), response({'name': canonical, 'state': 'ACTIVE'})]
        self.assertEqual(self.start()['name'], canonical)
        self.assertEqual(self.session.post.call_count, 1)

    def test_canonicalization_does_not_accept_other_resources(self):
        for unexpected in [self.execution.replace('citygraph', '999999'),
                self.execution.replace('us-central1', 'europe-west1'),
                self.execution.replace('test-coordinator', 'other-workflow'),
                self.execution + '/extra', self.parent + '/executions/']:
            with self.subTest(name=unexpected):
                self.assertFalse(deploy.execution_belongs_to_workflow(unexpected, 'test-coordinator'))

    def test_terminal_previous_execution_allows_explicit_new_start(self):
        self.bucket.data[self.claim] = (7, json.dumps({'execution': self.execution, 'run_id': 'run-1'}))
        self.session.get.side_effect = [response({'executions': []}), response({'name': self.execution, 'state': 'FAILED'})]
        self.start()
        self.session.post.assert_called_once()
        self.assertEqual(self.bucket.data[self.claim][0], 9)

    def test_uncertain_creation_retains_claim_and_blocks_retry(self):
        self.session.post.side_effect = requests.Timeout('response lost')
        with self.assertRaises(requests.Timeout):
            self.start()
        self.assertEqual(json.loads(self.bucket.data[self.claim][1])['state'], 'creating')
        with self.assertRaisesRegex(RuntimeError, 'pending or uncertain'):
            self.start()
        self.assertEqual(self.session.post.call_count, 1)

    def test_generation_conflict_and_worker_lease_prevent_launch(self):
        self.bucket.claim_conflict = True
        with self.assertRaisesRegex(RuntimeError, 'Another launcher'):
            self.start()
        self.session.post.assert_not_called()
        self.bucket.claim_conflict = False
        self.bucket.data['control/worker-lease.json'] = (1, '{}')
        with self.assertRaisesRegex(RuntimeError, 'crawler lease'):
            self.start()
        self.session.post.assert_not_called()

    def test_unverifiable_creation_response_retains_claim(self):
        self.session.post.return_value = response({'state': 'ACTIVE'})
        with self.assertRaisesRegex(RuntimeError, 'no verifiable execution'):
            self.start()
        self.assertEqual(json.loads(self.bucket.data[self.claim][1])['state'], 'creating')


class CoordinatorIAMTests(unittest.TestCase):
    def test_conditional_job_and_project_bindings_and_etags_are_preserved(self):
        policy = {'version': 3, 'etag': 'concurrent-update-guard', 'bindings': [
            {'role': 'roles/viewer', 'members': ['user:existing@example.org'],
             'condition': {'title': 'Temporary', 'expression': 'request.time < timestamp("2030-01-01T00:00:00Z")'}}],
            'auditConfigs': [{'service': 'allServices', 'auditLogConfigs': [{'logType': 'DATA_READ'}]}]}
        session = Mock()

        def get(url, **kwargs):
            if url.endswith(':getIamPolicy'):
                self.assertEqual(kwargs['params'], {'options.requestedPolicyVersion': 3})
                return response(copy.deepcopy(policy))
            return response({'etag': 'role-etag'})

        def post(url, **kwargs):
            if url.endswith(':getIamPolicy'):
                self.assertEqual(kwargs['json'], {'options': {'requestedPolicyVersion': 3}})
                return response(copy.deepcopy(policy))
            return response(kwargs.get('json', {}))

        session.get.side_effect = get
        session.post.side_effect = post
        session.patch.return_value = response({})
        bucket = Mock()
        bucket.get_iam_policy.return_value = SimpleNamespace(version=3, bindings=[])
        with patch.object(deploy.storage, 'Client', return_value=SimpleNamespace(bucket=lambda _: bucket)):
            deploy.provision(session, 'softpower-crawler')
        writes = [call.kwargs['json']['policy'] for call in session.post.call_args_list
                  if call.args[0].endswith(':setIamPolicy')]
        self.assertEqual(len(writes), 2)
        for written in writes:
            self.assertEqual(written['version'], 3)
            self.assertEqual(written['etag'], policy['etag'])
            self.assertEqual(written['bindings'][0], policy['bindings'][0])
            self.assertEqual(written['auditConfigs'], policy['auditConfigs'])
        bucket.get_iam_policy.assert_called_once_with(requested_policy_version=3)


if __name__ == '__main__':
    unittest.main()
