import contextlib
import copy
import io
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import deploy_crawler
import provision_shared_crawl as shared


def response(data, status=200):
    result = Mock(status_code=status)
    result.json.return_value = data
    return result


class DeploymentTests(unittest.TestCase):
    def test_multiple_instances_share_one_database_and_keep_per_instance_resources(self):
        args = deploy_crawler.arguments(['--run-id', 'test-image', '--instances', '4', '--cpu', '4', '--memory-gib', '4', '--heavy-slots', '3'])
        command = deploy_crawler.job_deploy_arguments(args, 'test:image')
        for argument in ('--tasks=4', '--parallelism=4', '--cpu=4', '--memory=4Gi', '--task-timeout=604800'):
            self.assertIn(argument, command)
        env = next(argument for argument in command if argument.startswith('--set-env-vars='))
        self.assertIn('CRAWL_DISTRIBUTED=1', env)
        self.assertIn('CRAWL_FIRESTORE_DATABASE=softpower-crawl', env)
        self.assertIn('CRAWL_ROTATE_SECONDS=518400', env)
        self.assertIn('CRAWL_HOST_CONCURRENCY=4', env)
        self.assertIn('CRAWL_REQUEST_SPACING=1.0', env)

    def test_archive_limits_are_separate_from_publisher_downloads(self):
        args = deploy_crawler.arguments(['--run-id', 'test', '--instances', '10',
            '--archive-concurrency', '8', '--archive-slots', '8'])
        env = next(x for x in deploy_crawler.job_deploy_arguments(args, 'test:image')
                   if x.startswith('--set-env-vars='))
        self.assertIn('CRAWL_HOST_CONCURRENCY=4', env)
        self.assertIn('CRAWL_ARCHIVE_CONCURRENCY=8', env)
        self.assertIn('CRAWL_ARCHIVE_SLOTS=8', env)

    def test_single_instance_preserves_legacy_path(self):
        args = deploy_crawler.arguments(['--run-id', 'pilot'])
        command = deploy_crawler.job_deploy_arguments(args, 'test:image')
        self.assertIn('--tasks=1', command)
        self.assertIn('--parallelism=1', command)
        self.assertIn('CRAWL_DISTRIBUTED=0', next(argument for argument in command if argument.startswith('--set-env-vars=')))
        self.assertNotIn('CRAWL_FIRESTORE_DATABASE=', next(argument for argument in command if argument.startswith('--set-env-vars=')))

    def test_single_instance_can_continue_existing_shared_queue(self):
        args = deploy_crawler.arguments(['--run-id', 'existing', '--instances', '1', '--shared-queue'])
        command = deploy_crawler.job_deploy_arguments(args, 'existing:image')
        env = next(x for x in command if x.startswith('--set-env-vars='))
        self.assertIn('--tasks=1', command)
        self.assertIn('--parallelism=1', command)
        self.assertIn('CRAWL_DISTRIBUTED=1', env)
        self.assertIn('CRAWL_FIRESTORE_DATABASE=softpower-crawl', env)

    def test_invalid_instances_and_database_are_rejected(self):
        for extra in (['--instances', '0'], ['--instances', '-1'], ['--firestore-database', 'unsafe,CRAWL_CPU=99'],
                      ['--host-concurrency','0'],['--host-concurrency','17'],['--request-spacing','-1'],
                      ['--request-spacing','nan'],['--request-spacing','inf'],
                      ['--archive-concurrency','0'],['--archive-concurrency','17'],
                      ['--archive-slots','0'],['--archive-slots','49']):
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                deploy_crawler.arguments(['--run-id', 'test'] + extra)


class ProvisionTests(unittest.TestCase):
    def test_scoped_binding_preserves_existing_policy_and_etag(self):
        original = {'version': 3, 'etag': 'existing-etag', 'bindings': [{'role': 'roles/viewer', 'members': ['user:owner@example.com']}]}
        session = Mock()
        session.post.side_effect = [response(copy.deepcopy(original)), response({})]
        self.assertTrue(shared.grant_worker_database_access(session))
        policy = session.post.call_args.kwargs['json']['policy']
        self.assertEqual(policy['etag'], original['etag'])
        self.assertEqual(policy['bindings'][0], original['bindings'][0])
        binding = policy['bindings'][1]
        self.assertEqual(binding['members'], [shared.WORKER])
        self.assertEqual(binding['condition']['expression'], 'resource.name == "projects/citygraph/databases/softpower-crawl"')

    def test_existing_scoped_binding_is_idempotent(self):
        session = Mock()
        session.post.return_value = response({'etag': 'same', 'bindings': [{'role': 'roles/datastore.user', 'members': [shared.WORKER], 'condition': dict(shared.CONDITION)}]})
        self.assertFalse(shared.grant_worker_database_access(session))
        session.post.assert_called_once()

    def test_existing_wrong_database_stops_before_iam_mutation(self):
        session = Mock()
        session.get.return_value = response({'name': shared.RESOURCE, 'locationId': 'europe-west1', 'type': 'FIRESTORE_NATIVE'})
        command = Mock()
        with self.assertRaisesRegex(RuntimeError, 'does not match'):
            shared.provision(session, command)
        session.post.assert_not_called()
        command.assert_called_once_with(['services', 'enable', 'firestore.googleapis.com', '--quiet'])

    def test_missing_database_created_before_scoped_access(self):
        session = Mock()
        data = {'name': shared.RESOURCE, 'locationId': shared.REGION, 'type': 'FIRESTORE_NATIVE'}
        session.get.side_effect = [response({}, 404), response(data)]
        command = Mock()
        with patch.object(shared, 'grant_worker_database_access', return_value=True) as grant:
            result = shared.provision(session, command)
        self.assertEqual(result['database'], data)
        self.assertIn('--delete-protection', command.call_args.args[0])
        grant.assert_called_once_with(session)

    def test_no_apply_does_not_authenticate_or_mutate_cloud(self):
        with patch.object(shared.google.auth, 'default') as auth, contextlib.redirect_stdout(io.StringIO()):
            shared.main([])
        auth.assert_not_called()


if __name__ == '__main__':
    unittest.main()
