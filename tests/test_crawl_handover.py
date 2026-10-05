"""Offline proof that rollouts replace the guard without an unowned gap."""
import copy
import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import crawl_handover as handover
from test_deploy_coordinator import Blob, Bucket


class HandoverTests(unittest.TestCase):
    def setUp(self):
        self.bucket = Bucket()
        self.old = {'owner': 'old-execution', 'execution': 'old-execution', 'run_id': 'run-1',
                    'distributed': True, 'task_count': 10,
                    'expires_at': (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()}
        self.bucket.data[handover.LEASE_PATH] = (12, json.dumps(self.old))

    def reserve(self):
        return handover.reserve(self.bucket, 'run-1', 'old-execution', 'unique-token')

    def transfer(self, execution='new-execution'):
        return handover.transfer(self.bucket, 'run-1', 'unique-token', execution, 10)

    def test_reservation_and_transfer_keep_guard_present_and_expected_lifetimes(self):
        before = datetime.now(timezone.utc)
        reserved = self.reserve()
        self.assertEqual(reserved['generation'], 13)
        self.assertEqual(reserved['lease']['owner'], 'handover-unique-token')
        self.assertEqual(reserved['lease']['execution'], 'handover-unique-token')
        self.assertEqual(reserved['lease']['previous_execution'], 'old-execution')
        self.assertGreaterEqual(datetime.fromisoformat(reserved['lease']['expires_at']), before + timedelta(minutes=30))
        self.assertEqual(set(self.bucket.data), {handover.LEASE_PATH})
        transferred = self.transfer()
        self.assertEqual(transferred['generation'], 14)
        self.assertEqual(transferred['lease']['execution'], 'new-execution')
        self.assertEqual(transferred['lease']['owner'], 'new-execution')
        self.assertEqual(transferred['lease']['handover_id'], 'unique-token')
        expiry = datetime.fromisoformat(transferred['lease']['expires_at'])
        self.assertGreaterEqual(expiry, before + timedelta(minutes=10))
        self.assertLess(expiry, before + timedelta(minutes=11))
        self.assertEqual(set(self.bucket.data), {handover.LEASE_PATH})

    def test_same_reservation_and_transfer_are_idempotent(self):
        self.reserve()
        self.assertTrue(self.reserve()['reused'])
        self.assertEqual(self.bucket.data[handover.LEASE_PATH][0], 13)
        self.transfer()
        self.assertTrue(self.transfer()['reused'])
        self.assertEqual(self.bucket.data[handover.LEASE_PATH][0], 14)

    def test_idempotent_transfer_accepts_normal_worker_renewal(self):
        self.reserve()
        real = self.transfer()['lease']
        real.pop('handover_id')
        self.bucket.data[handover.LEASE_PATH] = (15, json.dumps(real))
        self.assertTrue(self.transfer()['reused'])
        self.assertEqual(self.bucket.data[handover.LEASE_PATH][0], 15)

    def test_unknown_owner_run_or_kind_cannot_be_reserved(self):
        for patch_value in ({'owner': 'other'}, {'execution': 'other'}, {'run_id': 'other'},
                            {'distributed': False}, {'task_count': 0}, {'task_count': True}):
            with self.subTest(patch_value=patch_value):
                self.bucket.data[handover.LEASE_PATH] = (12, json.dumps({**self.old, **patch_value}))
                before = copy.deepcopy(self.bucket.data)
                with self.assertRaises(RuntimeError):
                    self.reserve()
                self.assertEqual(self.bucket.data, before)

    def test_missing_guard_is_never_created(self):
        self.bucket.data.clear()
        with self.assertRaisesRegex(RuntimeError, 'disappeared'):
            self.reserve()
        with self.assertRaisesRegex(RuntimeError, 'disappeared'):
            self.transfer()
        self.assertEqual(self.bucket.data, {})

    def test_transfer_rejects_unknown_or_expired_reservation(self):
        reserved = self.reserve()['lease']
        for patch_value in ({'handover_id': 'other'}, {'owner': 'other'}, {'run_id': 'other'},
                            {'distributed': False}, {'expires_at': '2000-01-01T00:00:00+00:00'},
                            {'expires_at': '2099-01-01'}, {'expires_at': 'invalid'}):
            with self.subTest(patch_value=patch_value):
                self.bucket.data[handover.LEASE_PATH] = (13, json.dumps({**reserved, **patch_value}))
                before = copy.deepcopy(self.bucket.data)
                with self.assertRaises(RuntimeError):
                    self.transfer()
                self.assertEqual(self.bucket.data, before)

    def test_transfer_does_not_return_guard_to_retired_execution(self):
        self.reserve()
        with self.assertRaisesRegex(RuntimeError, 'retired'):
            self.transfer('old-execution')

    def test_changed_generation_during_reserve_or_transfer_is_not_overwritten(self):
        for transferring in (False, True):
            with self.subTest(transferring=transferring):
                self.setUp()
                if transferring:
                    self.reserve()
                before = copy.deepcopy(self.bucket.data)
                original = Blob.upload_from_string

                def concurrent_write(blob, body, if_generation_match, **kwargs):
                    current = blob.bucket.data[blob.path]
                    blob.bucket.data[blob.path] = (current[0] + 1, current[1])
                    return original(blob, body, if_generation_match, **kwargs)

                with patch.object(Blob, 'upload_from_string', concurrent_write):
                    with self.assertRaisesRegex(RuntimeError, 'changed during handover'):
                        self.transfer() if transferring else self.reserve()
                self.assertEqual(self.bucket.data[handover.LEASE_PATH][1], before[handover.LEASE_PATH][1])
                self.assertEqual(self.bucket.data[handover.LEASE_PATH][0], before[handover.LEASE_PATH][0] + 1)

    def test_generation_change_between_read_and_download_is_rejected(self):
        original = Blob.download_as_text

        def changed(blob, if_generation_match):
            generation, body = blob.bucket.data[blob.path]
            blob.bucket.data[blob.path] = (generation + 1, body)
            return original(blob, if_generation_match)

        with patch.object(Blob, 'download_as_text', changed):
            with self.assertRaisesRegex(RuntimeError, 'disappeared or changed'):
                self.reserve()
        self.assertEqual(json.loads(self.bucket.data[handover.LEASE_PATH][1]), self.old)

    def test_minimal_operator_reservation_can_transfer(self):
        minimal = self.reserve()['lease']
        minimal.pop('previous_execution'); minimal.pop('created_at')
        self.bucket.data[handover.LEASE_PATH] = (13, json.dumps(minimal))
        self.assertEqual(self.transfer()['lease']['execution'], 'new-execution')


if __name__ == '__main__':
    unittest.main()
