import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from google.api_core.exceptions import InvalidArgument, ServiceUnavailable

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from transaction_retry import fresh_transaction


class TransactionRetryTests(unittest.TestCase):
    def test_expired_transaction_is_discarded_and_operation_rechecks_fresh_state(self):
        client = Mock()
        old, new = object(), object()
        client.transaction.side_effect = [old, new]
        calls = []
        def operation(transaction):
            calls.append(transaction)
            if transaction is old:
                raise InvalidArgument('The referenced transaction has expired or is no longer valid.')
            return 'current fenced owner'
        with patch('google.cloud.firestore.transactional', side_effect=lambda fn: fn):
            self.assertEqual(fresh_transaction(client, operation, sleep=Mock()), 'current fenced owner')
        self.assertEqual(calls, [old, new])

    def test_invalid_input_and_ambiguous_commit_failure_are_not_replayed(self):
        for error in [InvalidArgument('Invalid document path'), ServiceUnavailable('Unknown commit outcome')]:
            client, operation = Mock(), Mock(side_effect=error)
            with patch('google.cloud.firestore.transactional', side_effect=lambda fn: fn):
                with self.assertRaises(type(error)):
                    fresh_transaction(client, operation, sleep=Mock())
            self.assertEqual(client.transaction.call_count, 1)

    def test_expired_transaction_retry_is_bounded(self):
        client, operation, sleep = Mock(), Mock(side_effect=InvalidArgument('Transaction expired')), Mock()
        with patch('google.cloud.firestore.transactional', side_effect=lambda fn: fn):
            with self.assertRaises(InvalidArgument):
                fresh_transaction(client, operation, sleep=sleep)
        self.assertEqual(client.transaction.call_count, 4)
        self.assertEqual(sleep.call_count, 3)
