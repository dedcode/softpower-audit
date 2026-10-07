"""Restart explicitly expired Firestore transactions with a fresh transaction.

Do not replay ambiguous network/commit failures here. The SDK owns its normal
retry policy; this handles the explicit invalid/expired transaction response.
"""
import logging
import random
import time

from google.api_core.exceptions import InvalidArgument


def fresh_transaction(client, operation, *, attempts=4, sleep=time.sleep):
    from google.cloud import firestore
    for attempt in range(attempts):
        try:
            # A new wrapper and transaction discard any expired transaction ID.
            return firestore.transactional(operation)(client.transaction())
        except InvalidArgument as exc:
            message = str(exc).lower()
            if ('transaction' not in message or
                    not ('expired' in message or 'no longer valid' in message) or
                    attempt + 1 >= attempts):
                raise
            logging.warning('Firestore transaction expired; retrying with a fresh transaction (%s/%s)',
                            attempt + 1, attempts)
            sleep(min(8., 2 ** attempt) + random.uniform(0, .5))
