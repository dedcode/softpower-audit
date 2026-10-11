"""Only identifiable temporary infrastructure failures permit a cohort restart."""
from google.api_core import exceptions
import requests
TEMPORARY=(exceptions.ServiceUnavailable,exceptions.TooManyRequests,exceptions.DeadlineExceeded,
           exceptions.InternalServerError,exceptions.GatewayTimeout,requests.ConnectionError,requests.Timeout)
def temporary_failure(error):
    # Only the SDK's explicit retry wrapper may qualify through its reason.
    # Arbitrary exception context can contain an earlier outage even when the
    # actual failure is a permission, checkpoint or ownership error.
    seen=set()
    while isinstance(error,exceptions.RetryError) and id(error) not in seen:
        seen.add(id(error));error=error.cause
    return isinstance(error,TEMPORARY)

def recoverable_cohort(state,workers,expected):
    failed=[r for r in workers if r.get('state')=='failed']
    return (state=='failed' and len(workers)==expected and bool(failed)
            and all(r.get('state') in ('failed','completed','continuing') for r in workers)
            and all(r.get('infrastructure_recoverable') is True for r in failed))
