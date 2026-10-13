"""Predict the next archive dependency without requests or changing checkpoints."""
from urllib.parse import urlsplit

from extractor_version import VERSION

SERVICES = frozenset(('archive.org', 'web.archive.org', 'local'))


def archive_service(checkpoint, item):
    state = checkpoint or {}
    if not isinstance(state, dict):
        raise ValueError('Invalid archive dispatch checkpoint')
    pipeline = state.get('pipeline') if state.get('kind') == 'phased-toolbox' else state
    pipeline = pipeline or {}
    if not isinstance(pipeline, dict):
        raise ValueError('Invalid archive pipeline checkpoint')
    best = pipeline.get('best') or {}
    if best.get('raw_uri') and (pipeline.get('extractor_version') != VERSION
                               or best.get('continuations')):
        # Reanalysis and linked-original recovery must remain runnable while
        # archive servers are paused; their real requests retain host permits.
        return 'local'
    hint = pipeline.get('archive_service')
    if hint is not None:
        if hint not in SERVICES:
            raise ValueError('Invalid archive service hint')
        return hint
    lookups = pipeline.get('archive_lookups', {})
    seen = set(pipeline.get('archive_seen') or ())
    if not isinstance(lookups, dict):
        raise ValueError('Invalid cached archive lookups')
    for stamp in (str(item.get('first_observed', '')).replace('-', ''), ''):
        cached = lookups.get(stamp)
        payload = cached.get('payload') if isinstance(cached, dict) else None
        if payload is None:
            return 'archive.org'
        if not isinstance(payload, dict) or not isinstance(payload.get('archived_snapshots'), dict):
            raise ValueError('Invalid cached archive response')
        match = payload['archived_snapshots'].get('closest') or {}
        if not match.get('available'):
            continue
        url = str(match.get('url', '')).replace('http://web.archive.org/', 'https://web.archive.org/')
        if urlsplit(url).hostname != 'web.archive.org':
            return 'local'  # Let the pipeline reject the unexpected source.
        if url not in seen:
            return 'web.archive.org'
    return 'local'
