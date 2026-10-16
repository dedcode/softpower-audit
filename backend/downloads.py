"""Browse saved text without querying BigQuery or exposing the private bucket."""
import gzip
import hashlib
import io
import json
import logging
import re
import threading
import time
from functools import lru_cache
from urllib.parse import urlsplit

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import firestore, storage

BUCKET = 'citygraph-softpower-crawl'
COUNTRIES = frozenset(('KE', 'CI', 'RP'))
TEXT_STATUSES = frozenset(('saved', 'partial'))
MAX_INDEX = 64 * 2**20
MAX_EVIDENCE = 8 * 2**20
MAX_TEXT = 5 * 2**20
router = APIRouter()


@lru_cache(maxsize=1)
def bucket():
    return storage.Client(project='citygraph').bucket(BUCKET)


@lru_cache(maxsize=1)
def database():
    return firestore.Client(project='citygraph', database='softpower-crawl')


def country_code(country):
    if country not in COUNTRIES:
        raise HTTPException(422, 'Choose Kenya, Chile or the Philippines.')
    return country


def unpack(data, limit):
    """Bound decompression as well as the compressed object download."""
    if data.startswith(b'\x1f\x8b'):
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as stream:
            data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError('Stored document exceeds the viewer size limit')
    return data


def read_object(path, limit):
    blob = bucket().blob(path)
    blob.reload(timeout=15)
    if blob.size > limit:
        raise ValueError('Stored object exceeds the viewer size limit')
    data = blob.download_as_bytes(if_generation_match=blob.generation, timeout=30)
    return unpack(data, limit)


def progress(country):
    try:
        return json.loads(read_object('progress/' + country + '.json', MAX_EVIDENCE))
    except NotFound:
        return {'country': country, 'state': 'not_started', 'counts': {}}


def index_signature(snapshot):
    return [snapshot.get('run_id'), *(snapshot.get('counts', {}).get(s, 0)
                                     for s in ('saved', 'partial'))]


class DownloadIndex:
    """Persist a compact URL/status index; refresh changed queue metadata only.

    The existing worker's updated_at values are UTC epoch seconds. A two-minute
    overlap covers writes concurrent with the last snapshot. Count mismatches
    trigger full reconciliation, so the overlap cannot silently lose a result.
    No queue documents, raw pages or crawl results are changed here.
    """
    def __init__(self):
        self.cache = {}
        self.locks = {country: threading.Lock() for country in COUNTRIES}

    def get(self, country, *, reconcile=False):
        country_code(country)
        cached = self.cache.get(country)
        if cached and not reconcile and time.monotonic() - cached[0] < 60:
            return cached[1]
        with self.locks[country]:
            cached = self.cache.get(country)
            if cached and not reconcile and time.monotonic() - cached[0] < 60:
                return cached[1]
            snapshot = progress(country)
            run_id = snapshot.get('run_id')
            if not run_id:
                return {'country': country, 'run_id': None, 'updated_at': None, 'entries': []}
            if not re.fullmatch(r'[a-z0-9-]+', run_id) or snapshot.get('country') != country:
                raise ValueError('Invalid collection snapshot')
            path = 'downloads/' + country + '.json.gz'
            target = bucket().blob(path)
            generation = 0
            previous = None
            try:
                target.reload(timeout=15)
                generation = target.generation
                if target.size > MAX_INDEX:
                    raise ValueError('Download index exceeds size limit')
                previous = json.loads(unpack(target.download_as_bytes(
                    if_generation_match=generation, timeout=30), MAX_INDEX))
                if previous.get('country') != country or previous.get('run_id') != run_id:
                    previous = None
            except NotFound:
                pass
            signature = index_signature(snapshot)
            expected = sum(signature[1:])
            if (previous and not reconcile and previous.get('signature') == signature
                    and len(previous['entries']) == expected):
                result = self.public(previous)
            else:
                # Full reconciliation also handles changed/retracted results,
                # imported old collections, and an incomplete earlier index.
                full = (reconcile or previous is None or len(previous['entries']) != sum(previous['signature'][1:]))
                entries = {} if full else dict(previous['entries'])
                cutoff = time.time()
                query = database().collection('crawl_runs').document(run_id).collection('articles')
                if full:
                    query = query.where(filter=firestore.FieldFilter('status', 'in', sorted(TEXT_STATUSES)))
                else:
                    query = query.where(filter=firestore.FieldFilter('updated_at', '>=', previous['watermark'] - 120))
                query = query.select(['state', 'status', 'item.url', 'result_uri'])
                for document in query.stream(timeout=90):
                    row = document.to_dict()
                    url = row.get('item', {}).get('url')
                    if not url:
                        continue
                    if (row.get('state') == 'done' and row.get('status') in TEXT_STATUSES
                            and row.get('result_uri')):
                        entries[url] = row['status']
                    else:
                        entries.pop(url, None)
                value = {'version': 1, 'country': country, 'run_id': run_id,
                         'signature': signature, 'watermark': cutoff,
                         'updated_at': snapshot.get('updated_at'),
                         'entries': sorted(entries.items())}
                data = json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode()
                if len(data) > MAX_INDEX:
                    raise ValueError('Download index exceeds size limit')
                try:
                    target.upload_from_string(gzip.compress(data, mtime=0), content_type='application/gzip',
                                              if_generation_match=generation, timeout=60)
                except PreconditionFailed:
                    # A newer refresh won the race; never overwrite it.
                    value = json.loads(read_object(path, MAX_INDEX))
                    if value.get('country') != country or value.get('run_id') != run_id:
                        raise ValueError('Collection changed during download index refresh')
                result = self.public(value)
            self.cache[country] = (time.monotonic(), result)
            return result

    @staticmethod
    def public(value):
        return {key: value[key] for key in ('country', 'run_id', 'updated_at', 'entries')}


index = DownloadIndex()


@router.get('/downloads')
def downloaded(country: str = Query('KE', pattern='^[A-Z]{2}$')):
    country_code(country)
    try:
        result = index.get(country)
        return Response(json.dumps(result, separators=(',', ':'), ensure_ascii=False),
                        media_type='application/json', headers={'Cache-Control': 'public, max-age=60'})
    except HTTPException:
        raise
    except Exception:
        logging.exception('Download availability unavailable')
        raise HTTPException(503, 'Download status is temporarily unavailable. Please retry.')


def evidence_path(uri, run_id, article_id):
    parsed = urlsplit(uri or '')
    path = parsed.path.lstrip('/')
    patterns = [re.escape('runs/' + run_id + '/distributed/results/' + article_id + '/') + r'[a-zA-Z0-9-]+\.json\.gz',
                re.escape('runs/' + run_id + '/checkpoints/') + r'[a-zA-Z0-9-]+\.jsonl\.gz']
    if (parsed.scheme != 'gs' or parsed.netloc != BUCKET or parsed.query or parsed.fragment
            or not any(re.fullmatch(pattern, path) for pattern in patterns)):
        raise ValueError('Invalid accepted result evidence location')
    return path


def accepted_result(data, row, country, run_id, article_id, url):
    """Old accepted results share JSONL batches; choose the exact fenced version."""
    selected = None
    for line in data.splitlines():
        if not line.strip():
            continue
        result = json.loads(line)
        if result.get('article_id') != article_id:
            continue
        if row.get('result_updated_at') and result.get('updated_at') != row['result_updated_at']:
            continue
        if (result.get('run_id'), result.get('country'), result.get('url'), result.get('status')) != (
                run_id, country, url, row['status']):
            raise ValueError('Accepted evidence does not match this article')
        if selected is not None:
            raise ValueError('Ambiguous accepted article evidence')
        selected = result
    if selected is None:
        raise ValueError('Accepted article result is missing')
    return selected


def text_path(uri, article_id):
    parsed = urlsplit(uri or '')
    path = parsed.path.lstrip('/')
    # The URI is read from the accepted result, never supplied by the visitor.
    patterns = [rf'runs/[a-z0-9-]+/(?:claims/{article_id}/[a-zA-Z0-9-]+/)?extracted/{article_id}\.txt\.gz',
                rf'text/{article_id}/[a-f0-9]{{64}}\.txt\.gz']
    if (parsed.scheme != 'gs' or parsed.netloc != BUCKET or parsed.query or parsed.fragment
            or not any(re.fullmatch(pattern, path) for pattern in patterns)):
        raise ValueError('Invalid extracted text location')
    return path


@router.get('/downloaded-article')
def article(country: str = Query(..., pattern='^[A-Z]{2}$'), url: str = Query(..., max_length=8192)):
    country_code(country)
    try:
        current = progress(country)
        run_id = current.get('run_id')
        if not run_id:
            raise HTTPException(404, 'This article has no downloaded text.')
        article_id = hashlib.sha256(url.encode()).hexdigest()
        document = database().collection('crawl_runs').document(run_id).collection('articles').document(article_id).get(timeout=15)
        row = document.to_dict() if document.exists else {}
        if (row.get('state') != 'done' or row.get('status') not in TEXT_STATUSES
                or row.get('item', {}).get('url') != url):
            raise HTTPException(404, 'This article has no downloaded text.')
        result = accepted_result(read_object(evidence_path(row.get('result_uri'), run_id, article_id), MAX_EVIDENCE),
                                 row, country, run_id, article_id, url)
        text = read_object(text_path(result.get('text_uri'), article_id), MAX_TEXT).decode('utf-8')
        if not text.strip():
            raise ValueError('Stored text is empty')
        extractions = [event for event in result.get('attempts', [])
                       if event.get('stage', '').startswith('extract:') and event.get('title')]
        title = next((event['title'] for event in reversed(extractions)
                      if event.get('raw_uri') == result.get('raw_uri')), '')
        return {'url': url, 'outlet': result['outlet'], 'status': result['status'],
                'title': title, 'text': text, 'collected_at': result.get('fetched_at') or result.get('updated_at'),
                'source_url': result.get('final_url', url)}
    except HTTPException:
        raise
    except Exception:
        logging.exception('Stored article unavailable')
        raise HTTPException(503, 'The downloaded article could not be opened. Please retry.')
