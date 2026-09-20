"""Read-only, bounded queries for the public China news audit dashboard."""
import json
import logging
import os
import threading
import time
from collections import OrderedDict, deque
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import Response
from google.cloud import bigquery

REFERENCE = json.loads(Path(__file__).with_name('reference.json').read_text())
TABLE = os.environ.get('BQ_TABLE', 'citygraph.softpower.outlet_daily_2015_2025_20260919_192426')
PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT', 'citygraph')
MAX_BYTES = int(os.environ.get('MAX_QUERY_BYTES', str(16 * 2**30)))
BUCKETS = {'day': 'day', 'week': 'DATE_TRUNC(day, WEEK(MONDAY))',
           'month': 'DATE_TRUNC(day, MONTH)', 'year': 'DATE_TRUNC(day, YEAR)'}
GROUPS = ['Chinese', 'Local', 'Third country', 'Unknown or uncertain']
app = FastAPI(title='China news audit', docs_url=None, redoc_url=None, openapi_url=None)
app.add_middleware(CORSMiddleware, allow_origins=os.environ.get(
    'ALLOWED_ORIGINS', 'https://dedcode.github.io').split(','), allow_methods=['GET'], allow_headers=[])
app.add_middleware(GZipMiddleware, minimum_size=1000)
lock = threading.Lock()
query_slot = threading.BoundedSemaphore(1)
recent_queries = deque()
cache = OrderedDict()
CACHE_BYTES = 64 * 2**20


@lru_cache(maxsize=1)
def client():
    return bigquery.Client(project=PROJECT)


def build_query(interval):
    # Only these literal, enumerated expressions can enter SQL. All user
    # country/date values use typed query parameters, never interpolation.
    bucket = BUCKETS[interval]
    return f'''WITH filtered AS (
      SELECT {bucket} AS bucket, day, outlet_group, outlet, outlet_country,
        estimated_country, domain_country, classification_basis, article_count
      FROM `{TABLE}`
      WHERE target_country = @country AND day >= @start AND day < @end
    )
    SELECT CASE WHEN GROUPING(bucket)=0 THEN 'timeline'
                WHEN GROUPING(outlet)=0 THEN 'outlet' ELSE 'summary' END AS kind,
      bucket, outlet_group, outlet, outlet_country, estimated_country,
      domain_country, classification_basis,
      SUM(article_count) AS articles, COUNT(DISTINCT outlet) AS active_outlets,
      COUNT(DISTINCT day) AS active_days
    FROM filtered
    GROUP BY GROUPING SETS (
      (bucket, outlet_group),
      (outlet_group),
      (outlet_group, outlet, outlet_country, estimated_country, domain_country, classification_basis)
    )'''


def validate_selection(country, start, end, interval):
    if country == 'CH' or len(country) != 2 or not country.isascii() or not country.isalpha() or country != country.upper():
        raise HTTPException(422, 'Choose a target country other than China.')
    if not date(2015, 1, 1) <= start <= end <= date(2025, 12, 31):
        raise HTTPException(422, 'Choose an ordered date range within 2015–2025.')
    if interval not in BUCKETS:
        raise HTTPException(422, 'Choose day, week, month or year.')


def shape_result(rows, country, start, end, interval):
    result = dict(country=country, start=str(start), end=str(end), interval=interval,
        summary={}, timeline=[], outlets=[], source='BigQuery daily research snapshot',
        snapshot=REFERENCE['extracted_at'])
    for row in rows:
        r = dict(row)
        common = dict(group=r['outlet_group'], articles=int(r['articles']),
            active_outlets=int(r['active_outlets']), active_days=int(r['active_days']))
        if r['kind'] == 'summary':
            result['summary'][r['outlet_group']] = common
        elif r['kind'] == 'timeline':
            result['timeline'].append(dict(period=str(r['bucket']), **common))
        else:
            result['outlets'].append(dict(outlet=r['outlet'], country=r['outlet_country'],
                estimate=r['estimated_country'], domain_country=r['domain_country'],
                basis=r['classification_basis'], **common))
    for group in GROUPS:
        result['summary'].setdefault(group, dict(group=group, articles=0, active_outlets=0, active_days=0))
    result['timeline'].sort(key=lambda r: (r['period'], r['group']))
    result['outlets'].sort(key=lambda r: (-r['articles'], r['outlet'] or ''))
    # Concentration uses known publishing domains; missing domains stay
    # explicitly separate and cannot masquerade as an independent outlet.
    for group in GROUPS:
        known = [r for r in result['outlets'] if r['group'] == group and r['outlet']]
        summary = result['summary'][group]
        summary['largest_outlet'] = known[0]['outlet'] if known else None
        summary['largest_outlet_articles'] = known[0]['articles'] if known else 0
        summary['missing_domain_articles'] = sum(r['articles'] for r in result['outlets'] if r['group'] == group and not r['outlet'])
    return result


@app.get('/health')
def health():
    return {'status': 'ok', 'snapshot': REFERENCE['extracted_at']}


@app.get('/catalog')
def catalog():
    return REFERENCE


@app.get('/audit')
def audit(country: str = Query(..., pattern='^[A-Z]{2}$'), start: date = date(2015, 1, 1),
          end: date = date(2025, 12, 31), interval: str = Query('month', pattern='^(day|week|month|year)$')):
    validate_selection(country, start, end, interval)
    key = (country, str(start), str(end), interval)
    with lock:
        if key in cache:
            payload = cache[key]
            cache.move_to_end(key)
            return Response(payload, media_type='application/json', headers={'Cache-Control': 'public, max-age=86400', 'X-Audit-Cache': 'hit'})
    if not query_slot.acquire(blocking=False):
        raise HTTPException(429, 'Another uncached selection is loading. Please retry shortly.', headers={'Retry-After': '10'})
    try:
        with lock:
            now = time.monotonic()
            while recent_queries and now-recent_queries[0] > 3600:
                recent_queries.popleft()
            if len(recent_queries) >= 30:
                raise HTTPException(429, 'The public query allowance is busy. Cached selections remain available; please retry later.', headers={'Retry-After': '120'})
            recent_queries.append(now)
        config = bigquery.QueryJobConfig(maximum_bytes_billed=MAX_BYTES, use_query_cache=True,
            query_parameters=[bigquery.ScalarQueryParameter('country', 'STRING', country),
                bigquery.ScalarQueryParameter('start', 'DATE', start),
                bigquery.ScalarQueryParameter('end', 'DATE', end+timedelta(days=1))],
            labels={'app': 'softpower-audit', 'purpose': 'public-dashboard'})
        job = client().query(build_query(interval), job_config=config, location='US', timeout=30)
        try:
            data = shape_result(job.result(timeout=100), country, start, end, interval)
        except TimeoutError:
            job.cancel()
            raise HTTPException(504, 'This selection took too long. Please try a shorter date range.')
        payload = json.dumps(data, separators=(',', ':'), ensure_ascii=False).encode()
        with lock:
            cache[key] = payload
            while sum(len(v) for v in cache.values()) > CACHE_BYTES and len(cache) > 1:
                cache.popitem(last=False)
        return Response(payload, media_type='application/json', headers={'Cache-Control': 'public, max-age=86400', 'X-Audit-Cache': 'miss'})
    except HTTPException:
        raise
    except Exception:
        logging.exception('Audit query failed')
        raise HTTPException(503, 'The data service could not complete this selection. Please retry or narrow the dates.')
    finally:
        query_slot.release()
