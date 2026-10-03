# softpower-audit

The public interface at https://djelleldifallah.com/softpower-audit/ is a single
article browser. The publishing country is currently locked to Kenya in the header. Set observation
dates in the left panel, select a website, and open article links. `/kenya/` and
`/pilot/` redirect to this interface, preserving query parameters.

## Data

The browser queries `citygraph.softpower.china_articles`: 80,646,046 distinct
URL/day observations covering 2015–2025, extracted from GDELT. The extraction
includes known non-Chinese publisher estimates and detected China mentions.
Origin estimates are provisional; publisher type, ownership and article focus
are not verified. Unknown GDELT publisher origins and China-assigned publishers
are excluded from this underlying table, even when using the ccTLD filter.

Country codes are GDELT FIPS, not ISO. The header includes the 234 observed
non-China geographic codes present in either publisher-origin field; these
include territories and special codes. `article-catalog.json` provides labels.

**Interface counts are distinct URLs within the selected dates**, deduplicated
across observation days. Monthly histogram bars count distinct URLs within each
month and should not be summed to obtain the full-period unique URL count.
Article rows display the latest qualifying observation date and its detected
countries; expanding a row fetches its corresponding local place names.

The geographic filters require a China mention (already true for the corpus),
optionally the source country, or only that pair. A specific-place filter
requires a city/landmark or administrative-region mention in the selected source
country. These filters describe GDELT detections, not verified article subjects.
The same URL may qualify on some observation days and not others.

GDELT has no source partitions for 1 January–16 February 2015, 29 August 2017,
and 15 June–1 July 2025. 17 February 2015 has 25 source records but no China mentions.

## Architecture

`docs/` is the static GitHub Pages frontend. `backend/articles.py` serves
parameterized, read-only `/browse`, `/stories`, `/article-places`, and
`/article-catalog` routes on the existing Cloud Run service. The older aggregate
API remains available for compatibility but has no separate website interface.

For Kenya, `docs/data/kenya/` holds a complete versioned static cache: 86,383
daily candidate records (GDELT Kenya or ccTLD Kenya), including 85,993 unique
URLs under the GDELT Kenya filter. The download is 3.56 MB compressed, with an
18.18 MB JSON fallback for browsers without streaming gzip decompression.
`kenya-cache.js` filters and paginates in memory, preserves per-day geographic
evidence, and deduplicates URLs only after filtering. Place names are included.
Cache Storage retains the versioned payload when available; memory and normal
HTTP caching remain available if persistent storage is disabled. Kenya browsing
never calls the query API, including pagination and expanded place details.
Rebuild with `scripts/build_kenya_cache.py` and verify with
`node tests/test_kenya_cache.js`. The snapshot includes 2015–2025 inclusive
(11 calendar years); it is not a live news feed.

The current frontend only uses the Kenya cache, including links with another country code (normalized to KE). The API remains available separately for other countries. Overview queries return website rankings and the monthly histogram. Story
queries return 20 distinct URLs per page; pagination does not truncate the
corpus. Full location strings are fetched only when a story's details are
opened. The result summary displays the URL and website counts once; choose All sources
in the website list to clear a website selection.

The service identity has table-scoped BigQuery read access to the article table
and the earlier daily aggregate table. Credentials never reach the browser.
Existing protections remain: one Cloud Run instance, one uncached query at a
time, 30 uncached queries per process/hour, a 64 MiB response cache, and a 16 GiB
billing ceiling per query. These are not a guaranteed monthly spending cap.

## Development and deployment

Frontend configuration: `docs/config.js`. GitHub Pages publishes `docs/` from
`main`. API: install `backend/requirements.txt`, then run `uvicorn main:app`
inside `backend/`, setting `ALLOWED_ORIGINS` for local development as needed.

`scripts/prepare_article_browser.py` preserves IAM bindings while granting the
existing reader identity table access, refreshes the observed-country catalog,
and validates Kenya January 2025 against 429 URLs. `scripts/deploy_api.py`
deploys Cloud Run with local ADC and a short-lived token file removed afterward.

Checks:

```sh
python3 tests/test_backend.py
python3 tests/test_articles.py
node tests/test_kenya_dates.js
node tests/test_audit.js
node --check docs/browser.js
```
