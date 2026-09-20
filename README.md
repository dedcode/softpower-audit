# China news audit

A public research dashboard for inspecting the balance, diversity and continuity
of local and Chinese news outlet coverage for a China–country pair.

The interface is published at **https://djelleldifallah.com/softpower-audit/**.
The `dedcode.github.io` Pages account uses this existing custom domain.

## What it does

- Select any of the 267 observed target geographic codes and any date range
  within 2015–2025. The catalog includes countries, territories and historical
  or special codes; it is not a list of 267 sovereign countries.
- Aggregate **daily** counts into days, Monday-based weeks, months or years.
- Compare observations and distinct active outlets over time.
- Set minimum outlets and observations for **both** Chinese and local groups,
  and choose the required proportion of complete periods.
- Inspect leading outlets, concentration, classification conflicts and gaps.
- Search/sort the outlet table, export the filtered table or timeline as CSV,
  and copy a URL that reproduces the pair, period, interval and criteria.

## Data and interpretation

The source is a saved BigQuery table with **182,660,570 daily outlet/pair rows**,
extracted on 19 September 2026. It covers all 3,953 available source days within
the requested window. No source partitions exist for:

- 1 January–16 February 2015;
- 29 August 2017;
- 15 June–1 July 2025.

Daily counts deduplicate a URL within its day, pair and outlet. A URL observed
on different days can count on each day. Longer-period sums are consequently
**daily URL observations**, not period-wide distinct article counts. Active
outlets are counted distinctly within each requested interval, never summed
from daily distinct counts. Domains do not necessarily represent independent
owners, and syndicated stories remain separate publisher URLs.

Outlet origin uses GDELT's geographic coverage estimate and country-domain
clues. All origin labels are **provisional**, including agreement between these
clues. Conflicts remain uncertain. Chinese outlet origin does not establish a
soft-power campaign, and local reporting does not measure public opinion.
Historical classifications use a 2015–2021 estimate and can contain lookahead
or stale-location errors. Unknown-origin publishers require geographic
co-mentions, which can undercount local coverage without a home-country mention.

Sources: [GDELT outlet-estimate methodology](https://blog.gdeltproject.org/mapping-the-media-a-geographic-lookup-of-gdelts-sources-2015-2021/amp/),
[IANA country-code domains](https://www.iana.org/domains/root/db),
[GeoNames country references](https://www.geonames.org/export/).
See `docs/data/provenance.json` for dates and observed target codes.

## Architecture

`docs/` is a dependency-free HTML/CSS/JavaScript site on GitHub Pages.
`backend/` is a FastAPI service on Cloud Run. The service runs a fixed,
parameterized query over the saved daily BigQuery table. It returns only the
selected aggregate results; it never rescans the original GDELT corpus.

Three full-window monthly seed views are saved in `docs/data/` for fast startup.
Other selections use the API. Filters within the loaded outlet table, research
criteria, chart measures, and CSV export run locally without additional queries.

The Cloud Run identity has `bigquery.jobUser` on the project and
`bigquery.dataViewer` on **only the extracted table**. No service-account keys
or user credentials are embedded in the site. The API is deliberately public;
its CORS policy allows the GitHub Pages origin and the existing custom domain
(`https://djelleldifallah.com` and its `www` variant). CORS is not authentication.

Cost controls: one maximum Cloud Run instance, zero minimum instances, one
concurrent uncached query, 30 uncached queries per hour per running process,
64 MiB of application response cache, BigQuery result caching, and a 16 GiB
maximum billed scan per query (about $0.10 before free allowances at $6.25/TiB).
The process rate limiter resets on instance replacement: these controls are
**not a guaranteed monthly spending cap**. Requests beyond the allowance get
a retry message. Keep Cloud Billing monitoring enabled if traffic grows.

## Development and deployment

Frontend: serve `docs/` with any static HTTP server. Configure the API URL in
`docs/config.js`. Changes in `docs/` publish from the `main` branch through
GitHub Pages. This project does not modify the main `dedcode.github.io` site.

Backend: install `backend/requirements.txt`, then run `uvicorn main:app` from
`backend/`. Local development can set `ALLOWED_ORIGINS=http://127.0.0.1:8811`.
`scripts/deploy_api.py` deploys the backend using local Google ADC and a
short-lived token file that is removed afterward. Deployment metadata stays in
the git-ignored `deployment-private/` directory. `scripts/configure_cloud.py`
documents the scoped identities and IAM grants used for this deployment.

Tests:

```sh
node tests/test_audit.js
python3 tests/test_backend.py
```

The BigQuery grouping query was also validated with synthetic records across
days, proving that repeated outlets are counted distinctly within a period.
