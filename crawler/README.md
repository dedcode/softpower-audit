# Article collection pipeline

Status: deployed on 2026-10-03 after approval. The Kenya pilot completed; the full collection has not been launched. Public progress: https://djelleldifallah.com/softpower-audit/extraction-status/?country=KE .

The worker accepts a country code, date range, source table and resource limits. Kenya is the first input, not a special case in the crawler. Source selection uses GDELT's estimated publisher country. It does not select articles merely because Kenya appears in their text.

## Storage and provenance

Private bucket: `gs://citygraph-softpower-crawl` (US). Original HTTP response bodies are gzip-compressed with SHA-256 content hashes. Response metadata records the requested URL, HTTP status, headers, retrieval time and whether the saved body was truncated. Extracted text is stored separately. HTTP content decoding happens before preservation; these are original response bodies, not packet captures. Robots responses are retained too.

Dataset: `citygraph.softpower_crawl` (US).

- `crawl_inputs`: run, country, article ID, original URL, outlet, source table and first/last observation dates.
- `crawl_result_events`: append-only results, statuses, attempts, response metadata references and raw/text object references.
- `crawl_results`: latest result per run and article ID.
- `crawl_run_events` and `crawl_runs`: run configuration and progress summaries.

The article ID is SHA-256 of the exact original URL. Join the results back to `citygraph.softpower.china_articles` using `url`; the source contains daily observations, so this join can produce multiple rows for one URL. Observation dates are not publication dates.

A saved result means HTML yielded at least 400 characters of extracted text and passed basic challenge/homepage checks. It is not human verification. Syndicated articles and alternate URLs remain separate records. Unavailable means HTTP 404/410 at collection time; it does not rule out an archived copy.

## Scheduling and safeguards

One Cloud Run Job task, 1 vCPU and 1 GiB memory, up to six outlets concurrently, one URL per outlet at a time. Requests to each host have at least three seconds between starts, extended by robots crawl-delay/request-rate where available. Robots rules are honored on redirect destinations too. Private-network destinations are rejected.

At most three attempts per URL. Retry-After is honored; waits above two minutes are deferred. Repeated blocks, rate limits and unavailable robots policies pause that outlet. No proxy rotation, challenge bypass or paywall bypass. JavaScript rendering and archive recovery are not included in this first implementation.

Results checkpoint to storage and load into BigQuery in batches. Resuming the same run skips checkpointed outcomes; successful downloads are also reusable across runs. Failed outcomes require a subsequent run for another attempt. A global storage lease prevents overlapping workers. A `runs/RUN_ID/STOP` object requests a graceful stop. Paused work is retained; no automatic job retry or paid continuation is scheduled.

Limits stop scheduling new work and drain active downloads, so there can be a small overshoot. Runtime/byte/attempt limits are operational controls, not a guaranteed billing cap.

## Proposed first execution

Pilot: up to 24 URLs, oldest and newest from each of the 12 largest outlets; 20-minute scheduling limit, 100 MiB response limit, 100 attempts. This tests old links and recent links across several publishers; it is not a representative content sample.

Full Kenya run after reviewing the pilot: all selected distinct URLs, expected 85,993 for 2015–2025. Input query confirms the count. Maximum 23 hours per execution, 40 GiB responses, 150,000 attempts. Anything unfinished stays pending. The input query has a 16 GiB billed-byte ceiling and fails without proceeding if this is insufficient; do not silently raise it.

At published us-central1 rates of $0.000018/vCPU-second plus $0.000002/GiB-second, nominal worker compute is approximately $0.024 for 20 minutes or $1.66 for 23 hours before free allowances. Build, BigQuery, storage, storage operations and status API costs are additional. Source: https://cloud.google.com/run/pricing . Actual billing depends on execution duration, in-flight draining and current rates. Measure compressed bytes in the pilot before extrapolating persistent storage cost.

## Exact cloud access requested

Create service account `softpower-crawler@citygraph.iam.gserviceaccount.com`:

- project `citygraph`: `roles/bigquery.jobUser` (submit jobs);
- new bucket only: `roles/storage.objectAdmin` (originals, checkpoints, lease and progress);
- new dataset only: `WRITER` (tracking records).

Existing API account `softpower-audit-reader@citygraph.iam.gserviceaccount.com`: `roles/storage.objectViewer`, condition restricted to the new bucket's `progress/` objects. The public API cannot read stored originals under this grant. No public bucket access.

Create Cloud Run Job `softpower-crawler` in `us-central1`, with one task, no automatic retries, 1 CPU, 1 GiB RAM. Build the container and update the existing API to serve progress. Publish the status page through the existing GitHub Pages repository. Deployment and the pilot used the existing build account successfully; the scripts do not grant new build permissions automatically.

## Commands after approval

Use an environment with crawler requirements installed and Application Default Credentials. From the repository root:

```sh
python scripts/provision_crawler.py --apply
python scripts/prepare_crawl_run.py --country KE --pilot --apply
python scripts/deploy_crawler.py --run-id RUN_ID --deploy --execute
```

Without `--apply`, provisioning/preparation print plans only. Without `--deploy` or `--execute`, deployment makes no changes. Review pilot outcomes before preparing a full run without `--pilot`. The API route and status assets are published.

## Website progress

`/extraction-status/?country=KE` polls the existing API every 15 seconds while visible. The API reads a small progress object, with a 15-second cache, and makes no BigQuery query. It reports saved, blocked/rate-limited, unavailable, inspection-needed, robots-related and temporary failures, plus pending URLs and per-outlet progress. Counts of attempts/retries include completed URL outcomes; active retries appear as downloading until they finish. The page warns about stale progress rather than assuming the crawler is running.

Raw content remains private; public data is operational progress and object-free summary information.

## Local validation

```sh
python -m unittest discover -s tests -p test_crawler.py
python -m compileall -q crawler backend/extraction.py scripts
node --check docs/extraction-status/status.js
```

The eight retrieval tests mock HTTP and storage. Cloud IAM, image build, real publisher requests and end-to-end progress publication were subsequently verified in the pilot.

## First pilot result

Run `ke-pilot-20261003-160721`, execution `softpower-crawler-gjcrn`: 24 selected URLs, 21 processed, 3 pending on paused domains. Outcomes: 7 saved, 3 HTTP 404/410, 1 blocked, 4 needing inspection, 3 robots-policy retrieval failures, 3 temporary network errors. There were 27 attempts including 6 retries. The worker ran for approximately 85 seconds; its nominal compute estimate was $0.0017, excluding other services and startup overhead.

All 21 outcomes and the final state were verified in BigQuery. All 7 saved rows reference both original bodies and extracted text. A downloaded original was decompressed and its SHA-256 matched the stored hash. Successful text extraction alone does not establish article relevance or accuracy.

The sample deliberately uses date extremes from the largest outlets; do not extrapolate its recovery rate to the full corpus. Full collection remains unstarted, with no scheduled continuation.

## Automated toolbox restart (version toolbox-2)

The worker now invokes `pipeline.py` for every input, not the manual recovery report scripts. The same input manifest is copied with `scripts/restart_crawl_pilot.py --from-run ORIGINAL_PILOT --apply`; no source scan or new sample is used.

Ordered stages: cached-original/HTTP retrieval and bounded retries; structured article-body fields, HTML body selectors and precision/fallback parsers; discovery of up to two same-host canonical/OG article URLs; Chromium rendering for successful unrestricted HTML; and Wayback availability lookup near the observation date plus the latest snapshot. Distinct snapshots are downloaded and run through the same quality checks. No manually selected replacement URLs or LLM/API-based content judgments are used. The earlier manually found republication is not injected into this restart.

A structurally usable article candidate can stop recovery early. Otherwise every applicable configured stage is attempted or logged as not applicable. A temporary error, browser failure, archive service outage or per-URL time limit results in `deferred`, not terminal failure. Terminal `partial` and `exhausted` mean the configured toolbox was exhausted, not that every possible copy on the internet was searched. Current URL discovery does not search the whole publisher site or use a paid search API. Archived versions are identified by recovery URL and snapshot timestamp and can differ from the article on its original observation date.

Access restrictions are not bypassed. Browser rendering is not used to evade a challenge, robots exclusion, or paywall. Structured bodies are not used to bypass a page explicitly marked restricted. Browser work is serialized; images, fonts and media are blocked, network destinations are checked, and service workers/downloads are disabled. A browser navigation has a 35-second timeout plus up to eight seconds to settle, and a rendered document has a 5 MiB limit. The URL scheduling budget is five minutes checked between stages; in-flight stages may exceed it. The pilot uses three concurrent URLs, one browser, and the existing one-CPU/1-GiB job. No new cloud permissions were added.

Each stage persists under `runs/RUN_ID/toolbox/ARTICLE_ID.json`, with the final history in `crawl_results.attempts_json`. Final text is under `runs/RUN_ID/extracted/`; the original table still joins by exact input URL. Existing first-pass `saved` records are re-extracted through the new quality checks, rather than accepted as proof of full text. Worker checkpoints skip completed URL outcomes on resume; unfinished URLs restart their toolbox and can reuse stored successful HTTP bodies. Stage logs preserve evidence but are not a promise of exactly-once network requests.

The public status snapshot now includes live stages and expandable completed-URL histories. It distinguishes full-text candidates, terminal partial/no-text outcomes, and deferred work. It does not serve article bodies.

### Verified automated pilot

Final run `ke-toolbox-20261003-175851`, execution `softpower-crawler-btcp2`, image tag `ke-toolbox-20261003-165548-r4`: 24/24 URLs assessed automatically, with 16 full-text candidates, 5 partial results, 2 exhausted without usable text and 1 deferred for retry. Eight pipeline tests and eight original retrieval tests pass, with additional checks against 12 stored real pages. Verification compared the input manifest byte-for-byte to the original pilot, confirmed all 24 BigQuery results, downloaded every saved candidate's text and original response, validated content hashes and checked terminal stage coverage. Automated candidates are not a guarantee of perfect article completeness.

The pilot exposed and corrected outdated article-body selectors, an invalid empty archive timestamp, fallback navigation text on pages with an empty article body, and subscription previews misclassified as listings. Earlier diagnostic runs remain preserved. No LLM API calls were used and the full corpus has not been launched. The same status page now shows this automated run with each URL's complete stage history.

### Automatic retries and final-result semantics

`RetryingPipeline` now retries an incomplete toolbox pass automatically, with a 30-second delay and a maximum of two passes. Intermediate `deferred` outcomes are internal only: they never enter the worker's completed results. Retry state and attempt history are persisted. When the limit is exhausted, the URL becomes `failed` with the reason retained. Resuming an older run requeues its deferred URLs while retaining terminal results.

The existing pilot was resumed for its one outstanding URL. Execution `softpower-crawler-l94b5` actually performed both recovery passes, then recorded failure. BigQuery verification found 24 terminal records and no deferred/retrying records: 16 full-text candidates, 5 partial, 2 exhausted and 1 retry-exhausted failure. The dashboard combines the latter two statuses as 3 failures. It now displays only three outcome counters and one article table, with technical history under Details. Its completion count excludes pending/retrying URLs.

### Full Kenya collection launched 2026-10-03

Run `ke-full-20261003-192939`, execution `softpower-crawler-wjfrb`, image
`full-websites-20261003` uses all 85,993 distinct Kenya-source URLs from
2015-01-01 through 2025-12-31. Six workers operate across outlets; each outlet
has serial requests with a minimum three-second spacing. The execution pauses
at 23 hours, 40 GiB of recorded responses, or 150,000 recorded HTTP attempts.
These are operational limits, not a guaranteed billing cap. A paused run must
be resumed with the same run ID; terminal checkpoint results are retained.

The status page now has expandable per-website summaries. Public snapshots no
longer contain URL lists or individual attempt histories. Originals, extracted
text, checkpoints and stage histories remain private in Cloud Storage; result
references and source-table/URL links remain in BigQuery `crawl_results`.

Resume after inspecting the recorded stop reason:

```sh
python scripts/deploy_crawler.py --run-id ke-full-20261003-192939 --execute
```

Do not execute another worker while one is active. An operator can stop new
scheduling by creating `runs/ke-full-20261003-192939/STOP` in the crawl bucket.

### Memory incident and recovery, 2026-10-04

Execution `softpower-crawler-wjfrb` was terminated by Cloud Run for exceeding
its 1 GiB memory limit. Its final public snapshot was still marked running;
the UI now changes stale running snapshots to “Worker not reporting”.
The job memory limit was raised to 2 GiB, the existing run configuration was
reduced to three workers, and execution `softpower-crawler-w5jjk` was launched
with the same run ID and saved checkpoints. This provides memory headroom;
it does not establish that peak memory is bounded for every publisher page.
