# Article collection pipeline

Status: the full Kenya collection is running (85,993 URLs). The sections below include historical pilot and deployment notes; the latest concurrency configuration is described at the end. Public progress: https://djelleldifallah.com/softpower-audit/extraction-status/?country=KE .

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

### Memory-efficient execution

The controller now retains only compact latest-result metadata and counts.
Full histories are persisted in the existing GCS/BQ formats, with checkpoint
buffers bounded at 50 rows / 4 MiB and BQ buffers at 1,000 rows / 8 MiB (a
single oversized record is sent alone). Checkpoints and the input manifest
are streamed on restore, without constructing a full decoded copy. BQ replay
is batched and retains the existing latest-row deduplication semantics.

Pipeline HTTP retrieval no longer runs a redundant initial text parser.
The unchanged structural extractor runs in a fresh process per document;
Chromium rendering also runs in a disposable process group. Only one heavy
parser/render task runs at a time, while HTTP retrieval can run concurrently.
Browser document requests still use the parent's robots policy and shared
host pacing. Slot wait time does not consume the article's recovery budget.
Process-group cleanup stops browser descendants. Parser address space is
limited to 384 MiB on Linux; a container pressure check aborts an expensive
stage for normal retry handling. Existing browser/archive recovery and
quality criteria remain in place; resource failures are not accepted as text.

Private progress includes cgroup current/peak/limit bytes. At 75% container
memory usage, the scheduler stops adding work and drains active work; if
pressure persists it saves progress and pauses. Memory checks are sampled,
not a guarantee against instantaneous spikes. The subprocess time limit can
be extended by a synchronous parent robots request or host pacing.

`VERIFY_MEMORY=1` runs a bounded verification job (saved pilot originals,
100,000 compact result records, and a local JavaScript-only article fixture)
without taking the crawler lease or publishing collection status. Its private
report is stored under `verification/memory-v1/result.json` in the crawl bucket.

Cloud verification `softpower-crawler-spsq8` passed at a 1 GiB ceiling:
21 saved original pages had identical complete extraction outputs; three
JavaScript-only browser recoveries passed and request denial was respected.
Cgroup usage was 90.1 MiB initially, 126.1 MiB with 100,000 compact index
entries, and peaked at 568.1 MiB across the test (including Chromium and
filesystem cache). This is a bounded test measurement, not a claim that all
publisher pages have the same peak. All 40 local tests passed.

### Automatic memory recovery

A dedicated watchdog checks the container memory every 100 ms, including
while browser request approval waits for robots or host pacing. At 85% of
its memory limit it kills only the isolated parser/browser process group;
that stage enters the existing bounded retry path, and later articles can
continue. Crashes and broken process communication are retryable too.

At 75% memory the controller drains active URLs, persists buffers, runs GC
and requests allocator release. If pressure remains, it publishes
`recovering_memory`, releases its lease and replaces itself with a fresh
Python interpreter after ten seconds. The replacement uses one worker and
restores checkpoints. Three such recycles are allowed per Cloud execution;
the counter and start time are persisted, so recycling cannot reset the
runtime limit. Persistent failures surface as `recovery_failed` for attention.

Cloud Run also has three task retries for abrupt process/container failures.
A higher task attempt can reclaim the preceding attempt's lease only when
the execution and task index match. Other executions cannot take a live
lease. The same execution start time applies to Cloud retries. These are
bounded recovery mechanisms, not a promise to survive every possible fault.

`VERIFY_RECOVERY=1` runs a fault-injection execution with a private test lease
and checkpoint. It deliberately kills attempt zero, verifies Cloud Run retries
it, reclaims its own old lease, and interrupts one parser before successfully
extracting another page. It does not take the collection's global lease or
fetch publisher pages.

Recovery verification `softpower-crawler-8qqmz` completed successfully after
one Cloud Run retry: task attempt 1 resumed the private checkpoint and reclaimed
the terminated attempt's test lease. A forced parser memory interruption was
followed by a successful extraction. The earlier SIGKILL-self test was invalid
because the container's PID 1 did not terminate; it is superseded by this
`os._exit(137)` test with an explicit task-attempt assertion. All 51 local tests
pass. The production run was checkpointed at 4,116 URLs before switching to
image `memory-recovery-20261004-r3` at the existing 1 GiB ceiling.

### Network concurrency increase

The Kenya run configuration now permits six concurrent article pipelines
(up from three). The single parser/browser slot, per-host serial requests and
minimum three-second spacing remain unchanged. Archive calls share the archive
host throttle, so throughput is not expected to scale linearly with threads.
Memory-pressure backoff and automatic recovery to one worker remain active.
This changes concurrency within the same 1 CPU / 1 GiB Cloud Run task, not the
number of cloud tasks. The updated setting takes effect on checkpointed resume.

### Higher throughput configuration, 2026-10-04

The higher-throughput deployment uses one Cloud Run task with 4 vCPUs / 4 GiB,
48 concurrent article pipelines and up to four articles per outlet. Large
outlet backlogs start first. An article can wait for parsing or archive recovery
while another article from its outlet proceeds. Ordinary HTTP requests remain
serialized per host; browser document starts use the same host pacing (browser
assets can load concurrently). The minimum request interval remains three
seconds, or a longer robots-specified delay. Shared archive hosts have one
shared throttle, not a separate allowance for every source outlet.

`CRAWL_HEAVY_SLOTS=3` permits up to three isolated parsing/rendering processes;
`CRAWL_BROWSER_SLOTS=1` limits those to one browser. Waiting browsers do not
occupy parser slots. Host/robots/pacing queue waits are excluded from article
recovery deadlines, so concurrency does not prematurely exhaust the toolbox.
The existing process-group memory watchdog, checkpointing, bounded Cloud Run
retries and automatic memory recovery remain active. No URL subset or extraction
stage is removed to increase the completion count.

Deploy the image with explicit resource parameters, then resume the existing
run only after its preceding execution drains and releases the global lease:

```sh
python scripts/deploy_crawler.py --run-id IMAGE_TAG --deploy \
  --cpu 4 --memory-gib 4 --heavy-slots 3 --browser-slots 1
python scripts/deploy_crawler.py --run-id ke-full-20261003-192939 --execute
```

The run configuration uses `workers=48`, `per_outlet_workers=4`, and
`max_total_attempts=300000`. The 40 GiB response and 23-hour scheduling limits
remain; reaching an operational limit pauses remaining work without calling it
complete. These limits are not a guaranteed billing cap. At published rates,
compute is about $0.288/hour ($2.88 for ten hours), before free allowances and
separate storage, operations, build and network charges.

The requested ten-hour completion is a target, not a promise: at the start of
this change about 81,400 URLs remained, including 19,625 at standardmedia.co.ke.
At three seconds between requests that outlet alone needs about 16.4 hours for
one request per URL, before robots checks, redirects, retries or archive work.
Additional CPU or threads cannot remove the publisher/archive rate limits.

Verification: all 81 local tests passed. Cloud execution
`softpower-crawler-hlqkh` passed the concurrent extraction/browser check with
identical outputs for 21 preserved articles, three JavaScript renders, request
denial enforcement and a 100,000-result compact index. Sampled peak memory was
about 570 MiB in this bounded fixture (not a production-wide peak guarantee).
The private report is `verification/parallel-20261004-r1/result.json`.
The prior execution drained completely at 4,671 terminal URLs; execution
`softpower-crawler-ktlxv` was launched with image `parallel-20261004-r1` to resume
the remaining 81,322 URLs with the same input manifest and checkpoints.

Live verification at 05:48 UTC confirmed 48 active article pipelines, 4,778
terminal results (107 beyond the restart checkpoint), and matching public API
counts. Peak container memory at that point was about 909 MiB. An initial
111-second observation saw 88 completions (about 2,849/hour); this short,
outlet-dependent sample is not a reliable whole-run ETA and is below the
roughly 8,100/hour required for ten hours. Forty of the 48 active pipelines
were in archive lookup/replay stages, indicating external recovery throttles
are now the principal constraint rather than available parser slots.

### Continuous collection across Cloud Run executions, 2026-10-05

Full runs now have `max_runtime_seconds: null`: elapsed time does not stop the
collection. Pilots may still specify a short explicit runtime budget. The
existing Kenya run resumes the same 85,993-URL manifest and saved checkpoints.

Cloud Run requires a finite task timeout (seven days maximum). Production tasks
use that maximum and begin a graceful rotation after six days:
`CRAWL_ROTATE_SECONDS=518400`. They stop scheduling new articles, drain active
work, flush all checkpoints, and release the global lease. The final state is
`continuing` only when unfinished work remains and no stop, error or resource
limit intervened. Task-attempt age survives interpreter memory recovery; a new
Cloud Run task attempt gets a fresh platform timeout.

The `softpower-crawl-continuation` Cloud Workflow waits for each execution and
starts its replacement only after checking that the private progress snapshot
belongs to that exact run and execution, has no active downloads, and explicitly
requests continuation. It checks `STOP` before launching. Completed runs,
operator stops, exhausted byte/attempt budgets, and failures do not loop. A
connector failure stops the controller instead of risking a duplicate worker.
The Cloud Workflow itself has Google's one-year execution limit; there is no
application-level collection deadline or periodic restart schedule.

Deployment order (the coordinator uses a dedicated service account with access
only to the crawl job, operation polling, and operational GCS snapshots):

```sh
python scripts/deploy_crawler.py --run-id IMAGE_TAG --deploy \
  --cpu 4 --memory-gib 4 --heavy-slots 3 --browser-slots 1
python scripts/deploy_crawl_coordinator.py --deploy
python scripts/deploy_crawler.py --run-id ke-full-20261003-192939 --execute
```

`--execute --direct` explicitly launches one execution for a pilot or verification
fixture. Normal full runs use the workflow. Repeated starts reuse an active
controller for the same run; the worker's global lease additionally prevents
overlapping crawls. Article/request timeouts, bounded retries, memory protection,
the 40 GiB response budget and 300,000-attempt budget are unchanged. Those limits
are independent of elapsed runtime and are not a guaranteed billing cap.

Verification: 106 local tests pass, including graceful rotation, draining the
last article, STOP during draining, byte/attempt limits, memory-attempt age,
lease-release failures, conditional IAM preservation and duplicate-safe launch.
The cloud fixture `verify-continuation-20261005-0833` used exactly two distinct
Cloud Run executions (`softpower-crawler-continuation-check-mmrn6` and
`softpower-crawler-continuation-check-ss4qk`), then finished with zero pending
work. A separate STOP fixture launched zero executions. The private evidence is
`verification/continuous-20261005-r2/continuation.json`; temporary test job and
workflow resources were removed after verification.

Production image `continuous-20261005-r2` is deployed with the same 4 CPU / 4 GiB,
48 article pipelines, four per outlet and three isolated heavy-process slots
(one browser). The existing Kenya config's runtime limit is now null; its prior
config is preserved as `runs/ke-full-20261003-192939/config-before-continuous-20261005.json`.
Workflow execution `2f4e634b-6042-4902-9a1b-0c3f727f84a4` launched worker execution
`softpower-crawler-d6xsv` to resume from 17,922 completed results. The public
status page recognizes the automatic `continuing` state.

Live verification at 09:54 UTC confirmed all 17,922 prior results were restored,
48 active article pipelines, 17,933 terminal results and 14,490 saved full texts
(11 additional full texts after restart). The public status API reported the
same running state with no error. All 18 checkpoint replay loads completed with
no bad records or load failures.

### Multiple crawler instances with one shared list, 2026-10-05

The distributed deployment uses four Cloud Run tasks in one execution. Each
instance has 48 article slots, four pipelines per outlet, 4 CPUs / 4 GiB, three
isolated heavy-process slots and one browser. Instance count is configurable
with `--instances`; the first deployment provides 192 article slots.

The named Firestore database `softpower-crawl` in `us-central1` stores the shared
list and compact coordination metadata. Each URL has one transactional claim
with an owner, a random fencing token and a renewable ten-minute lease. A
replacement may reclaim expired work, while stale owners cannot complete it or
increment counts. Original evidence and extracted text remain in private GCS;
result rows remain in BigQuery. Claim-specific artifact paths prevent a late
worker from overwriting a replacement's accepted text. Retrieval is at least
once after failures; accepted completion/accounting is idempotent.

Host leases are shared across all instances, including archive hosts. HTTP
requests remain serialized per host with shared spacing, robots delays and
Retry-After cooldowns. Browser document starts share pacing and broadcast
429/503 cooldowns; browser assets retain their existing behavior. More instances
increase overlap across sources and extraction stages without multiplying the
per-host request rate. Each worker stops new requests if its article ownership
becomes uncertain. The existing memory guard and parser/browser isolation remain.

Progress is aggregated from 32 compact counter shards and task heartbeats, not
by scanning every article. One public snapshot combines all instances and shows
the active worker count. A durable outbox records results needing BigQuery
export; a load failure cannot lose an accepted article result. The existing
latest-row view removes repeated export events after a retry. Heartbeats and
bounded query/refill intervals keep coordination work proportional to activity.

Migration order:

```sh
python scripts/provision_shared_crawl.py --apply
python scripts/deploy_crawler.py --run-id IMAGE_TAG --deploy --instances 4 \
  --cpu 4 --memory-gib 4 --heavy-slots 3 --browser-slots 1
# Drain the old execution using its existing STOP control; wait for lease release.
python scripts/prepare_shared_crawl.py --run-id ke-full-20261003-192939 --apply
# Remove STOP only after checking imported total and completed counts.
python scripts/deploy_crawler.py --run-id ke-full-20261003-192939 --execute
```

The importer checks the final checkpoint count and manifest fingerprint before
exposing the queue to workers. It preserves completed URLs and never resets a
ready queue. Shared worker access is restricted to this named database by a
conditional IAM binding. Full runs retain automatic continuation and no
application runtime cutoff; byte/attempt limits apply to aggregate results.
Cloud Run CPU/memory charges scale with the number of active instances, while
Firestore bills coordination reads/writes. Idle workers use bounded backoff.

Validation before migration: 177 Python tests pass. A live Firestore contention
check let four independent clients claim 64 distinct URLs with no duplicate
claims; all were returned to the synthetic queue afterward. A shared-host check
confirmed only one admitted holder. The first two-container pilot exposed GCS
Blob generation caching during cohort lease renewal. Renewal and release now
use fresh object handles and retry concurrent 404/412 generation races; a live
four-client concurrent renewal check passed after that fix. The final image is
`distributed-20261005-r3`.

The corrected two-task cloud pilot `softpower-crawler-distributed-check-q8xrf`
completed all 96 synthetic URLs: task 0 accepted 47 and task 1 accepted 49.
Validation confirmed unique claims/results, all 96 immutable GCS evidence
objects, matching aggregate counters, zero remaining work, and an empty
BigQuery export outbox. This tested coordination using synthetic article results;
the existing extraction tests cover the content pipeline. Both cloud tasks
succeeded. Proof: `verification/distributed-20261005-r3/result.json` in the
private crawl bucket. The temporary Cloud Run test job was removed afterward.

For a legacy execution whose in-flight archive requests make draining impractical,
keep STOP in place and cancel that exact Cloud Run execution. After both the
execution and its continuation workflow are terminal, use the explicit recovery
helper with their IDs and the verified old lease owner:

```sh
python scripts/recover_crawl_for_migration.py --run-id RUN_ID \
  --execution OLD_EXECUTION --workflow-execution OLD_WORKFLOW_EXECUTION \
  --lease-owner OLD_LEASE_OWNER --expected-count EXPECTED_INPUT_COUNT --apply
```

Recovery replays every durable checkpoint into BigQuery, reconstructs counts
solely from those checkpoints, and returns interrupted articles to pending.
It preserves original files and publishes `stopped_for_migration`, never clean
completion. Only the exact terminated worker's lease can be released. The
importer requires a finalized recovery proof matching the checkpoint inventory,
manifest/config/progress generations, and count totals. STOP must remain until
import succeeds. This is a one-time migration action, not a runtime cutoff.
Validation including these recovery safeguards: 189 Python tests pass.

Production migration cancelled legacy execution `softpower-crawler-d6xsv` after
STOP because its remaining archive requests shared a serialized host queue.
Its old workflow `2f4e634b-6042-4902-9a1b-0c3f727f84a4` then became terminal.
Recovery replayed all 18,241 durable checkpoint events without errors and
preserved all 18,241 terminal results, leaving 67,752 of the original 85,993 URLs
pending. Proof: `runs/ke-full-20261003-192939/distributed/migrations/softpower-crawler-d6xsv.json`.

The queue import finished with exactly 85,993 records: 18,241 terminal and 67,752
pending. Workflow `2c1d5ed8-a876-4c57-b627-b587c1638a26` launched production
execution `softpower-crawler-ks4gh` with four parallel tasks. Live verification
confirmed four running instances, 192 configured article slots, 190 active
articles in the published snapshot, and at least 33 new terminal results beyond
the imported baseline. The public status API reports all four instances without
an error. Evidence: `runs/ke-full-20261003-192939/distributed/production-verification.json`.
The temporary pilot job deletion and deployed public worker label were also
verified. The existing byte/attempt budgets and automatic continuation remain.
A new production result was additionally checked end to end: its accepted result
object, preserved raw response, and 3,578-character extracted text all exist;
the text uses the expected claim-specific path. Proof:
`runs/ke-full-20261003-192939/distributed/real-fulltext-verification.json`.

### Scale to ten instances, 2026-10-05

The existing job template is now configured with `taskCount=10` and
`parallelism=10` (generation 19), giving 480 article slots. Each instance retains
48 article slots, 4 CPUs / 4 GiB, three heavy-process slots and one browser. The
image remains `distributed-20261005-r3`; only the task count and parallelism were
changed. Regional CPU/memory quota checks permit this deployment.

The initial distributed resize used STOP and cancellation, then waited
until that execution and its continuation workflow were terminal. The
`scripts/release_distributed_crawl_for_scaleover.py` helper verifies the exact old
cohort, the STOP generation, and the ready target template before releasing only
the old global lease. Article claims, completed results, counters, export outbox,
and host cooldowns stay unchanged. Old claims expire normally while the new
instances work on other ready URLs. Remove only the verified STOP and start the
normal continuation controller. No queue reimport was required. This historical
guard-deletion handover is superseded by the atomic reservation procedure below:
Cloud Run cancellation alone did not prove that every late task retry had stopped.

Ten scaleover safety tests pass. An isolated live test with ten clients completed
all 30 simultaneous join/renew operations and cleaned up its test lease. Proof:
`runs/verify-distributed-ten-lease-20261005-141605-2eb0ec62/verification.json`.
The old four-task execution `softpower-crawler-ks4gh` was cancelled and its lease
released only after its controller was terminal. All 19,048 completed results
were retained, with 66,945 remaining URLs. Resize proof:
`runs/ke-full-20261003-192939/distributed/scaleovers/softpower-crawler-ks4gh-to-10.json`.

The first ten-task execution exposed a startup contention issue: task 2 exhausted
12 shared-lease CAS retries, exited with code 1, and its native retry correctly
entered the conservative one-slot fallback. Nine tasks continued with 48 slots
(433 effective slots in total); there was no out-of-memory failure. The previous
capacity report also incorrectly multiplied the publishing task's reduced limit
by all tasks.

The `distributed-20261005-r4` correction lets peers reuse their cohort lease
while it has more than five minutes left; one peer refreshes it when needed.
Article leases still renew independently. Public status reads use a fresh GCS
handle, avoiding a separate stale-generation race. Configured cohort capacity is
stable, and private per-worker heartbeats report effective capacity separately.
The memory/unknown-crash safeguards remain enabled.

All 204 tests pass. A live ten-client test performed 40 successful operations:
initial joins, a no-write fresh-lease round, and two forced near-expiry rounds.
Each required renewal had exactly one successful writer; concurrent conflicts
were handled successfully. The slowest operation took 2.489 seconds. Proof:
`runs/verify-distributed-ten-refresh-20261005-144526-caec5997/verification.json`.

Corrected production execution `softpower-crawler-wkzlt` ran image
`distributed-20261005-r4`, launched by workflow
`120dac3d-db9b-4193-bc11-46571625e1bb`. The rollover preserved 19,310 completed
results. Verification on 2026-10-05 around 15:01 UTC confirmed all ten live
workers reporting both configured and effective capacity of 48 each, total
configured/effective capacity 480, and over 60 seconds of stable capacity.
The sample contained 431 distinct active URL claims and no duplicate assignments.
The authoritative count had advanced to 19,359, including 47 additional saved
full texts. A new result's original response and extracted text were verified
in GCS; the public status API also reports ten active instances and 480 slots.
Proof: `runs/ke-full-20261003-192939/distributed/scale-to-10-r4/verification.json`.

### Separate publisher collection from archive recovery, 2026-10-05

The r4 cohort was healthy but eventually had 477 of 480 article pipelines in
archive stages. Whole-article futures held their slots while waiting for the
same two archive hosts, leaving publisher websites idle. Worker count alone did
not deliver proportional throughput.

Image `distributed-20261005-r5` runs durable publisher and archive phases.
Each 48-slot instance reserves 46 publisher slots and at most two archive slots.
Publisher work retains HTTP, discovered canonical URLs, extraction, and eligible
browser rendering. If archive recovery is needed, it saves an immutable private
checkpoint and releases the publisher slot. A recovery claim resumes that
checkpoint without repeating publisher requests. Checkpoints retain partial
text, original-response references, attempted stages, counters, useful elapsed
time, and retry-pass state. Whole-pass retries return to the publisher queue with
a due time instead of sleeping in a slot. All applicable recovery attempts remain.

Firestore uses the existing indexed `due_at` field for publisher work and a
separate indexed `archive_due_at` field for recovery. Legacy records need no
rewrite. A phase handoff is fenced by owner, token, phase, and lease expiry; it
does not increment processed counts or produce a BigQuery result. Cumulative
attempt/byte metrics are accounted at handoff and only their remaining delta at
completion. `publisher_remaining` and `archive_remaining` include their active
claims; both remain unfinished work. Completed results and outbox acknowledgments
retain their existing semantics.

BigQuery exports now take up to 500 results per batch, retaining the 8 MiB batch
limit. Only eight immutable result objects are read concurrently, and records
that do not fit remain in the outbox. Worker heartbeats also run during bursts of
completed article futures. Per-host pacing, resource limits, memory safeguards,
collection budgets, and automatic continuation remain enabled.

All 235 Python tests pass. A live isolated test with ten clients handed off and
completed 20 synthetic articles, verified checkpoints and actual archive queries,
and repeated handoff/completion calls without duplicate counts. Exactly 60
attempts, 3,000 response bytes, and 20 saved test results were recorded. No
publisher requests or production articles were used in this test. Proof:
`runs/verify-distributed-phases-20261005-175913-cb3f40/verification.json`.
Cloud Build `1e3d7ed8-8822-410a-89ad-3a8f868d62ac` succeeded; its uploaded source
was compared byte-for-byte with the four tested crawler modules.

The r5 rollover retained all 20,544 completed results, including 16,646 saved
full texts, in the same 85,993-URL queue. Workflow
`0ad32b27-d1d1-42b1-b2da-a95d2c0da3d1` launched execution
`softpower-crawler-vdwqp` at 18:07 UTC. Its image digest is
`sha256:d501160a624627a1ff5488f89128f2b37558e8214a3785f56be9a5e26a13325a`.
Rollover and launch evidence are under
`runs/ke-full-20261003-192939/distributed/phased-r5/`.
The last 586 seconds before stopping r4 produced 56 full texts, approximately
344 saved texts/hour; this is an observed window, not a controlled benchmark.

r5 verification observed 251 new saved texts in 304 seconds (2,969/hour), with
all ten instances healthy and at most 20 archive claims. It verified a real
saved original/text pair and a queued archive checkpoint. However, browser
stages grew from 16 to 107 during the sample: rendering still occupied ordinary
publisher slots. Evidence: `distributed/phased-r5/verification.json` under the
same run prefix.

Image `distributed-20261005-r6` also separates browser rendering. Each instance
now admits up to 44 publisher claims, two browser claims, and two archive claims,
within the same 48-slot limit. Only one actual Chromium process family per
instance may run; the second browser claim waits without taking a heavy-process
slot. Publisher HTTP/canonical extraction hands off eligible rendering with its
response metadata and best candidate preserved. Browser recovery either saves
the text or hands it to archives. Ineligible browser work records its reason
and goes directly to archives. No recovery tool is removed.

The third phase uses indexed `browser_due_at` and the `browser_remaining`
counter, with legacy defaults of zero. r5 archive checkpoints remain valid and
skip already-attempted browser work. All 244 Python tests pass, including literal
r5 checkpoint compatibility, three-phase retries, queue-time exclusion, and
independent recovery caps. A live ten-client test completed 20 synthetic
publisher/browser/archive sequences with exactly-once metrics. Evidence:
`runs/verify-distributed-three-phases-20261005-181944-45b14d/verification.json`.
After browser-phase records exist, rollback must use a browser-aware worker or
a forward fix: unchanged r5 cannot claim `browser_due_at` work. Never reseed the
queue or discard these checkpoints during a rollback.

### Atomic handover and retired-worker fencing

The first r6 launch exposed late retries from the cancelled r5 execution.
Although Cloud Run reported the old execution cancelled, replacement task
attempts subsequently reclaimed the deleted cohort guard. That blocked all ten
r6 tasks at startup and triggered their conservative retry fallback. This was a
handover error, not a memory failure or a phase-processing failure. Both
executions were retired; their completed results and pending checkpoints were
preserved, and a held reservation fenced them out.

`scripts/crawl_handover.py` replaces the exact old guard with a 30-minute
reservation, then transfers it directly to the verified new execution with a
10-minute expiry. There is no unowned interval. Coordinator `start()` accepts
the matching `handover_id`, still using its existing launch deduplication and
generation checks. The caller keeps STOP and the queue stop flag set while
retiring old executions, creates immutable markers at
`runs/<run_id>/retired-executions/<execution>.json`, verifies the ready job and
terminal old controllers, then clears its own stop and launches while holding
the reservation. It verifies the new execution's run/image/task identity and
transfers the reservation before its containers start. Retain launch and
transfer proofs; never blindly repeat an ambiguous creation request.

r7 workers check STOP and retirement before acquiring a guard, and check
retirement on renewal. Only the current live owner may publish progress, with
ownership rechecked before private and public writes. A peer may read an already
verified terminal snapshot after final guard release without writing again.
Late retired tasks cannot restart crawling or replace the current status.

A live isolated guard test rejected 20 stale-client reservation attempts across
reservation and transfer, while maintaining continuous ownership. Production
queue state was untouched. Proof:
`runs/verify-distributed-handover-20261005-183853-332ba8/verification.json`.

r8 workers also wait up to three minutes if they start while the same run's
handover reservation is still held. Waiting does not consume native retries or
reduce article capacity; STOP and retirement remain enforced throughout.
Terminal workers reread the latest task attempts before releasing the cohort
guard, and failed cohorts retain ownership for native retries. These checks
prevent a late attempt or stale terminal snapshot from reducing the replacement
fleet to one slot per task. All 280 Python tests pass. The r8 uploaded source was
verified byte-for-byte against the tested worker, pipeline, retry, and queue
modules.

The r8 build `1d66ca1a-5d50-4e6b-9f53-0a7355465e3f` succeeded. Controller
`dd73815a-9105-480f-bc74-4a07131ee0f3` launched execution
`softpower-crawler-chplt` at 18:55:47 UTC. The continuously held reservation
was assigned to that execution after verifying its exact build digest and ten
tasks. The same queue resumed with all 21,305 completed results, including
17,383 saved full texts. Launch evidence is under
`runs/ke-full-20261003-192939/distributed/phased-r8/`.

r8's first five-minute window saved 53 full texts (615/hour), while the next
window saved 275 in 309 seconds (3,205/hour). All ten tasks stayed on native
attempt zero with 48 slots each; browser and archive claims stayed at or below
20 each. A real saved original/text pair and both durable recovery queues were
verified. Proofs are `verification.json` and `steady-verification.json` under
the r8 prefix. These are observed windows with changing outlet mixes, not a
controlled scaling benchmark.

### Spread initial publisher scans across the queue

The slow restart exposed a second admission bottleneck: every task initially
scanned the beginning of the same `due_at` index. A sample of its first 2,000
ready publisher rows contained only six outlets, while later hash ranges had
28–31 outlets per 250-row sample. The per-instance outlet cap filled waits for
the same slow sites before bounded cursor pagination reached other available
websites. Current robots rules also require 20 seconds for Kenya Star and ten
seconds for Capital FM's redirected host; those limits remain enforced.

r9 gives each task an initial publisher cursor at the midpoint of its equal
SHA256 stratum, using the existing `due_at` and document-ID ordering. These are
starting points, not fixed partitions: normal pagination and wrap still visit
every record, including lower IDs and due retries. Browser/archive scheduling is
unchanged. Refills still read at most two bounded pages; no new index or article
rewrite is required. All 287 Python tests pass, including distinct starting
points, access beyond a saturated front, wrap completeness, future-due exclusion,
bounded reads, real-SDK cursor serialization, and unchanged default behavior.

An isolated real-Firestore test used ten clients to claim 40 distinct articles
across nine hash bands/outlets, then completed all 240 synthetic articles exactly
once despite replaying every completion. A separate high-start cursor completed
all 12 records, including eight before its initial position. No publisher traffic
or production queue mutation was involved. Proof:
`runs/verify-distributed-cursor-20261005-191036-67ff17/verification.json`.

Cloud Build `73cb9950-5f70-4c44-be4b-60fccbb30b43` succeeded; all four uploaded
crawler modules matched the tested source byte-for-byte. The atomic handover
retired r8 and transferred the same queue to `softpower-crawler-5kbpm`, started
by controller `c7e1a258-0129-45dd-bd28-20c4bf251501` at 19:16:22 UTC. All 21,931
completed results, including 17,975 saved full texts, were retained. Launch and
reservation proofs are under `distributed/phased-r9/` in the same run prefix.

The first meaningful r9 snapshot at 19:20:33 UTC had 308 publisher claims across
37 outlets (306 HTTP stages across 36), roughly 70 seconds after task startup.
This confirms that the deployed cursors reach the available later outlets
immediately. All ten tasks remained on attempt zero with 48 slots each.

Final r9 production verification covered 19:19:29–19:24:40 UTC: 325 new saved
full texts in 311.2 seconds, equivalent to 3,760/hour, and 335 terminal results
(3,875/hour). This is about 10.9 times the r4 observed saved-text rate of
344/hour; differing outlet mixes and the short measurement window mean this is
not a guarantee for the recovery-heavy tail. All ten tasks remained healthy on
attempt zero, with 480 effective slots, unique active assignments, and at most
20 browser plus 20 archive claims. A newly saved original/text pair and queued
browser/archive checkpoints were verified in GCS. The collection reached
22,266 terminal results, including 18,300 saved full texts. Proof:
`runs/ke-full-20261003-192939/distributed/phased-r9/verification.json`.

### Recovery fairness and stored HTML reanalysis (6 October)

The five-minute r9 result above did not hold over the following hours. An
article-deduplicated read of the result events showed 773–861 saved texts per
full hour between 21:00 and 02:00 UTC. At 02:31, 27,072 articles were waiting
for archive recovery and 1,372 for browser recovery; publisher work was mainly
Kenya Star (12,830) and Standard Media (12,591). Kenya Star's 20-second robots
interval alone implies at least 71 hours for those original requests. Ten task
instances cannot multiply a shared host's allowed request rate.

Recovery scans now revisit the oldest due rows on every refill. Previously a
48-row page was shuffled and its cursor advanced even when only two recovery
slots were filled, allowing older rows to be repeatedly bypassed. Publisher
scans retain their distributed rotating cursors. Recovery reads remain bounded
to two pages, and transactional claims still prevent duplicate ownership.

A confirmed redirect to a bare homepage skips browser rendering and continues
archive recovery. Query- and fragment-routed pages remain eligible. Chromium
admits each actual request once through CDP interception, including redirect
hops, replacing the duplicate preflight admission. Auxiliary frames, popups,
and image/media/font requests remain blocked. Local real-Chrome tests verify
redirect admission, denied targets, auxiliary requests, and Retry-After reports.

BusinessDaily's scoped `article-story` body is now recognized despite many
related-story cards. This selector applies only to the exact publisher hosts
and their recognized Wayback replay URLs, retaining paywall and quality checks.
A versioned checkpoint resume reparses previously stored successful HTML before
making new recovery requests. Results retain their original raw reference;
partial candidates and cumulative accounting survive the reanalysis. Missing
or corrupt objects continue ordinary recovery. No LLM is involved.

Validation: 314 Python tests (311 passed; 3 browser tests opt-in), plus all three
opt-in local Chrome tests passed. The real stored-HTML check and deployment
proofs are recorded separately; throughput must be assessed over complete hours,
not extrapolated from an initial burst.

A bounded read-only check of 30 pending BusinessDaily browser checkpoints and
30 archive checkpoints found no immediately recoverable full texts in that
sample. Archive candidates were subscription previews (11) or listing pages
(19). A separate real stored archive body did produce 4,742 characters through
the complete reanalysis path, using an explicitly synthetic legacy checkpoint,
with zero publisher/archive/browser requests and zero production writes. This
validates the path; it is not evidence of a production backlog recovery rate.

Cloud Build `c8bb41d7-aa66-4350-bb9a-e8283155326f` succeeded. The uploaded changed
modules matched the tested files exactly. The atomic handover retired r9 and
transferred the existing queue to `softpower-crawler-wcjsk`, launched by
controller `86f0e295-02ce-49b0-ab31-8fec7281ab9b` at 03:54:28 UTC. All 31,277
finished URLs, including 26,753 saved texts, were retained. Launch and reservation
proofs are under `runs/ke-full-20261003-192939/distributed/phased-r10/`.

At 03:58:16 UTC, 23 of the 24 oldest recovery records sampled before handover
had been claimed or advanced by r10 (12 archive, 11 browser). Stored checkpoints
confirmed old Citizen TV homepage redirects now skip rendering and proceed to
archive recovery. This directly verifies the scheduling and homepage fixes;
claiming recovery work is not the same as finishing it or recovering full text.

### Separate start spacing from simultaneous downloads (6 October)

The old fleet-wide host lease was exclusive for the full HTTP response. Ten
instances therefore had the same single-download capacity for a hostname as
one instance. The new coordinator maintains separately fenced, expiring permits
per host. The deployment defaults to four simultaneous responses per hostname
and one second between starts; verified stricter robots rules and shared
Retry-After cooldowns still take precedence. Legacy exclusive leases remain
exclusive until released or expired. Old imposed spacing can be lowered only
once a robots lookup distinguishes that default from the site's actual rule.

`--host-concurrency` and `--request-spacing` set `CRAWL_HOST_CONCURRENCY` and
`CRAWL_REQUEST_SPACING`. The latter overrides the stored run's previous default
without rewriting its manifest or losing checkpoint compatibility. Local HTTP
mutexes no longer wrap shared-coordinator downloads. Browser main documents
hold a permit for the render, release it before a redirect's next robots check,
and renew/check/release it even across callback threads or exceptions. Browser
script/XHR traffic retains the existing resource policy and request-count cap.

Before claiming new publisher articles, workers batch-read host availability
and leave known-full, cooling, or not-yet-due hosts queued. Each refill admits
at most one new article per actual URL hostname. The atomic network admission
still protects against races after that advisory read. Later redirects, robots
requests and recovery requests may wait safely; this is not a claim that every
mid-article wait has become asynchronous. Recovery checkpoints and retry counts
are unchanged. Full hosts are rechecked within three seconds rather than using
the empty-queue 30-second backoff.

Worker progress now distinguishes actual HTTP requests in flight, HTTP starts,
responses completed, transport errors, response time, admission waiters and time
spent acquiring coordinator permits. Article slots are not network requests.
Counters are per worker attempt; live gauges exclude stopped/expired workers.
Admission time includes Firestore latency and excludes robots-cache mutex waits.

`tests/verify_host_scaling.py` is an opt-in local HTTP-only benchmark with 48
article threads per simulated instance. Three slow local hosts stayed near 14.4
requests/second with the old cap at 1, 2, 5 and 10 instances. With four permits,
they reached about 55 requests/second and 12 overlapping downloads (3.8x).
With 120 independent hosts, 1/2/5/10 instances reached about 209/373/788/1,151
requests/second and up to 48/96/240/480 actual downloads. All host bounds and
spacing assertions passed, including a real fixture 429 and a 32-way admission
race. These are synthetic results excluding Firestore latency, dispatch,
extraction and storage, not a production throughput forecast.

An isolated real-Firestore test admitted exactly four of 20 competing clients,
with zero transaction errors. SDK batch reads, independent lease release/renew,
stale ownership, cooldowns, 20-second robots policy and legacy migration passed.
All six test documents were deleted; production coordination was untouched.

The Firestore integration check is reproducible with
`python tests/verify_shared_host_capacity.py --output /tmp/shared-host-proof.json`.
It writes only to its unique verification collection and deletes its own test
documents. All 348 regression tests, including local Chrome fixtures, passed.

Cloud Build `ec32a7d2-7661-42f7-9d4b-15f15f7952c4` succeeded and its uploaded
crawler code matched the tested files. The atomic handover retired r10 and
transferred the queue to `softpower-crawler-2z4zs`, launched by controller
`ed1229b7-c98b-48f4-8fba-8301ec89f5a2` at 06:22:06 UTC. All 34,037 finished URLs,
including 29,228 saved texts, were retained. Launch/reservation and verification
proofs live under `runs/ke-full-20261003-192939/distributed/phased-r11/`.

Production verification at 06:25:16–06:30:21 UTC kept all ten tasks on attempt
zero, with unique article assignments and valid saved original/text objects.
Both `standardmedia.co.ke` and `www.standardmedia.co.ke` reached four overlapping
permits; `archive.org` reached two. No observed host exceeded four. The window
saved 80 texts and finished 95 URLs in 305 seconds; this short changing-workload
sample does not establish a sustained end-to-end speedup.

The Archive availability API still had an inherited three-second default because
that API path does not call the publisher robots cache. Its current official
[robots document](https://archive.org/robots.txt) was checked and had no crawl
interval or request-rate directive for this path. At 06:33:08 UTC, an atomic
compare-and-swap changed only `delay_seconds`, `default_delay_seconds` and
`robots_delay_seconds` to 1, 1 and 0 respectively. Active permits and cooldown
fields were untouched. Evidence is `archive-default-migration.json` under the
r11 prefix. Subsequent admissions retain this setting; stricter learned rules
still override it.

### Archive capacity (r12, 2026-10-07)

Archive recovery can borrow otherwise unused publisher slots, up to
`CRAWL_ARCHIVE_SLOTS` per task (default 2). Publisher admission runs first;
the total remains bounded by the configured article workers and memory
safeguards. Browser recovery retains its separate small pool.

`--archive-concurrency` sets a separate fleet-wide download cap for
`archive.org` and `web.archive.org`. It does not change publisher host limits,
request spacing, robots rules, ownership fencing, or shared 429/503 cooldowns.
The production trial uses eight archive article slots per task and eight
simultaneous downloads per archive hostname, with the existing ten instances.
These are capacity limits, not a guarantee of throughput. Compare saved full
texts and errors over time; adding waiting article jobs alone is not success.

Build `f061df6f-6760-4133-bc31-f1bf3849a5cb` matched the tested crawler sources.
The fenced handover retained 61,733 completed URLs and 49,543 saved texts;
execution `softpower-crawler-nbwlt` resumed the same queue. All 350 crawler
regressions, including Chrome fixtures, and the additional deployment checks
passed. Production proof is under `distributed/phased-r12/verification.json`.

At 01:09:07–01:14:16 UTC, all ten tasks stayed on their first attempt, assignments
were unique, and saved originals/text objects were verified. The 309-second
window saved 81 full texts (944/hour) and finished 138 URLs (1,608/hour).
Wayback reached six overlapping permits, exceeding the old four-permit limit;
no observed host exceeded its configured bound. The short pre-handover sample
was 197 saved texts/hour. This is initial evidence, not a sustained or matched
workload speedup claim. Connection errors and shared archive pacing remain.

### Database retry recovery (r13, 2026-10-07)

The r12 fleet reached 76,827 finished URLs / 59,339 saved texts, then repeated
Firestore `InvalidArgument: The referenced transaction has expired or is no
longer valid` failures restarted all tasks. Their persisted memory restart
counters were zero. The previous fallback incorrectly interpreted any native
retry as memory pressure and reduced each task from 48 article slots to one.

Expired transaction responses now retry with a fresh SDK transaction/wrapper,
with bounded jittered backoff. Other invalid arguments and ambiguous network
commit failures are not replayed by this additional retry layer. Existing
queue and host ownership fences are rechecked against fresh state. Capacity
reduction now requires an observed memory restart; native task attempt numbers
alone do not reduce capacity. Proactive memory recycling and isolated process
limits remain active. All 354 tests, including Chrome fixtures, passed.

At diagnosis the archive queue was empty and the remaining publisher queue was
KenyaStar (about 9,166 URLs), with a verified 20-second robots interval. Restoring
slots addresses the erroneous fallback; it cannot remove that publisher's
fleet-wide pacing or promise linear throughput on a single-host tail.

Live verification rejected the first r13 rollout: the Dockerfile's explicit
module list omitted `transaction_retry.py`, causing startup failure before new
article requests. No completed results changed. The r14 Dockerfile copies all
crawler Python modules and imports the worker/queue/host/retry modules during
image build. Build `4b8ab195-db97-454c-abf7-90fbf0bc3ebc` passed that smoke check
and matched the tested sources. The failed execution was retired with an atomic
handover; r14 resumed 76,834 completed URLs and 59,345 saved texts under
`softpower-crawler-22wh2`. Proofs use `distributed/phased-r14/`.

Production verification at 17:26:37–17:30:14 UTC kept all ten tasks on attempt
zero with 48 slots each and unique assignments. It finished three more URLs
and saved two full texts; original/text objects were checked. KenyaStar's
20-second interval was unchanged. The run reached 76,837 finished URLs and
59,347 saved texts. This verifies recovery, not a throughput improvement claim
for the single-publisher tail. Evidence is `phased-r14/verification.json`.

### Single-instance tail (2026-10-08)

The user requested one instance for the remaining KenyaStar queue. Cloud Run
keeps the verified r14 image, 48 article slots, 4 CPUs / 4 GiB, robots pacing,
archive recovery, memory safeguards and automatic continuation. Only task count
and parallelism change from ten to one. The durable shared queue is retained;
this is not a switch to the legacy single-worker manifest crawler. Future
redeployment of this mode uses `--instances 1 --shared-queue`. Deployment tests
cover both the legacy default and this one-instance shared-queue configuration.

Execution `softpower-crawler-656gd` verified exactly one live task, 48 article
slots, the existing Firestore queue, and no native retries. It resumed from
77,371 finished URLs / 59,755 saved texts and reached 77,373 / 59,756 during
verification. The previous ten-task execution and its controller are retired.
Launch, reservation and verification evidence is under
`distributed/single-instance-20261008/`. CPU and memory allocation are reduced
90%; storage costs and publisher pacing are unchanged.

### Bounded adaptive-rate pilot (2026-10-08)

The user authorized replacing KenyaStar's fixed crawl-delay with measured,
adaptive request pacing. This is an explicit per-host opt-in; other publishers
and archives retain their existing robots pacing and limits. Prohibited paths,
the crawler's own identity, server Retry-After instructions, global ownership
fences, article recovery and stored originals are unchanged.

`adaptive_rate.py` keeps each experiment's settings, request budget, response
counts, latency and rate adjustments in the existing Firestore host document.
Admission and scheduler readiness use the same adaptive limits across workers.
Response feedback is fenced to the active host lease and deduplicated. Restarting
an instance cannot reset a pilot's request count. Once its request budget is used,
new starts automatically return to the recorded robots interval. Three consecutive
429 responses abort a bounded experiment sooner. Article denials alone do not
change serving capacity in the final r17 controller.

The first pilot is only `www.kenyastar.com`, 100 HTTP/document starts, initially
five seconds apart and at most two concurrent responses. A twenty-response
window with predominantly fast serving responses permits a gradual rate
increase; slow/error windows
reduce it. Its minimum is three seconds, matching the existing queue refill
cadence. Timeouts and server errors reduce the rate; 429/503 responses retain
shared Retry-After cooldowns. A pilot may recover fewer than 100 articles because
robots requests, redirects and retries also consume its bounded request budget.

Deploy opt-in settings through `--adaptive-policy path/to/host-policies.json`
with `--shared-queue`; the deployer validates and base64-encodes the JSON into
`CRAWL_ADAPTIVE_HOSTS_B64`. Omitting this flag disables experiments in a full
redeployment. For the same pilot ID, changed settings are rejected instead of
silently restarting the experiment. Keep one task, 48 article slots and the
existing resource allocation. All 369 regression tests, including Chrome
fixtures, pass. Cloud build `efe0ffc6-3801-4bf8-a66f-b64cd4ed77e9` packages the
new module and runs the container import smoke check. Run evidence is stored
under `distributed/adaptive-r15/`; throughput must be measured before enabling
an unbounded or wider experiment.

The first 16-request experiment produced no transport errors, but its initial
controller overreacted to isolated 403s. The preceding half-hour already had
10 publisher 403s, 28 missing responses and 15 transport failures across 35
finished articles, so a single denied historical URL did not establish overload.
That experiment was deliberately ended and its normal pacing restored; eight
new texts and their original objects were verified (this includes archive
recovery, not eight successful publisher responses).

The refined r16 controller keeps isolated 401/403 responses local to the article.
Two consecutive denials slow and pause the host; three abort the experiment.
429, 503, timeouts and other overload signals still reduce the host rate. Fast
404/410 responses inform serving capacity but remain recorded as missing pages,
never as extracted texts. All 370 tests pass. The refined pilot has a fresh,
100-request ID with the same one-worker allocation; evidence uses
`distributed/adaptive-r16/`.

The r16 trial was ended after 80 requests: 11 returned pages, 51 were missing,
18 were denied and none had transport failures. Its denied historical URLs
included old `/index.php/sid/` paths, while other routes still responded normally.
Treating repeated denials as host overload made that controller progressively
slower, so these numbers do not establish a sustained speed improvement.

The final r17 controller uses rate limits (429), server errors, transport errors
and response latency as host-capacity signals. A 401/403 remains a denied article
and follows the existing archive recovery path; it cannot accelerate the rate,
but does not by itself slow unrelated URLs. Explicit Retry-After on a denial is
still honored globally. Healthy windows exclude neutral denials from their
capacity denominator, require at least ten informative responses, and never
count a missing page as a saved article. All 374 tests, including Chrome fixtures,
pass. A fresh 100-request r17 pilot precedes continued adaptive collection.

For continued operation, an explicitly opted-in policy can set `requests: null`.
It keeps rate feedback and the same host/ownership bounds across all remaining
articles, without returning to fixed robots pacing after an arbitrary request
count. History, windows and outstanding feedback remain bounded in memory and
Firestore. It retains the existing run's attempt/byte budgets and one-instance
resource allocation; it does not cap the corpus or add URLs. The bounded trial
used a five-second initial delay and three-second minimum; the continued
configuration below is more conservative. The recorded robots delay remains
available for disabling the opt-in policy.

Historical r17 policy (superseded by r18 below):

```json
{
  "www.kenyastar.com": {
    "pilot_id": "ke-20261009-r17-continuous-5s",
    "requests": null,
    "initial_delay": 10,
    "min_delay": 5,
    "max_delay": 120,
    "max_concurrency": 1,
    "window": 20,
    "healthy_seconds": 5
  }
}
```

A new policy ID deliberately starts a new feedback history; ordinary worker
restarts retain the existing history and rate. Deployment preserves one task
and the durable shared queue. Existing executions must be retired with the
atomic handover guard before applying a new policy to the running collection.

The r17 trial completed 100 admitted starts in 13 minutes 42 seconds, with 99
recorded responses and one start whose response was not observed. The recorded
responses were 9 HTTP 200, 64 HTTP 404, 24 HTTP 403, one redirect and one HTTP 429;
there were no recorded transport failures or server errors. Pacing changed from
5 to 3.5 to 3 seconds, then the rate limit triggered a 60-second shared cooldown,
a 10-second interval and capacity of one. The finite trial's fallback to the
recorded 20-second robots interval was verified.

At verification, the queue had finished 96 more URLs and saved 24 more texts,
including archive recoveries. A stored article's prose and original response
were checked. These counts include work completed around the trial and are not
100 successful publisher downloads. The observed 7.3 publisher starts per minute
is a short trial result, not a promised completion rate or proportional scaling
claim. Evidence is under `distributed/adaptive-r17/` in the existing private run.

The first continued collection used a more conservative policy: initially 10 seconds,
a five-second minimum and one simultaneous publisher request. Archive recovery
keeps its separate existing slots. This avoids retesting the three-second floor
that produced the rate limit. Healthy windows could gradually reduce the delay;
rate limits, slow responses and failures increased it. It kept the same
image and single-worker CPU/memory allocation, with no finite adaptive request
cap and no reset of the article queue.

The continued execution `softpower-crawler-ft2zt` was verified after 21
responses: no 429s, server errors, slow responses, transport failures or native
task retries. A healthy first window reduced the interval from 10 to 7 seconds.
At that check, eight more URLs were finished and one new text was saved; its
original and extracted objects exist. The observed 4.6 starts per minute
includes the conservative startup window and does not establish a sustained
full-text recovery rate. The public status API returned current running progress
with one active instance. Reservation, launch and verification evidence is under
`distributed/adaptive-continuous-20261009/`. Runtime code is commit `0595072`;
image digest is `sha256:d351988cef7dcfaf425c0294af1638a22699473378a62ebff9cb0b6367bb3d5b`.

### Faster dispatch and bounded recovery (r18, 2026-10-09)

The r17 continuous controller eventually reached a 120-second request interval
despite only 31 transport failures and seven HTTP 429 responses among 2,917
recorded responses. Recovering through twenty-response windows at that interval
took about forty minutes per window. The r18 changes remove that lasting delay
and avoid serial database work before article threads can start. The collection
retains one Cloud Run task, 48 article slots and the existing CPU/memory
allocation, full input queue, ownership fences and archive/browser recovery.

`SharedQueue.claim` checks run control and up to 48 live article versions in one
transactional bulk read, then writes the accepted leases atomically. It retains
fresh claim tokens, due-date and phase checks, per-outlet capacity and bounded
pagination. A peer claim or operator stop invalidates the transaction's live
read conditions; it cannot produce duplicate current ownership. In a local
30 ms database-call latency model, an eight-article refill fell from 0.879 to
0.138 seconds. This measures queue overhead, not publisher or full-text recovery
throughput. The BigQuery outbox exporter also runs separately from dispatch and
heartbeats, while HTTP threads reuse their own connections.

For continuous adaptive policies (`requests: null`), HTTP 429 creates a shared
pause of at least 60 seconds. Repeated timeouts or an unhealthy server-response
window create a pause of at least 30 seconds. Once the pause expires, the host
allows one probe at the configured initial interval, which must be at most ten
seconds. Three fast HTTP 200/404/410 responses restore the last healthy interval
and normal host capacity. Missing pages remain missing extraction outcomes;
they demonstrate serving capacity without counting as saved article text.

A failed recovery probe extends the next default pause, up to 300 seconds.
An explicit longer `Retry-After` remains in force; the 300-second bound never
shortens it. Responses from requests already in flight during a pause do not
pass the recovery probe. HTTP 401/403 responses and positively identified
TLS/DNS failures remain recorded but neither pass nor reset healthy probes.
Other transport failures retain the cautious pause/probe behavior. Finite
experiments keep their existing request budgets, backoffs and fallback rules.
Response feedback remains fenced and deduplicated across restarts.

The intended one-host production settings are `initial_delay: 2`,
`min_delay: 2`, `max_delay: 20`, `max_concurrency: 2`, `window: 20` and
`healthy_seconds: 5`, with `requests: null` and a fresh policy ID. Other hosts
retain their existing limits. Apply the policy through the normal atomic
handover; do not overlap the retired and replacement executions or reseed the
queue. The controller's default cooldown bound is separate from `max_delay`,
which bounds request spacing.

Regression checks cover shared cooldowns, a 600-second `Retry-After`, expired
owner fencing, single-slot probes, repeated failed probes, neutral denials,
transport diagnostics and the unchanged finite-pilot behavior. A synthetic
trace matching all 2,917 observed HTTP/transport outcome counts finishes without
ratcheting request spacing above two seconds. Its event order is synthetic;
it is not an estimate of the remaining corpus's completion time.

The stopped handover moved 1,257 strictly recognized old publisher retries to
archive recovery. Their prior publisher responses were definitive 401/403/404/410
outcomes followed by archive outages; all original evidence and cumulative
metrics remain. `scripts/crawl_queue_migration.py` accepts at most 48 prepared
immutable replacements per transaction and requires the exact owned STOP
reason, independently verified terminated execution and guard reservation.
Changed checkpoints or foreign owners are skipped. It preserves future retry
dates, removes retired claim tokens and increments only the archive phase
counter; processed, saved, total and budget counters do not change.

Archive outages now retain their phase checkpoint, availability payloads and
original lookup artifacts. They neither consume an article retry pass nor
restart completed publisher/browser work. Failed robots retrievals have a
short negative cache and shared cooldown, without granting access permission.
Archive work can use its full eight configured article slots even when all
remaining URLs belong to one original publisher.

### Rate-limited URLs yield their slots (r20, 2026-10-09)

A publisher HTTP 429 now returns after its first preserved response instead
of sleeping and retrying that URL inside its article thread. The shared host
pause still applies, and the existing durable recovery/retry phases retain the
unfinished work. This allows other URLs to test host recovery after the pause.
HTTP 500/503 and transport retries retain their existing behavior. A temporary
publisher robots-policy failure is also unfinished, including failures on
discovered URLs; permanent robots disallow stays distinct and is never bypassed.
The final combined suite passes 430 tests, including real TCP connection reuse
and the configured Chromium fallback.

The first r18 live check did **not** establish a full-text speed improvement:
saved texts remained at 60,893. Independent cloud probes reproduced TCP
timeouts/refusals to `web.archive.org:443` before TLS or an HTTP response.
Archive.org metadata and publisher requests remained reachable. Both HTTP and
HTTPS publisher robots succeeded at other times; HTTPS robots and an HTTPS
article also returned 429. Thus scheme changes, parser changes and more CPU
alone do not resolve the observed connectivity and rate-limit failures.
These are diagnostic observations, not proof of a permanent provider block.

The deployed final image is
`sha256:4e3989b3e5e4891e501137e106aa455bfd45facef23021a4358bc3f6e0b0bdf0`
from Cloud Build `46351479-3459-4103-a75d-a61c8aeb80fc`. The uploaded source bytes
were checked against the tested working files. It keeps one task, 48 article
slots, the existing four-CPU/four-GiB allocation and the full 85,993-URL input.
Private handover, deployment and connectivity evidence is under
`distributed/speed-r18/` and `distributed/speed-r20/` in the private run prefix.

The final execution `softpower-crawler-tszcw` then showed real recovery:
between 09:01:13 and 09:09:31 UTC on 2026-10-09, saved texts increased from
60,893 to 61,216 (323 articles in 497.8 seconds, about 2,336 saved texts/hour).
Finished URLs increased by 382; this separate figure includes partial and
unsuccessful outcomes. The input remains 85,993 URLs. One worker remained on
native attempt zero, without an error; the public status endpoint returned 200.

Independent reads verified two newly saved results from this execution:
1,657 and 521 words of sustained article prose, matching their preserved raw
HTML and payload hashes. They were new extractions, not reused text objects.
Evidence is in `distributed/speed-r20/verification.json`, `followup.json` and
`fulltext-proof.json`. The 430-test suite passed before deployment.

This is a short observed archive-recovery window, not a completion-time
forecast. Archive connectivity recovered during the final execution; the
deployment does not establish what caused the earlier TCP refusals to clear.
All remaining work belongs to KenyaStar, and publisher rate limits can still
constrain the tail after the ready archive backlog drains. The worker retains
shared host pauses, retry checkpoints, durable results and its existing cost
limits rather than treating these temporary failures as completed work.

### Archive first during publisher cooldowns (r21, 2026-10-09)

An explicit shared publisher cooldown now makes unfinished publisher URLs
eligible for a metadata-only handoff into the existing archive queue. These
handoffs and actual archive downloads share the eight-slot archive limit.
Existing archive work has priority, publisher spacing or occupied host permits
alone do not trigger this route, and actual HTTP still takes shared host leases.
The normal fenced article claim and immutable checkpoint paths remain in use.

Verified full text can finish directly from an archive. An archive miss or
partial text returns to publisher work, retaining the original unattempted
status, evidence and partial candidate. A completed preflight is not repeated.
An archive outage wakes no later than the observed publisher cooldown deadline
and then returns to publisher work; unfinished archive recovery remains
available after a genuine publisher failure. These handoffs neither finish the
article nor consume its publisher retry passes. A bounded local eligibility
cache avoids repeatedly reading checkpoints already returned to the publisher.

The combined suite passes 450 tests, including real TCP reuse, Chromium,
durable worker/queue handoffs, missed snapshots, partial candidates, archive
outages, checkpoint restarts and exact cumulative byte/retry accounting. This
change preserves one worker, 48 article slots and the existing response,
attempt and resource budgets.

Cloud Build `092dd2cf-6808-444d-adea-5d19310bb956` produced image
`sha256:2e4d6f7841baec204160968079729dfafeb7346f9d2ef4cca288455753584856`.
The deployed source hashes match the tested files. A fenced handover retired
`softpower-crawler-tszcw`; execution `softpower-crawler-s26sq` resumed the same
85,993-URL queue with 61,942 saved texts and zero remaining old archive jobs.

The first measured live window recovered 30 additional texts in 185.9 seconds.
Independent inspection of two new archive-first results verified 1,185 and 660
words, matching raw hashes and extracted paragraphs, with no original publisher
fetch. An actual two-probe archive miss remained ready for the publisher with
zero completed publisher retry passes. The bounded proof read 88 article rows
and eight objects. Evidence is under `distributed/speed-r21/verification.json`,
`routing-proof.json` and `followup.json` in the private run prefix.

At the 09:45 UTC follow-up, 49 additional texts had been saved (61,991 total),
with one worker on native attempt zero, no run error, and a successful public
status response. Archive transport errors were also observed; the short window
does not establish a sustained rate or completion deadline. Missing archives
continue as publisher work, preserving the distinction between recovered text
and an article that remains unresolved.

### Interrupted archive preflights resume independently (r22, 2026-10-10)

The zero-active investigation found publisher-ready checkpoints with
`archive_first_done: true` but `archive_first_complete: false`: temporary
archive failures had returned to publisher work, and the dispatcher cached
them as permanently ineligible for archive preflight. A publisher cooldown
could therefore leave the entire worker idle despite unfinished archive work.

Only an explicitly incomplete preflight with an unresolved publisher can now
resume during a later publisher cooldown. It retains cached lookups, snapshot
history, partial text, retry passes and cumulative counters. Returning from
another interrupted preflight records a five-minute archive backoff, and the
local exclusion cache expires at that time. Confirmed archive misses retain
their completed marker and remain publisher-only. A healthy publisher may
still run normally during archive backoff. Synthetic publisher-pending state
does not make a completed archive retry appear permanently unresolved.

All 454 tests pass, including interruption/restart/recovery without a publisher
fetch, exact byte accounting, completion after a cached missing snapshot and
expiry of temporary worker exclusions. The instance count and cost limits are
unchanged.

The r22 image is
`sha256:44571c9dd9e3a4ebcb75d83bc88ea34b3347a15bc36386b325ce966486087ca2`
from Cloud Build `1d4499c9-3e96-4130-9668-76cfef1b58f2`. The tested source hashes
were verified against the build input before a fenced handover. Execution
`softpower-crawler-7mswc` resumed with 63,295 saved texts. Its first measured
185.4-second window saved 47 more texts, reaching 63,342, with no HTTP transport
errors in that window. This is an observed short window, not a forecast.

Independent bounded inspection verified two newly recovered articles whose
evidence records archive outages from the previous day: 614 and 445 words,
matching raw HTML hashes and extracted paragraphs, with no original publisher
fetch. An incomplete pending checkpoint retained its publisher work. Proof is
under `distributed/speed-r22/routing-proof.json` and `verification.json` in the
private run prefix. Temporary zero-active snapshots remain possible between
batches and during source or archive backoff.

### KenyaStar rate experiment stopped (2026-10-10)

The continuous `ke-20261009-r18-continuous-2s` experiment repeatedly resumed
at two-second spacing after HTTP 429, rather than finding a sustainable rate.
Its shared Firestore adaptive phase was changed to `aborted` at
`2026-10-10T16:24:36.866044Z`, using the document update-time precondition.
The existing cooldown, leases, recorded robots interval, queue and saved
results were preserved. This invokes the existing non-experimental policy;
no worker image, instance count or extraction threshold changed.

After the previous cooldown expired, live requests used the recorded
20-second interval. Observed request starts were about 20–24 seconds apart,
and four URLs reached final results in the initial verification window.
No additional full text was saved in that window. This verifies corrected
pacing, not guaranteed access or a sustained recovery rate.

A separate Common Crawl pilot checked two exact remaining URLs in four
collections after their publication dates. Two indexes returned no capture
and two returned gateway timeouts. That pilot is inconclusive and has not
been integrated into extraction. The earlier two-instance comparison also
does not establish whether KenyaStar's HTTP 429 throttling is per IP: it
tested two old links returning identical 403/404 responses.
