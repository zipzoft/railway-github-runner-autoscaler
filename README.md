# Autoscaled Self-Hosted GitHub Actions Runner on Railway

Fix high memory usage and runners not picking up new jobs — a lightweight autoscaler that manages ephemeral self-hosted GitHub Actions runners on Railway automatically.

[![Deploy on Railway](https://railway.com/button.svg)](https://railway.com/deploy/autoscaled-github-actions-runner?referralCode=xOFE9K&utm_medium=integration&utm_source=template&utm_campaign=generic)

```
GitHub Actions
     │
     │  POST /webhook (workflow_job.queued / .in_progress / .completed)
     ▼
┌─────────────────────┐
│  autoscaler service │  ← always on, 1 replica
│  (this repo)        │
└────────┬────────────┘
         │  Railway GraphQL API
         │  serviceInstanceUpdate(numReplicas)
         ▼
┌─────────────────────┐
│  runner service     │  ← myoung34/github-runner
│  (1–N replicas)     │    scales to N for concurrent jobs
└─────────────────────┘    resets to 1 when all jobs done
```

## Common Problems This Solves

### GitHub runners using too much memory on Railway

Non-ephemeral self-hosted runners stay alive between jobs, consuming significant memory on Railway even when idle. With ephemeral runners (`--ephemeral`), the runner process exits after each job — but that triggers the second problem.

### GitHub runners not picking up new jobs after completing one

Ephemeral runners deregister from GitHub when they exit. Without something to redeploy the runner container, the next queued job has nowhere to run and sits pending indefinitely. Previously this required a manual redeploy in the Railway dashboard every time.

This autoscaler listens for GitHub `workflow_job` webhook events and automatically scales up the runner when a new job arrives.

## How It Works

The autoscaler tracks each **unfinished** job by its GitHub job ID. A completed job is deleted outright — it needs no runner, so it is not counted and not remembered.

The replica count it wants is always the same expression:

```
desired = clamp(queued + inProgress, 1, MAX_RUNNERS)
```

- **`queued`** — job ID is added to the queued set, then `setReplicas(desired)` is called. Jobs above `MAX_RUNNERS` do not get their own replica, but the count is still **asserted at the cap** rather than left unmanaged.
- **`in_progress`** — job ID moves from queued to in-progress. No scaling call — the totals are unchanged, and a replica is already running for it. If the id was never queued here, it is **adopted** when `GITHUB_TOKEN` is set (see *Reconciling against GitHub* below) and ignored when it is not.
- **`completed`** — job ID is removed from both sets. While other jobs are still in progress the count is held; once the batch fully drains, `setReplicas` either picks up the remaining queued jobs or resets to 1.

Two rules keep this safe, and both exist because of a real outage (see fork note 3):

- **The count is asserted, never assumed.** Every scale decision pushes a value, including when the backlog is over the cap and when the value is unchanged. That push is the only thing that can revive a fleet whose replicas died unobserved. Repeat pushes of an unchanged value are coalesced within a 30s window, and the background tick re-asserts every 5 minutes for as long as any job is outstanding — so recovery never depends on another webhook arriving.
- **The count never shrinks while *tracked* work is outstanding.** Railway may drop any replica when `numReplicas` decreases, including one mid-job, so while this process has jobs outstanding the count only holds or rises.

  Note the word *tracked*, because it is the caveat — and boot-era jobs are the largest class of untracked work, not the only one. **Jobs already running when the process starts are invisible to it forever** — a restart-era job is never counted and its completion never observed. (Without `GITHUB_TOKEN`, `markInProgress` also refuses to adopt any id it never queued, which widens the invisible class to every job whose `queued` delivery was lost. Reconcile is what makes adoption safe, and turns that refusal off.) Empty counters after a restart mean "I know nothing", not "the fleet is idle", and no sequence of webhooks can tell those apart: deciding on a drain the process *did* watch only defers the harm by one job cycle. So a second floor, seeded from Railway's live count at boot, holds for `STALE_JOB_TTL_MINUTES` — the same horizon past which this service already declares a job dead — and time, not tracking, is what releases it.

  Two things this does **not** do, both deliberate:

  - A job whose `queued` delivery never lands is invisible the same way, with no restart involved, and once the horizon has lapsed neither floor covers it. (The common failure keeps the job tracked — `scaleUp` records the id *before* pushing, so a Railway error still leaves it counted — but a delivery GitHub never lands is not recoverable this way.) **With `GITHUB_TOKEN` set and the repository proven readable, this specific case is covered**: the job's `in_progress` delivery is adopted, so it becomes tracked after all. What stays uncovered is a job that loses *both* deliveries, and a boot-era job that was already running before this process started and so has no further event to adopt. Both are pre-existing, but note that reconcile removes something that used to shield them by accident — a leak held the fleet wide for longer than GitHub's own default job timeout, so an invisible job was in practice carried by whatever leak happened to be outstanding. That accident is gone.
  - The floor bounds how far the *count* falls, not *which* replica Railway drops. A contraction from 6 down to a boot floor of 3 is still a contraction, and Railway is free to drop a busy one.

  The cost is real: a restart while the fleet is wide holds it wide for that horizon, even when the width was itself a leak — and if the boot read fails, "wide" means `MAX_RUNNERS`. That is bounded over-provisioning, and it is the recoverable direction. **Reconcile does not remove this horizon** (see below): it can only ask about ids it already tracks, and a boot-era job is one it never saw.

### Reconciling against GitHub

A counter fed only by webhooks leaks every time a delivery is lost. Asserting on every decision removed the *deadlock* that leak used to cause, but not the leak itself: an id whose `completed` delivery never arrived stays in the tracked set until the `STALE_JOB_TTL_MINUTES` reaper purges it — 7 hours by default — and the fleet is pinned above its idle baseline for all of it. Not an outage any more, but a bill.

Set **`GITHUB_TOKEN`** and the background tick first asks GitHub whether each tracked job is really still unfinished, via `GET /repos/{owner}/{repo}/actions/jobs/{job_id}` (scope `actions:read`; the repo comes from the webhook payload). Leaks of that class then clear within one 5-minute cycle instead of seven hours. Leave it unset and the service behaves exactly as it did before — reconcile is inert and the TTL reaper is the only cleanup.

Two qualifications on "clears within one cycle", because they are the difference between the counter and the bill:

- Retiring the entries is not the same as shrinking the fleet. The contraction additionally needs the tracked set to reach zero **and** the boot-era floor to have lapsed. Deploying this service reseeds that floor from Railway's live count — so a deploy made *while* a leak is holding the fleet wide adopts that width for a full `STALE_JOB_TTL_MINUTES` before any contraction can happen. The `reconcile: retired ...` log says so explicitly when that is the case.
- **Adoption waits for proof, not for configuration.** A job is adopted only once GitHub has answered a lookup for *that repository* in this process — per repository, because a token can hold `actions:read` on one and not another, and GitHub reports the difference as a 404. Until then the old refusal stands, so a wrong or expired token cannot create phantoms nothing can retire.

Only an authoritative `200 + "completed"` is authority to forget an id. **A 404 is not**, and this is the one rule worth stating twice: GitHub answers 404 — not 403 — both for a job it has never heard of and for a private repository the token cannot read, and it answers 404 for a valid job id queried against the wrong repo. A token scoped to one repository would otherwise delete every live job belonging to every other repository in the org and shrink the fleet under them. Everything that is not a completion (404, 403, a rate limit, a 5xx, a transport failure) keeps the entry and leaves the TTL reaper as its backstop. Because that rule makes a mis-scoped token *silent*, every cycle logs a census (`checked/finished/active/notfound/error`), three consecutive all-404 cycles raise a `[WARN]` that then repeats roughly hourly for as long as the condition lasts, and the token is probed once at boot against `/rate_limit`.

Two bounds, protecting different things: at most 50 lookups per cycle (against GitHub's hourly budget, oldest entries first since those are nearest the TTL horizon), and at most 60 seconds of wall clock per cycle — because `reapStaleJobs` and `assertDesired` run after reconcile in the same tick, and a degraded GitHub must never be able to starve them. The same provider that stops answering lookups is the one that stops delivering webhooks, so that is exactly when they matter most.

**Alternatives considered.** *Enumerating* GitHub's in-progress runs and diffing was rejected: the tracked set is keyed by job id, not run id, and an org-wide webhook makes enumeration O(repos x runs x jobs) per cycle against O(tracked ids) for per-id lookup. Subscribing the webhook to **`workflow_run.completed`** and pruning every tracked id sharing that `run_id` is a real alternative that needs no credential at all — it was rejected because it covers a strictly narrower failure set (both deliveries travel the same channel, so an outage loses both, and it cannot clear a leak that already exists), and because changing an org webhook's event subscription is no less gated than setting a token. It composes with this rather than replacing it, if the residual leak rate ever justifies it. *Doing nothing* — accepting the 7-hour horizon — remains the correct choice for a deployment with no token to give.

> **Note:** This approach is best suited for projects with infrequent or bursty CI workloads. One replica stays running at all times (consuming minimal memory while idle), scaling up for concurrent jobs and resetting back to 1 when all jobs are done. If your runners are consistently running many concurrent jobs, consider adjusting `MAX_RUNNERS` accordingly.

## Prerequisites

- A Railway account with a project
- A GitHub repo or org where you control webhook settings
- A Railway API token (generate at [railway.app/account/tokens](https://railway.app/account/tokens))

## Deploy to Railway

The template creates two services:

**github-autoscaler** (this repo) — set these variables:
| Variable | Description |
|---|---|
| `GITHUB_WEBHOOK_SECRET` | Auto-generated by the template — copy this value when configuring the GitHub webhook |
| `RAILWAY_API_TOKEN` | Generate at [railway.app/account/tokens](https://railway.app/account/tokens) (this fork sends it as `Project-Access-Token` — use a **project** token) |
| `RAILWAY_RUNNER_SERVICE_ID` | Auto-filled in template from the **github-runner** service |
| `MAX_RUNNERS` | Optional, default `3` |
| `STALE_JOB_TTL_MINUTES` | Optional, default `420` (7h) — safety-net TTL for jobs whose terminal webhook was never received. Also the horizon for the boot-era replica floor. Keep it above your longest `timeout-minutes`; GitHub's own default is 360 |
| `GITHUB_TOKEN` | **Optional.** A token with `actions:read` on the repositories your jobs run in — a fine-grained PAT is enough, no `admin:org`. Enables reconcile: tracked jobs are checked against GitHub every 5 minutes, so a lost terminal webhook clears within one cycle instead of on the `STALE_JOB_TTL_MINUTES` horizon. Unset, reconcile is inert and the service behaves exactly as it did before it existed |

**github-runner** (`myoung34/github-runner:latest`)
| Variable | Description |
|---|---|
| `ACCESS_TOKEN` | GitHub PAT — for organizations: **Self-hosted runners** permission, Read and write. For repositories: **Administration** permission, Read and write. |
| `RUNNER_SCOPE` | `repo` or `org` |
| `ORG_NAME` | GitHub org name (if `RUNNER_SCOPE=org`) |
| `REPO_URL` | Full repo URL e.g. `https://github.com/you/repo` (if `RUNNER_SCOPE=repo`) |
| `LABELS` | `self-hosted,railway` |

Set the **github-runner** service to start with **1 replica** in Railway.

### Configure the GitHub webhook

1. In Railway: open the **github-autoscaler** service → **Variables** → copy the value of `GITHUB_WEBHOOK_SECRET`
2. In your GitHub repo or org: **Settings → Webhooks → Add webhook**
3. **Payload URL**: `https://<your-autoscaler-railway-domain>/webhook`
4. **Content type**: `application/json`
5. **Secret**: paste the `GITHUB_WEBHOOK_SECRET` value copied from Railway
6. **Events**: select *"Let me select individual events"* → check **Workflow jobs**
7. Save

### Use the runner in your workflows

```yaml
jobs:
  build:
    runs-on: [self-hosted, railway]
```

## zipzoft fork notes

This is zipzoft's fork of `shaezzy/railway-github-runner-autoscaler`, carrying four
patches on top of upstream:

1. **Project-token auth.** `github-autoscaler` runs with a Railway **project**
   token, not an account/workspace token, so the Railway API call authenticates
   with the `Project-Access-Token` header instead of `Authorization: Bearer`.
   See `railwayClient.gqlDo` in `server.go`.
2. **Queued-job leak fix.** A job cancelled while still queued (e.g. superseded
   by a new push under `concurrency.cancel-in-progress`) fires `completed`
   without ever firing `in_progress`. `scaleDown` now retires the job id from
   *both* `queued` and `inProgress` on every `completed` event, so the queued
   count always returns to zero when nothing is running. A periodic
   `reapStaleJobs` sweep (`STALE_JOB_TTL_MINUTES`, default 420 = 7h) is a
   defense-in-depth safety net for the separate, much rarer case of a
   completely lost webhook delivery. See `server_test.go` for the regression
   coverage, in particular `TestScaleDown_RepeatedCancelWhileQueued_NeverLeaksAcrossManyBatches`.
3. **Assert, never assume (ATT-482).** On 2026-08-27 the fleet deadlocked for
   2.5h: four `completed` webhooks were lost, `scaleDown` therefore never
   cleared the finished-job set, that set was summed into the total compared
   against `MAX_RUNNERS`, and once the total passed the cap `scaleUp` returned
   **without calling `setReplicas` at all**. No runner started, so no job
   completed, so no webhook arrived to unwind the count. The finished-job set is
   gone, the count is asserted on every decision (coalesced, plus a periodic
   re-assert), and two replica floors — one for tracked work, one covering the
   boot-era jobs this process can never see — stop a scale decision from
   shrinking a fleet that is mid-job. See `deadlock_test.go` and `seed_test.go`.

4. **Reconcile against GitHub (ATT-487).** ATT-482 removed the deadlock but not
   the leak that caused it: an id whose `completed` delivery was lost still held
   a replica until the 7h TTL reaper. With `GITHUB_TOKEN` set, the background
   tick asks GitHub whether each tracked job is really unfinished and retires the
   ones it reports completed, so that class of leak clears in one cycle. Only a
   `200 + "completed"` retires an id — a 404 does not, because GitHub returns 404
   for a repository the token cannot read. `markInProgress` also adopts a job it
   never saw queued, once GitHub has proved that repository readable. Unset the
   token and all of this is inert. See *Reconciling against GitHub* above,
   `github.go`, and `reconcile_test.go`.

**Image:** built by `.github/workflows/docker-publish.yml` on every event
(including pull requests, so a Dockerfile that stops building fails a check
rather than a deploy) and published to GHCR on every push to `main` and on `v*` tags — `ghcr.io/zipzoft/railway-github-runner-autoscaler`.
Point the Railway service's build at this image instead of the `alpine:3`
placeholder to remove the "any var-change redeploy reverts to alpine" footgun.
