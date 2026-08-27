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
- **`in_progress`** — job ID moves from queued to in-progress. No scaling call — the totals are unchanged, and a replica is already running for it.
- **`completed`** — job ID is removed from both sets. While other jobs are still in progress the count is held; once the batch fully drains, `setReplicas` either picks up the remaining queued jobs or resets to 1.

Two rules keep this safe, and both exist because of a real outage (see fork note 3):

- **The count is asserted, never assumed.** Every scale decision pushes a value, including when the backlog is over the cap and when the value is unchanged. That push is the only thing that can revive a fleet whose replicas died unobserved. Repeat pushes of an unchanged value are coalesced within a 30s window, and the background tick re-asserts every 5 minutes for as long as any job is outstanding — so recovery never depends on another webhook arriving.
- **The count never shrinks while *tracked* work is outstanding.** Railway may drop any replica when `numReplicas` decreases, including one mid-job, so while this process has jobs outstanding the count only holds or rises.

  Note the word *tracked*, because it is the whole caveat. **Jobs already running when the process starts are invisible to it forever** — `markInProgress` refuses to adopt an id it never queued, so a restart-era job is never counted and its completion never observed. Empty counters after a restart mean "I know nothing", not "the fleet is idle", and no sequence of webhooks can tell those apart: deciding on a drain the process *did* watch only defers the harm by one job cycle. So a second floor, seeded from Railway's live count at boot, holds for `STALE_JOB_TTL_MINUTES` — the same horizon past which this service already declares a job dead — and time, not tracking, is what releases it.

  The cost is real: a restart while the fleet is wide holds it wide for that horizon, even when the width was itself a leak. That is bounded over-provisioning, and it is the recoverable direction. Reconciling against GitHub's own view of in-progress jobs is what would remove the horizon.

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

This is zipzoft's fork of `shaezzy/railway-github-runner-autoscaler`, carrying three
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

**Image:** published to GHCR by `.github/workflows/docker-publish.yml` on every
push to `main` and on `v*` tags — `ghcr.io/zipzoft/railway-github-runner-autoscaler`.
Point the Railway service's build at this image instead of the `alpine:3`
placeholder to remove the "any var-change redeploy reverts to alpine" footgun.
