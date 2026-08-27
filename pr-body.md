## The outage

CI for `zipzoft/products` was dead from **09:42 to 12:08+ UTC on 2026-08-27** — 30 workflow runs queued, zero in progress. The autoscaler logged the same line every time a job arrived, for two and a half hours:

```
at max runners (6), job 98511040288 queued and waiting (queued=30 inProgress=4 completed=5972)
```

`completed=5972` is the tell. `scaleUp` summed that set into the total it compared against `MaxRunners`, so the total sat ~1000x over the cap and the cap branch was taken every time — and that branch returned **without calling `SetReplicas` at all**. No runner started, so no job completed, so no terminal webhook arrived to unwind the counters. A closed loop with no exit.

The defect is in the **currently deployed** commit `c27cba2`, verified directly: `server.go:156` computes the total including `completed`, and `:168` is the log line above, matching production verbatim.

## What was wrong, and what each fix answers

**1. The service stopped managing the fleet once over the cap.** Desired replicas is now `clamp(queued + inProgress, 1, MaxRunners)`, pushed on every scale decision — including over the cap. Capping means the fleet is *held at* the cap, not that scaling stops.

**2. The finished-job set grew without bound.** It was only cleared by an end-of-batch path that `scaleDown` skips while anything is in progress; four phantom `inProgress` entries held that branch open and the set reached 5972. It is deleted outright — a completed job needs no runner, and nothing else read it.

**3. Nothing re-asserted without a webhook.** Webhooks fire on job state *changes*, and a queued job no runner ever picks up has no further state to change. `assertDesired` runs on the background tick and re-pushes for as long as work is outstanding, so the no-deadlock property holds even on a quiet repo instead of depending on CI staying busy. It also retries a **contraction** that failed, which nothing else would — the batch is over, so no webhook is coming.

**4. Shrinking the fleet can cancel running jobs.** Railway may drop any replica when `numReplicas` decreases, including one mid-job. `State.applied` is an explicit floor: while work is outstanding the count never goes down. This replaces the accidental protection the unbounded set was providing.

**5. A restart knows nothing about the fleet — and that is not the same as the fleet being idle.** Two distinct holes here, both found in review:
   - The floor started at zero, so the first `queued` webhook after any restart computed `SetReplicas(1)` and pushed it at a fleet it had never looked at. `RailwayClient.Replicas` now reads the live count at boot and seeds the floor from it (the same project-scoped token that authorises `SetReplicas` can read it — no new credential). A failed read falls back to the cap, which over-provisions for one batch rather than shrinking under a running job.
   - Empty counters release the floor — but they read empty right after boot too. The first `completed` webhook after a restart is routinely for a job this process never tracked: `scaleDown` deletes an id from neither map, both counts read zero, and treating that as a drain pushed `SetReplicas(1)` at a live fleet. `observedWork` separates "I watched the queue drain" from "I just booted"; the floor only releases on a drain this process actually witnessed.

**6. Boot must not block on the network.** The floor read runs concurrently with serving, holding `scaleMu` so it is ordered against scale decisions rather than raced. Reading it before binding would delay the port past the deploy healthcheck, and a webhook refused in that window is not safely recoverable — GitHub does not guarantee automatic redelivery of a connection it could not establish, and a job never recorded is one `assertDesired` can never assert for.

**7. Call amplification.** Asserting on every webhook turned a 30-job burst into 30 serialized Railway mutations, and the tail of that burst can outlive GitHub's delivery timeout. A repeat push of an *unchanged* count within 30s is coalesced; the periodic re-assert is what makes that safe.

**8. The stale-job TTL had zero margin.** The default was 360 minutes — exactly GitHub's own default job timeout — so a job running to that default would be reaped while alive and the reaper would then scale down under it. Now 420.

## Tests

`deadlock_test.go` and `seed_test.go` reproduce the production state and each hole above. Every guarantee is mutation-checked — the block is removed and the suite must go red:

| mutation | test that dies |
|---|---|
| boot-ignorance guard removed | `TestApply_EmptyCountsAfterBootAreNotAnObservedDrain` |
| whole floor block removed | 5 tests, incl. `TestApply_NeverShrinksTheFleetWhileAJobIsStillRunning` |
| coalescing removed | `TestApply_CoalescesUnchangedPushesWithinTheWindow` |
| idle contraction-retry removed | `TestAssertDesired_RetriesAContractionThatFailed` |
| `seedFloor` fallback returns 1, not the cap | `TestSeedFloor_ReadFailureFallsBackToTheCapNotToOne` (+1) |
| boot seed overwrites a floor real work set | `TestSeedFloorOnce_DoesNotOverwriteAFloorAScaleDecisionAlreadySet` |
| `assertDesired` unwired from the tick | `TestReapLoop_TickRepushesOutstandingWorkWithoutAnyWebhook` |

The three original deadlock tests fail on pristine `b15297e` with only the new file added:

```
--- FAIL: TestScaleUp_BacklogOverMaxStillAssertsCap
    fleet deadlocked: a job queued behind a 30-deep backlog produced no SetReplicas call,
    so a fleet with 0 live runners can never recover
--- FAIL: TestScaleDown_CompletedJobsNeitherAccumulateNorInflateDesiredCount
    call 1 asked for 3 replicas; finished jobs are inflating the desired count
--- FAIL: TestScaleUp_ColdStart...
    first job after boot must assert the replica count, but SetReplicas was never called
```

`go vet` clean · `gofmt` clean · `go test ./...` ok · `go test -race -count=15 ./...` ok.

Existing tests that asserted the old assumptions were rewritten deliberately: `TestScaleUp_CapsAtMaxRunners` asserted "over cap → must NOT call again", which *is* the deadlock, and the first-job test asserted the base replica may be trusted, which this outage falsified.

## Unverified assumption, stated as one

`serviceInstanceUpdate` is treated as idempotent — a repeat push of the same `numReplicas` neither redeploys nor churns replicas. **This is not verified.** Probing it means mutating shared production infrastructure, which is human-gated here. The supporting evidence is historical rather than experimental: the previous code already re-pushed the same value between consecutive single-job batches, across 5972 completed jobs, without destroying the fleet. Coalescing bounds the exposure.

What genuinely changes is *when* those repeat pushes happen: from "between batches" to "every 5 minutes, including while jobs run". If an unchanged update does churn replicas, the blast radius moves inside a window where work is in flight. **The first deploy should watch one busy batch** — look for `periodic assert:` in the logs and confirm no runner container restarts against it.

## Residual risks, not closed

- **The reaper can still shrink under a job it wrongly reaped.** If a job's `in_progress` webhook is lost, the job is tracked only in `queued`; once the TTL purges it, both counts read zero and the fleet contracts under a runner that is still working. Pre-existing, and now bounded by an hour of TTL margin rather than zero, but real. Fixing it properly needs reconciliation against GitHub's own view.
- **The phantom `inProgress` leak itself remains**, cleared only by the TTL reaper. With this change that costs idle replicas for up to 7h instead of an outage.
- **Reconciling counters against GitHub's real `in_progress` set** would make every leak in this class self-correcting within one cycle. It needs a GitHub token on the service, which is a gated config change, so it is filed separately rather than smuggled in here.

Refs ATT-482.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
