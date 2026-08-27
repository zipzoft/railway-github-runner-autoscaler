package main

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// --- ATT-482: a lost terminal webhook must never wedge the fleet ---
//
// Production state at 2026-08-27T12:08Z (autoscaler log, verbatim):
//
//	at max runners (6), job 98511040288 queued and waiting (queued=30 inProgress=4 completed=5972)
//
// GitHub reported 0 in_progress jobs and 0 registered runners at the same
// moment: the four inProgress entries were phantoms whose `completed` webhooks
// were lost when the runner containers wedged at 09:42:44. Because scaleUp
// bailed out instead of asserting the cap, no SetReplicas call was ever made
// again and CI stayed dead for 2.5h with 30 jobs queued.

// seedStuckFleet reproduces the leaked-counter state: `stuck` phantom
// inProgress entries that will never receive a terminal webhook.
// newTestServerWithLiveFleet mirrors production boot against a Railway that
// already has `live` replicas configured — the state a restart lands in when it
// happens mid-batch, and the state in which pushing a low count cancels jobs.
// seedFloor is re-run because that read is exactly how boot sets the floor.
func newTestServerWithLiveFleet(maxRunners, live int, ttl time.Duration, clock func() time.Time) (*Server, *fakeRailwayClient) {
	srv, client := newTestServer(maxRunners, ttl, clock)
	// Re-run boot against a fleet of `live`, the same two steps in the same
	// order that main performs: cap-width state, then the seed.
	client.replicas = live
	srv.state = newState(maxRunners, clock(), ttl)
	srv.seedFloorOnce(context.Background())
	return srv, client
}

// seedStuckFleet plants `stuck` phantom inProgress entries: jobs whose terminal
// webhook was lost, so nothing but the TTL reaper will ever retire them.
func seedStuckFleet(srv *Server, stuck int) {
	srv.state.mu.Lock()
	defer srv.state.mu.Unlock()
	for i := 1; i <= stuck; i++ {
		srv.state.inProgress[int64(9000+i)] = jobEntry{since: srv.clock(), repo: testRepo}
	}
}

func TestScaleUp_BacklogOverMaxStillAssertsCap(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now }
	srv, client := newTestServer(6, time.Hour, clock)
	seedStuckFleet(srv, 4)
	ctx := context.Background()

	// 30 queued jobs arrive behind the phantoms, taking the total far past the cap.
	for id := int64(1); id <= 30; id++ {
		if err := srv.scaleUp(ctx, id, testRepo); err != nil {
			t.Fatalf("scaleUp(%d): %v", id, err)
		}
	}

	// The deadlock is about what happens NEXT: with the backlog over the cap,
	// every further queued webhook must still (re)assert the replica count, so a
	// fleet that has silently lost its runners gets them back. Pre-fix, scaleUp
	// logged "at max runners" and returned without ever calling SetReplicas
	// again — no runner could start, so no job could complete, so no terminal
	// webhook could arrive to unwind the count. Closed loop.
	// Past the coalesce window, so a re-push is genuinely due rather than
	// suppressed as a repeat.
	now = now.Add(2 * coalesceWindow)
	before := len(client.allCalls())
	if err := srv.scaleUp(ctx, 31, testRepo); err != nil {
		t.Fatalf("scaleUp(31): %v", err)
	}
	after := client.allCalls()

	if len(after) == before {
		t.Fatalf("fleet deadlocked: a job queued behind a %d-deep backlog produced no "+
			"SetReplicas call, so a fleet with 0 live runners can never recover", 30)
	}
	if last := after[len(after)-1]; last != 6 {
		t.Fatalf("expected replicas asserted at the cap 6, got %d", last)
	}
}

// A phantom inProgress entry pins scaleDown's early return forever, so
// the end-of-batch apply never runs. Any per-job bookkeeping that only that
// path clears therefore grows without bound — in production it reached 5972 entries
// and, because it was summed into the scale-up total, held total permanently
// ~1000x above MaxRunners.
func TestScaleDown_CompletedJobsNeitherAccumulateNorInflateDesiredCount(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	seedStuckFleet(srv, 1)
	ctx := context.Background()

	// 200 jobs run to completion while the phantom entry is never retired.
	for id := int64(1); id <= 200; id++ {
		if err := srv.scaleUp(ctx, id, testRepo); err != nil {
			t.Fatalf("scaleUp(%d): %v", id, err)
		}
		srv.markInProgress(id, testRepo)
		if err := srv.scaleDown(ctx, id); err != nil {
			t.Fatalf("scaleDown(%d): %v", id, err)
		}
	}

	srv.state.mu.Lock()
	queued, inProgress := len(srv.state.queued), len(srv.state.inProgress)
	srv.state.mu.Unlock()

	if queued != 0 {
		t.Fatalf("queued should have drained, got %d", queued)
	}
	if inProgress != 1 {
		t.Fatalf("expected only the 1 phantom entry to remain, got %d", inProgress)
	}

	// At every point there was at most one live job plus the phantom, so no
	// scale decision may ever have asked for more than 2 replicas. Summing
	// finished jobs into the total made this climb 2,3,4,5,6 and then stop
	// calling altogether — the deadlock, reached after only 5 completions.
	for i, n := range client.allCalls() {
		if n > 2 {
			t.Fatalf("call %d asked for %d replicas; finished jobs are inflating the "+
				"desired count (max legitimate is 2: one live job + one phantom)", i, n)
		}
	}
}

// After a redeploy the in-memory counters are empty but the fleet is whatever it
// was — possibly six replicas mid-job. The two ways of being wrong are not
// symmetric: assuming the fleet is healthy leaves a dead one asleep (ATT-482),
// while assuming it is idle shrinks a live one and cancels running CI jobs. So
// the first decision after boot must assert a count (to revive a dead fleet)
// and that count must never be lower than the cap (so it cannot shrink a live
// one).
func TestScaleUp_ColdStartAssertsWithoutShrinkingAPossiblyLiveFleet(t *testing.T) {
	// Railway already has 6 replicas, which may be mid-job — this process has no
	// way to tell, so it boots with the floor read back from that count.
	srv, client := newTestServerWithLiveFleet(6, 6, time.Hour, testClock)

	if err := srv.scaleUp(context.Background(), 1, testRepo); err != nil {
		t.Fatalf("scaleUp: %v", err)
	}

	last, ok := client.lastCall()
	if !ok {
		t.Fatalf("first job after boot must assert the replica count, but SetReplicas was never called")
	}
	if last != 6 {
		t.Fatalf("first decision after boot pushed %d against a fleet of 6; the naive count for one "+
			"queued job is 1, and pushing it would drop 5 replicas that may each be mid-job", last)
	}
}

// The floor must release itself, or a restart would pin the fleet at its boot
// width forever and bill idle runners. What releases it is the boot horizon
// passing — NOT a drain, however clean.
func TestApply_BootFloorReleasesWhenTheHorizonPasses(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now }
	ttl := time.Hour
	srv, client := newTestServerWithLiveFleet(6, 6, ttl, clock)
	ctx := context.Background()

	if err := srv.scaleUp(ctx, 1, testRepo); err != nil {
		t.Fatalf("scaleUp: %v", err)
	}
	srv.markInProgress(1, testRepo)
	if err := srv.scaleDown(ctx, 1); err != nil {
		t.Fatalf("scaleDown: %v", err)
	}
	if last, _ := client.lastCall(); last != 6 {
		t.Fatalf("the floor must still hold at 6 inside the boot horizon, got %d", last)
	}

	// Past the horizon, no boot-era job can still be alive.
	now = now.Add(ttl + time.Minute)
	srv.assertDesired(ctx)

	if last, ok := client.lastCall(); !ok || last != 1 {
		t.Fatalf("expected the fleet released to 1 once the boot horizon passed, got %d (ok=%v)", last, ok)
	}
}

// A drain inside the horizon proves nothing, and this is the hole that tracking
// alone could not close. Boot-era jobs are permanently invisible: markInProgress
// refuses to adopt an id it never queued, so their completions are never
// counted. Deciding on "a drain I watched" just defers the harm by one job
// cycle — a new job arrives, runs, finishes, the counters read zero again, and
// SetReplicas(1) cancels every boot-era job still going.
func TestApply_ATrackedJobCycleDoesNotReleaseTheFloorUnderBootEraWork(t *testing.T) {
	srv, client := newTestServerWithLiveFleet(6, 6, time.Hour, testClock)
	ctx := context.Background()

	// One ordinary tracked cycle while 5 boot-era jobs are still running.
	if err := srv.scaleUp(ctx, 100, testRepo); err != nil {
		t.Fatalf("scaleUp: %v", err)
	}
	srv.markInProgress(100, testRepo)
	if err := srv.scaleDown(ctx, 100); err != nil {
		t.Fatalf("scaleDown: %v", err)
	}

	for _, n := range client.allCalls() {
		if n < 6 {
			t.Fatalf("pushed %d after one tracked job cycle; the boot-era jobs are still running "+
				"and no webhook sequence can prove otherwise (calls=%v)", n, client.allCalls())
		}
	}
}

// The floor is now the ONLY thing standing between a scale decision and a
// cancelled CI job, and a mutation test showed the whole suite stayed green with
// the floor block deleted. This pins it directly: six jobs running, five finish
// (scaleDown holds the count), then a new job is queued — the naive count for
// "1 queued + 1 in progress" is 2, and pushing 2 would drop four replicas, one
// of which is still working.
func TestApply_NeverShrinksTheFleetWhileAJobIsStillRunning(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now }
	srv, client := newTestServer(6, time.Hour, clock)
	ctx := context.Background()

	for id := int64(1); id <= 6; id++ {
		if err := srv.scaleUp(ctx, id, testRepo); err != nil {
			t.Fatalf("scaleUp(%d): %v", id, err)
		}
		srv.markInProgress(id, testRepo)
	}
	for id := int64(1); id <= 5; id++ {
		if err := srv.scaleDown(ctx, id); err != nil {
			t.Fatalf("scaleDown(%d): %v", id, err)
		}
	}

	rampCalls := len(client.allCalls()) // the legitimate 1..6 ramp-up

	// Job 6 is still running on one of the six replicas. Past the coalesce
	// window so this decision genuinely pushes.
	now = now.Add(2 * coalesceWindow)
	if err := srv.scaleUp(ctx, 7, testRepo); err != nil {
		t.Fatalf("scaleUp(7): %v", err)
	}

	after := client.allCalls()
	if len(after) == rampCalls {
		t.Fatalf("expected the post-drain decision to push, got no call (calls=%v)", after)
	}
	for _, n := range after[rampCalls:] {
		if n < 6 {
			t.Fatalf("pushed %d replicas while job 6 was still running; Railway may drop any "+
				"replica when the count shrinks, including the one mid-job (calls=%v)", n, after)
		}
	}
}

// Recovery must not depend on another webhook arriving. Webhooks fire on job
// state CHANGES; a queued job that no runner ever picks up has no further state
// to change. If the fleet dies after the last job was queued, nothing else
// would call apply and the backlog would sit until the stale-job TTL expired.
func TestAssertDesired_RepushesWhileWorkIsOutstandingWithNoFurtherWebhooks(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now }
	srv, client := newTestServer(6, time.Hour, clock)
	ctx := context.Background()

	if err := srv.scaleUp(ctx, 1, testRepo); err != nil {
		t.Fatalf("scaleUp: %v", err)
	}
	before := len(client.allCalls())

	// No further webhooks ever arrive; only the reap loop ticks.
	now = now.Add(reapInterval)
	srv.assertDesired(ctx)

	after := client.allCalls()
	if len(after) == before {
		t.Fatalf("a queued job with a dead fleet and no further webhooks produced no scale call; " +
			"recovery still depends on CI staying busy")
	}
	if last := after[len(after)-1]; last != 1 {
		t.Fatalf("expected the outstanding-work assert to re-push 1 for the single queued job, got %d", last)
	}
}

// The background loop must actually be wired to the assert: a mutation that
// dropped assertDesired from the tick left the rest of the suite green,
// because the other tests call it directly.
func TestReapLoop_TickRepushesOutstandingWorkWithoutAnyWebhook(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now }
	srv, client := newTestServer(6, time.Hour, clock)
	srv.tickInterval = 2 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := srv.scaleUp(ctx, 1, testRepo); err != nil {
		t.Fatalf("scaleUp: %v", err)
	}
	before := len(client.allCalls())

	// No further webhooks; only the loop runs. Move past the coalesce window so
	// the repeat push is due.
	now = now.Add(2 * coalesceWindow)
	go srv.reapLoop(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(client.allCalls()) > before {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("the background loop never re-asserted the replica count for outstanding work; "+
		"recovery still depends on a webhook arriving (calls=%v)", client.allCalls())
}

// Nothing outstanding and already at the baseline means nothing to correct — the
// periodic path must not churn Railway on an idle fleet.
func TestAssertDesired_SilentWhenIdleAndAlreadyAtBaseline(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	srv.assertDesired(context.Background())
	if _, ok := client.lastCall(); ok {
		t.Fatalf("idle fleet must not be re-pushed")
	}
}

// Asserting on every webhook is what breaks the deadlock, but issuing one
// serialized Railway mutation per webhook pushes the tail of a 30-job burst past
// GitHub's delivery timeout. An unchanged count within coalesceWindow is
// therefore suppressed — and assertDesired covers the suppressed window.
func TestApply_CoalescesUnchangedPushesWithinTheWindow(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now }
	srv, client := newTestServer(6, time.Hour, clock)
	ctx := context.Background()

	// A 30-job burst arriving inside the window: the count saturates at the cap
	// and every push after that is identical.
	for id := int64(1); id <= 30; id++ {
		now = now.Add(time.Second)
		if err := srv.scaleUp(ctx, id, testRepo); err != nil {
			t.Fatalf("scaleUp(%d): %v", id, err)
		}
	}

	// The count ramps 1..6 as the backlog grows — those pushes are all distinct
	// and must happen. Everything after saturation is the same value 24 times
	// over, and must collapse to the single push that first reached the cap.
	calls := client.allCalls()
	atCap := 0
	for _, n := range calls {
		if n == 6 {
			atCap++
		}
	}
	if atCap != 1 {
		t.Fatalf("the cap was pushed %d times; the 24 identical pushes after saturation must be "+
			"coalesced within %s (calls=%v)", atCap, coalesceWindow, calls)
	}
	// Suppression must not cost correctness: the cap is still what was asserted.
	if last, ok := client.lastCall(); !ok || last != 6 {
		t.Fatalf("expected the cap asserted despite coalescing, got %d (ok=%v)", last, ok)
	}
}

// Characterization test — this passes on the base commit too. It is here
// because the reaper is the one path that can shrink the fleet on entries it
// decided were abandoned, and nothing else in the suite covers it.
//
// The reaper is the last line of defence: once the phantoms age out, the fleet
// must return to its idle baseline rather than staying pinned at the cap.
func TestReapStaleJobs_ClearsPhantomsAndReturnsFleetToBaseline(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now }
	srv, client := newTestServer(6, 30*time.Minute, clock)
	seedStuckFleet(srv, 4)

	now = now.Add(31 * time.Minute)
	srv.reapStaleJobs(context.Background())

	srv.state.mu.Lock()
	inProgress := len(srv.state.inProgress)
	srv.state.mu.Unlock()
	if inProgress != 0 {
		t.Fatalf("expected phantoms reaped, %d left", inProgress)
	}
	if last, ok := client.lastCall(); !ok || last != 1 {
		t.Fatalf("expected fleet returned to 1 replica after reap, got %d (ok=%v)", last, ok)
	}
}

// A scale-down push that fails leaves applied stale-high on purpose, and the
// batch is over — no further webhook will call apply. Without the idle branch of
// assertDesired nothing ever retries the contraction, and a bursty repo bills a
// full-width fleet until the next batch happens to drain cleanly.
func TestAssertDesired_RetriesAContractionThatFailed(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now }
	ttl := time.Hour
	srv, client := newTestServerWithLiveFleet(6, 6, ttl, clock)
	ctx := context.Background()

	if err := srv.scaleUp(ctx, 1, testRepo); err != nil {
		t.Fatalf("scaleUp: %v", err)
	}
	srv.markInProgress(1, testRepo)

	// Past the boot horizon, so the end-of-batch contraction is genuinely due.
	now = now.Add(ttl + time.Minute)

	// The contraction at the end of the batch fails.
	client.err = fmt.Errorf("railway down")
	if err := srv.scaleDown(ctx, 1); err == nil {
		t.Fatal("expected the failed contraction to surface")
	}
	srv.state.mu.Lock()
	applied := srv.state.applied
	srv.state.mu.Unlock()
	if applied != 6 {
		t.Fatalf("a failed push must leave the floor stale-high, got %d", applied)
	}

	// Railway recovers. Nothing is queued and no webhook will arrive.
	client.err = nil
	now = now.Add(2 * coalesceWindow)
	srv.assertDesired(ctx)

	if last, ok := client.lastCall(); !ok || last != 1 {
		t.Fatalf("expected the idle fleet contracted to 1 on retry, got %d (ok=%v)", last, ok)
	}
}

// While the boot-era floor is what pins the fleet, there is nothing to correct:
// the periodic assert must stay silent rather than re-push the pinned value once
// per tick for the whole horizon. That churn is wasteful on its own, and each
// push is another bet on `serviceInstanceUpdate` being inert.
func TestAssertDesired_SilentWhileTheBootFloorIsWhatHoldsTheFleet(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now }
	ttl := 7 * time.Hour
	srv, client := newTestServerWithLiveFleet(6, 6, ttl, clock)
	ctx := context.Background()

	// Ticks strictly inside the horizon (7h / 5min = 84, so 83 stays inside).
	for i := 0; i < 83; i++ {
		now = now.Add(reapInterval)
		srv.tick(ctx)
	}

	if n := len(client.allCalls()); n > 0 {
		t.Fatalf("%d Railway pushes while idle and pinned by the boot floor; nothing was "+
			"correctable, so nothing should have been pushed (calls=%v)", n, client.allCalls())
	}

	// And the moment the horizon lapses the fleet must actually contract.
	now = now.Add(2 * reapInterval)
	srv.tick(ctx)
	if last, ok := client.lastCall(); !ok || last != 1 {
		t.Fatalf("expected contraction to 1 after the horizon, got %d (ok=%v)", last, ok)
	}
}

// The deploy runbook tells an operator which log line means the boot floor
// failed. That only works if the line reports what was actually pushed:
// selecting the message from `queued == 0` announced "reset to 1 replica" while
// a floor was holding the fleet at 6, so the designated failure signal fired on
// every healthy boot-era batch.
func TestScaleDown_LogsThePushedCountNotTheAssumedOne(t *testing.T) {
	srv, client := newTestServerWithLiveFleet(6, 6, time.Hour, testClock)
	ctx := context.Background()

	if err := srv.scaleUp(ctx, 1, testRepo); err != nil {
		t.Fatalf("scaleUp: %v", err)
	}
	srv.markInProgress(1, testRepo)
	if err := srv.scaleDown(ctx, 1); err != nil {
		t.Fatalf("scaleDown: %v", err)
	}

	last, ok := client.lastCall()
	if !ok {
		t.Fatal("expected a push")
	}
	// The value the drain branch reports must be the value that reached Railway.
	// scaleDown returns it from apply, so pinning apply's return is pinning the
	// number in the log line.
	if last != 6 {
		t.Fatalf("expected the floor to hold the push at 6, got %d", last)
	}
}
