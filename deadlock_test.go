package main

import (
	"context"
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
func seedStuckFleet(srv *Server, stuck int) {
	srv.state.mu.Lock()
	defer srv.state.mu.Unlock()
	for i := 1; i <= stuck; i++ {
		srv.state.inProgress[int64(9000+i)] = srv.clock()
	}
}

func TestScaleUp_BacklogOverMaxStillAssertsCap(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	seedStuckFleet(srv, 4)
	ctx := context.Background()

	// 30 queued jobs arrive behind the phantoms, taking the total far past the cap.
	for id := int64(1); id <= 30; id++ {
		if err := srv.scaleUp(ctx, id); err != nil {
			t.Fatalf("scaleUp(%d): %v", id, err)
		}
	}

	// The deadlock is about what happens NEXT: with the backlog over the cap,
	// every further queued webhook must still (re)assert the replica count, so a
	// fleet that has silently lost its runners gets them back. Pre-fix, scaleUp
	// logged "at max runners" and returned without ever calling SetReplicas
	// again — no runner could start, so no job could complete, so no terminal
	// webhook could arrive to unwind the count. Closed loop.
	before := len(client.allCalls())
	if err := srv.scaleUp(ctx, 31); err != nil {
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
// applyAndClear never runs. Any per-job bookkeeping that only applyAndClear
// clears therefore grows without bound — in production it reached 5972 entries
// and, because it was summed into the scale-up total, held total permanently
// ~1000x above MaxRunners.
func TestScaleDown_CompletedJobsNeitherAccumulateNorInflateDesiredCount(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	seedStuckFleet(srv, 1)
	ctx := context.Background()

	// 200 jobs run to completion while the phantom entry is never retired.
	for id := int64(1); id <= 200; id++ {
		if err := srv.scaleUp(ctx, id); err != nil {
			t.Fatalf("scaleUp(%d): %v", id, err)
		}
		srv.markInProgress(id)
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

// After a redeploy the in-memory counters are empty but the fleet's real
// replicas may be wedged or gone. Trusting an unobserved "base replica" means
// the first job after boot gets no runner at all — exactly the state ATT-482
// left behind. The count must be asserted, not assumed; SetReplicas is
// idempotent so re-asserting the current value is free.
func TestScaleUp_ColdStartAssertsReplicaCountRatherThanAssumingBaseReplica(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)

	if err := srv.scaleUp(context.Background(), 1); err != nil {
		t.Fatalf("scaleUp: %v", err)
	}

	last, ok := client.lastCall()
	if !ok {
		t.Fatalf("first job after boot must assert the replica count, but SetReplicas was never called")
	}
	if last != 1 {
		t.Fatalf("expected replicas asserted at 1 for a single job, got %d", last)
	}
}

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
