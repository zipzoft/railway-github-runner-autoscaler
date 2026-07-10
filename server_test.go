package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// fakeRailwayClient records SetReplicas calls instead of hitting the network,
// so scaling logic is testable without a live Railway project.
type fakeRailwayClient struct {
	mu            sync.Mutex
	calls         []int
	err           error
	delay         time.Duration // artificial in-call delay, to widen the overlap window
	respectCtx    bool          // honor ctx cancellation, as the real client does
	concurrent    int           // appliers currently inside SetReplicas
	maxConcurrent int           // high-water mark of concurrent appliers
}

func (f *fakeRailwayClient) SetReplicas(ctx context.Context, n int) error {
	if f.respectCtx {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	f.mu.Lock()
	f.concurrent++
	if f.concurrent > f.maxConcurrent {
		f.maxConcurrent = f.concurrent
	}
	delay, err := f.delay, f.err
	f.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	f.mu.Lock()
	f.concurrent--
	if err == nil {
		f.calls = append(f.calls, n)
	}
	f.mu.Unlock()
	return err
}

func (f *fakeRailwayClient) lastCall() (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return 0, false
	}
	return f.calls[len(f.calls)-1], true
}

func (f *fakeRailwayClient) allCalls() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int, len(f.calls))
	copy(out, f.calls)
	return out
}

func testClock() time.Time {
	return time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC)
}

func newTestServer(maxRunners int, ttl time.Duration, clock func() time.Time) (*Server, *fakeRailwayClient) {
	client := &fakeRailwayClient{}
	srv := &Server{
		cfg: Config{
			WebhookSecret: "test-secret",
			MaxRunners:    maxRunners,
			StaleJobTTL:   ttl,
			RunnerLabels:  []string{"self-hosted", "railway"},
		},
		state: &State{
			queued:     make(map[int64]time.Time),
			inProgress: make(map[int64]time.Time),
			completed:  make(map[int64]struct{}),
		},
		client: client,
		clock:  clock,
	}
	return srv, client
}

// postWebhook signs and delivers a workflow_job webhook through the real
// handler, so tests exercise the JSON decode + HMAC path, not just the
// internal state methods.
func postWebhook(srv *Server, action string, id int64) *httptest.ResponseRecorder {
	payload := fmt.Sprintf(`{"action":%q,"workflow_job":{"id":%d,"labels":["self-hosted","railway"]}}`, action, id)
	body := []byte(payload)
	mac := hmac.New(sha256.New, []byte(srv.cfg.WebhookSecret))
	mac.Write(body)
	sig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "workflow_job")
	req.Header.Set("X-Hub-Signature-256", sig)
	rec := httptest.NewRecorder()
	srv.handleWebhook(rec, req)
	return rec
}

// --- pure helper functions ---

func TestHasLabels(t *testing.T) {
	cases := []struct {
		name      string
		jobLabels []string
		required  []string
		want      bool
	}{
		{"exact match", []string{"self-hosted", "railway"}, []string{"self-hosted", "railway"}, true},
		{"case insensitive", []string{"Self-Hosted", "Railway"}, []string{"self-hosted", "railway"}, true},
		{"extra job labels ok", []string{"self-hosted", "railway", "linux"}, []string{"self-hosted", "railway"}, true},
		{"missing required label", []string{"self-hosted"}, []string{"self-hosted", "railway"}, false},
		{"no job labels", nil, []string{"self-hosted"}, false},
		{"no required labels", []string{"self-hosted"}, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasLabels(tc.jobLabels, tc.required); got != tc.want {
				t.Fatalf("hasLabels(%v, %v) = %v, want %v", tc.jobLabels, tc.required, got, tc.want)
			}
		})
	}
}

func TestValidateHMAC(t *testing.T) {
	secret := "test-secret"
	body := []byte(`{"action":"queued"}`)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	validSig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	if !validateHMAC(body, validSig, secret) {
		t.Fatalf("expected valid signature to pass")
	}
	if validateHMAC(body, validSig, "wrong-secret") {
		t.Fatalf("expected signature to fail with wrong secret")
	}
	if validateHMAC(body, "sha256=deadbeef", secret) {
		t.Fatalf("expected malformed signature to fail")
	}
	if validateHMAC(body, "", secret) {
		t.Fatalf("expected missing signature to fail")
	}
	if validateHMAC([]byte(`{"action":"tampered"}`), validSig, secret) {
		t.Fatalf("expected tampered body to fail signature check")
	}
}

// --- scaling state machine ---

func TestScaleUp_FirstJobUsesBaseReplicaWithoutAPICall(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	if err := srv.scaleUp(context.Background(), 1); err != nil {
		t.Fatalf("scaleUp: %v", err)
	}
	if _, ok := client.lastCall(); ok {
		t.Fatalf("first job should be handled by the base replica with no API call")
	}
}

func TestScaleUp_ConcurrentJobsScaleReplicas(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	ctx := context.Background()
	if err := srv.scaleUp(ctx, 1); err != nil {
		t.Fatalf("scaleUp(1): %v", err)
	}
	if err := srv.scaleUp(ctx, 2); err != nil {
		t.Fatalf("scaleUp(2): %v", err)
	}
	last, ok := client.lastCall()
	if !ok || last != 2 {
		t.Fatalf("expected replicas=2 for second concurrent job, got %v (ok=%v)", last, ok)
	}
}

func TestScaleUp_CapsAtMaxRunners(t *testing.T) {
	srv, client := newTestServer(2, time.Hour, testClock)
	ctx := context.Background()
	_ = srv.scaleUp(ctx, 1) // total=1, base replica, no call
	_ = srv.scaleUp(ctx, 2) // total=2 == max, SetReplicas(2)
	_ = srv.scaleUp(ctx, 3) // total=3 > max, must NOT call again

	calls := client.allCalls()
	if len(calls) != 1 || calls[0] != 2 {
		t.Fatalf("expected exactly one SetReplicas(2) call once at max runners, got %v", calls)
	}
}

func TestScaleDown_WaitsForRemainingInProgressJobs(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	ctx := context.Background()
	_ = srv.scaleUp(ctx, 1)
	srv.markInProgress(1)
	_ = srv.scaleUp(ctx, 2)
	srv.markInProgress(2)

	callsBefore := len(client.allCalls()) // scaleUp(2) already made one call
	if err := srv.scaleDown(ctx, 1); err != nil {
		t.Fatalf("scaleDown(1): %v", err) // job 2 still in progress
	}
	if got := len(client.allCalls()); got != callsBefore {
		t.Fatalf("must not scale down while another job is still in progress (calls %d -> %d)", callsBefore, got)
	}

	if err := srv.scaleDown(ctx, 2); err != nil { // now idle
		t.Fatalf("scaleDown(2): %v", err)
	}
	last, ok := client.lastCall()
	if !ok || last != 1 {
		t.Fatalf("expected reset to 1 replica once fully idle, got %v (ok=%v)", last, ok)
	}
}

func TestMarkInProgress_MovesJobFromQueuedToInProgress(t *testing.T) {
	srv, _ := newTestServer(6, time.Hour, testClock)
	_ = srv.scaleUp(context.Background(), 1)
	srv.markInProgress(1)

	srv.state.mu.Lock()
	defer srv.state.mu.Unlock()
	if _, stillQueued := srv.state.queued[1]; stillQueued {
		t.Fatalf("job should have moved out of queued")
	}
	if _, inProg := srv.state.inProgress[1]; !inProg {
		t.Fatalf("job should be in inProgress")
	}
}

// TestScaling_AppliersDoNotOverlap fires many scale operations concurrently and
// asserts the fake client never sees two appliers inside SetReplicas at once.
// The in-call delay widens the overlap window so that, without scaleMu
// serializing compute-and-apply, the calls would reliably overlap (maxConcurrent
// > 1) and fail this test — which is exactly the out-of-order race the mutex fixes.
func TestScaling_AppliersDoNotOverlap(t *testing.T) {
	srv, client := newTestServer(10, time.Hour, testClock)
	client.delay = 5 * time.Millisecond

	var wg sync.WaitGroup
	for id := int64(1); id <= 8; id++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			_ = srv.scaleUp(context.Background(), id)
		}(id)
	}
	wg.Wait()

	client.mu.Lock()
	maxc := client.maxConcurrent
	client.mu.Unlock()
	if maxc > 1 {
		t.Fatalf("replica appliers overlapped (max concurrent = %d); compute-and-apply is not serialized", maxc)
	}
}

func TestMarkInProgress_IgnoresJobNotQueued(t *testing.T) {
	srv, _ := newTestServer(6, time.Hour, testClock)
	srv.markInProgress(999) // never queued

	srv.state.mu.Lock()
	_, present := srv.state.inProgress[999]
	srv.state.mu.Unlock()
	if present {
		t.Fatal("in_progress for a job that was never queued must not be injected into inProgress")
	}
}

// TestMarkInProgress_LateWebhookAfterCompleteDoesNotReinject reproduces the
// out-of-order delivery the guard exists for: a retried in_progress arrives
// after the job already completed and the batch settled (which clears the
// completed set). The id must stay out of inProgress, not leak until the reaper.
func TestMarkInProgress_LateWebhookAfterCompleteDoesNotReinject(t *testing.T) {
	srv, _ := newTestServer(6, time.Hour, testClock)
	ctx := context.Background()

	if err := srv.scaleUp(ctx, 100); err != nil {
		t.Fatalf("scaleUp(100): %v", err)
	}
	srv.markInProgress(100)
	if err := srv.scaleDown(ctx, 100); err != nil { // completes; batch settles; state empty
		t.Fatalf("scaleDown(100): %v", err)
	}

	srv.markInProgress(100) // late/retried in_progress for the finished job

	srv.state.mu.Lock()
	_, present := srv.state.inProgress[100]
	n := len(srv.state.inProgress)
	srv.state.mu.Unlock()
	if present || n != 0 {
		t.Fatalf("late in_progress re-injected completed job 100 (inProgress size=%d)", n)
	}
}

// --- regression coverage for the queued-counter leak ---

// TestScaleDown_CancelledWhileQueued_DoesNotLeak reproduces the production
// incident directly: a job is cancelled (e.g. superseded by a new push under
// concurrency.cancel-in-progress) before it ever starts, so GitHub fires
// "completed" with no preceding "in_progress". Before the fix, scaleDown only
// ever deleted from inProgress, so this job's id stayed in `queued` forever.
func TestScaleDown_CancelledWhileQueued_DoesNotLeak(t *testing.T) {
	srv, client := newTestServer(6, 6*time.Hour, testClock)
	ctx := context.Background()

	// Job 1 starts running.
	if err := srv.scaleUp(ctx, 1); err != nil {
		t.Fatalf("scaleUp(1): %v", err)
	}
	srv.markInProgress(1)

	// Job 2 is queued behind it, then cancelled before it ever runs.
	if err := srv.scaleUp(ctx, 2); err != nil {
		t.Fatalf("scaleUp(2): %v", err)
	}
	if err := srv.scaleDown(ctx, 2); err != nil {
		t.Fatalf("scaleDown(2) [cancelled while queued]: %v", err)
	}

	srv.state.mu.Lock()
	_, stillQueued := srv.state.queued[2]
	srv.state.mu.Unlock()
	if stillQueued {
		t.Fatalf("job 2 leaked in the queued set after being cancelled while queued")
	}

	// Job 1 finishes normally.
	if err := srv.scaleDown(ctx, 1); err != nil {
		t.Fatalf("scaleDown(1): %v", err)
	}

	srv.state.mu.Lock()
	finalQueued := len(srv.state.queued)
	finalInProgress := len(srv.state.inProgress)
	srv.state.mu.Unlock()
	if finalQueued != 0 || finalInProgress != 0 {
		t.Fatalf("expected fully idle state, got queued=%d inProgress=%d", finalQueued, finalInProgress)
	}

	last, ok := client.lastCall()
	if !ok || last != 1 {
		t.Fatalf("expected replicas to reset to 1 once idle, got %v (ok=%v)", last, ok)
	}
}

// TestScaleDown_RepeatedCancelWhileQueued_NeverLeaksAcrossManyBatches drives
// 20 queued->cancelled cycles (mirroring repeated pushes under
// cancel-in-progress) and asserts the tracked state always returns to zero.
// Against the pre-fix code this fails after the very first iteration, and the
// queued set grows by one on every subsequent cycle - the exact "queued=6/7
// forever" drift reported in production.
func TestScaleDown_RepeatedCancelWhileQueued_NeverLeaksAcrossManyBatches(t *testing.T) {
	srv, client := newTestServer(6, 6*time.Hour, testClock)
	ctx := context.Background()

	var nextID int64 = 1
	for i := 0; i < 20; i++ {
		running := nextID
		nextID++
		if err := srv.scaleUp(ctx, running); err != nil {
			t.Fatalf("iteration %d: scaleUp(running=%d): %v", i, running, err)
		}
		srv.markInProgress(running)

		cancelled := nextID
		nextID++
		if err := srv.scaleUp(ctx, cancelled); err != nil {
			t.Fatalf("iteration %d: scaleUp(cancelled=%d): %v", i, cancelled, err)
		}
		if err := srv.scaleDown(ctx, cancelled); err != nil {
			t.Fatalf("iteration %d: scaleDown(cancelled=%d): %v", i, cancelled, err)
		}

		if err := srv.scaleDown(ctx, running); err != nil {
			t.Fatalf("iteration %d: scaleDown(running=%d): %v", i, running, err)
		}

		srv.state.mu.Lock()
		queuedLen := len(srv.state.queued)
		inProgressLen := len(srv.state.inProgress)
		srv.state.mu.Unlock()
		if queuedLen != 0 || inProgressLen != 0 {
			t.Fatalf("iteration %d: leaked state, queued=%d inProgress=%d (want 0/0)", i, queuedLen, inProgressLen)
		}
	}

	last, ok := client.lastCall()
	if !ok || last != 1 {
		t.Fatalf("expected final replica count 1 after 20 cancel/complete batches, got %v (ok=%v)", last, ok)
	}
}

// TestHandleWebhook_QueuedThenCancelledDoesNotLeak drives the same scenario
// through the real HTTP handler (JSON decode + HMAC included) to prove the
// fix holds at the webhook boundary, not just via direct method calls.
func TestHandleWebhook_QueuedThenCancelledDoesNotLeak(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)

	send := func(action string, id int64) *httptest.ResponseRecorder {
		return postWebhook(srv, action, id)
	}

	if rec := send("queued", 1); rec.Code != http.StatusOK {
		t.Fatalf("queued(1): status=%d body=%s", rec.Code, rec.Body)
	}
	if rec := send("in_progress", 1); rec.Code != http.StatusOK {
		t.Fatalf("in_progress(1): status=%d body=%s", rec.Code, rec.Body)
	}
	if rec := send("queued", 2); rec.Code != http.StatusOK {
		t.Fatalf("queued(2): status=%d body=%s", rec.Code, rec.Body)
	}
	// Job 2 is cancelled before it ever runs: "completed" fires with no
	// preceding "in_progress".
	if rec := send("completed", 2); rec.Code != http.StatusOK {
		t.Fatalf("completed(2): status=%d body=%s", rec.Code, rec.Body)
	}
	if rec := send("completed", 1); rec.Code != http.StatusOK {
		t.Fatalf("completed(1): status=%d body=%s", rec.Code, rec.Body)
	}

	srv.state.mu.Lock()
	queuedLen := len(srv.state.queued)
	inProgressLen := len(srv.state.inProgress)
	srv.state.mu.Unlock()
	if queuedLen != 0 || inProgressLen != 0 {
		t.Fatalf("expected idle state after cancel+complete, got queued=%d inProgress=%d", queuedLen, inProgressLen)
	}

	last, ok := client.lastCall()
	if !ok || last != 1 {
		t.Fatalf("expected replicas reset to 1, got %v (ok=%v)", last, ok)
	}
}

// --- TTL reaper (defense-in-depth against a lost webhook delivery) ---

func TestReapStaleJobs_PurgesEntriesOlderThanTTL(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	ctx := context.Background()

	srv.state.mu.Lock()
	srv.state.queued[99] = testClock().Add(-2 * time.Hour) // older than the 1h TTL
	srv.state.mu.Unlock()

	srv.reapStaleJobs(ctx)

	srv.state.mu.Lock()
	_, present := srv.state.queued[99]
	srv.state.mu.Unlock()
	if present {
		t.Fatalf("expected stale job 99 to be reaped")
	}

	last, ok := client.lastCall()
	if !ok || last != 1 {
		t.Fatalf("expected reap to reset replicas to 1, got %v (ok=%v)", last, ok)
	}
}

func TestReapStaleJobs_LeavesFreshEntriesAlone(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	ctx := context.Background()

	srv.state.mu.Lock()
	srv.state.queued[7] = testClock().Add(-5 * time.Minute) // well under the 1h TTL
	srv.state.mu.Unlock()

	srv.reapStaleJobs(ctx)

	srv.state.mu.Lock()
	_, present := srv.state.queued[7]
	srv.state.mu.Unlock()
	if !present {
		t.Fatalf("fresh job 7 should not have been reaped")
	}
	if _, ok := client.lastCall(); ok {
		t.Fatalf("reap should not touch replicas when nothing was purged")
	}
}

func TestReapStaleJobs_DoesNotTouchReplicasWhileOtherJobInProgress(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	ctx := context.Background()

	// Job 1 is genuinely running right now (fresh timestamp).
	if err := srv.scaleUp(ctx, 1); err != nil {
		t.Fatalf("scaleUp(1): %v", err)
	}
	srv.markInProgress(1)

	// Job 2 has been stuck in queued for 2h - e.g. a lost webhook delivery for
	// its terminal event.
	srv.state.mu.Lock()
	srv.state.queued[2] = testClock().Add(-2 * time.Hour)
	srv.state.mu.Unlock()

	srv.reapStaleJobs(ctx)

	srv.state.mu.Lock()
	_, stillQueued := srv.state.queued[2]
	_, job1Present := srv.state.inProgress[1]
	srv.state.mu.Unlock()
	if stillQueued {
		t.Fatalf("expected stale job 2 to be reaped from queued")
	}
	if !job1Present {
		t.Fatalf("job 1 is genuinely in progress and must not be reaped")
	}
	if _, ok := client.lastCall(); ok {
		t.Fatalf("reap must not touch replicas while a real job (1) is still in progress")
	}
}

// TestReapStaleJobs_PurgesStaleInProgressEntryButKeepsFreshOne pins that the
// reap loop over inProgress evaluates each entry independently: a stale
// in-progress entry (e.g. its completed webhook was lost) is purged even
// while a different, genuinely fresh in-progress job is left untouched and
// still blocks any replica change.
func TestReapStaleJobs_PurgesStaleInProgressEntryButKeepsFreshOne(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	ctx := context.Background()

	srv.state.mu.Lock()
	srv.state.inProgress[1] = testClock()                     // fresh: just started
	srv.state.inProgress[2] = testClock().Add(-2 * time.Hour) // stale: past the 1h TTL
	srv.state.mu.Unlock()

	srv.reapStaleJobs(ctx)

	srv.state.mu.Lock()
	_, job1Present := srv.state.inProgress[1]
	_, job2Present := srv.state.inProgress[2]
	srv.state.mu.Unlock()
	if !job1Present {
		t.Fatalf("fresh job 1 should not have been reaped")
	}
	if job2Present {
		t.Fatalf("expected stale job 2 to be reaped from inProgress")
	}
	if _, ok := client.lastCall(); ok {
		t.Fatalf("reap must not touch replicas while job 1 is still genuinely in progress")
	}
}
