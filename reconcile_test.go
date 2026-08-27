package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// --- fake GitHub client -----------------------------------------------------

type fakeGitHubClient struct {
	mu sync.Mutex
	// answers maps a job id to what GitHub says about it. An id with no entry
	// answers jobUnknown/errJobNotFound, which is the real API's behaviour for
	// an id it does not know.
	answers map[int64]jobLiveness
	// failWith, when set, is returned for every lookup instead of an answer.
	failWith error
	calls    []int64
	repos    []string
	// duringCall runs inside JobStatus, before it returns. It is how a test
	// interleaves another goroutine with the (lock-free) network window.
	duringCall func()
	delay      time.Duration
}

func (f *fakeGitHubClient) JobStatus(ctx context.Context, repo string, id int64) (jobLiveness, error) {
	f.mu.Lock()
	f.calls = append(f.calls, id)
	f.repos = append(f.repos, repo)
	answer, known := f.answers[id]
	failWith, during, delay := f.failWith, f.duringCall, f.delay
	f.mu.Unlock()

	if during != nil {
		during()
	}
	if delay > 0 {
		// Honour ctx while delaying, as the real client does: it builds its
		// request with http.NewRequestWithContext, so a cancelled or expired
		// context aborts the round-trip. A fake that slept through cancellation
		// would model a client this package does not have, and would make the
		// reconcile budget look ineffective when it is not.
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return jobUnknown, ctx.Err()
		}
	}
	if failWith != nil {
		return jobUnknown, failWith
	}
	if !known {
		return jobUnknown, errJobNotFound
	}
	return answer, nil
}

func (f *fakeGitHubClient) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeGitHubClient) callsMade() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int64, len(f.calls))
	copy(out, f.calls)
	return out
}

// newReconcileServer builds a server whose fleet is already scaled up to `live`
// with `live` phantom inProgress entries — the exact shape of the leak this card
// is about: jobs whose `completed` webhook never arrived, holding the fleet up.
func newReconcileServer(t *testing.T, maxRunners, live int, ttl time.Duration, clock func() time.Time) (*Server, *fakeRailwayClient, *fakeGitHubClient) {
	t.Helper()
	srv, rail := newTestServerWithLiveFleet(maxRunners, live, ttl, clock)
	gh := &fakeGitHubClient{answers: map[int64]jobLiveness{}}
	srv.github = gh
	srv.state.mu.Lock()
	for i := 1; i <= live; i++ {
		srv.state.inProgress[int64(9000+i)] = jobEntry{since: clock(), repo: testRepo}
	}
	srv.state.mu.Unlock()
	return srv, rail, gh
}

func trackedCount(srv *Server) (int, int) {
	srv.state.mu.Lock()
	defer srv.state.mu.Unlock()
	return len(srv.state.queued), len(srv.state.inProgress)
}

// --- the core prune property ------------------------------------------------

func TestReconcile_PrunesAnInProgressEntryGitHubReportsCompleted(t *testing.T) {
	now := testClock()
	srv, _, gh := newReconcileServer(t, 6, 3, time.Hour, func() time.Time { return now })

	// GitHub has the terminal answer for two of the three; the third is still
	// running. Only the two finished ones may be forgotten.
	gh.answers[9001] = jobFinished
	gh.answers[9002] = jobFinished
	gh.answers[9003] = jobActive

	srv.reconcile(context.Background())

	q, ip := trackedCount(srv)
	if q != 0 || ip != 1 {
		t.Fatalf("after reconcile want queued=0 inProgress=1, got queued=%d inProgress=%d", q, ip)
	}
	srv.state.mu.Lock()
	_, stillThere := srv.state.inProgress[9003]
	srv.state.mu.Unlock()
	if !stillThere {
		t.Fatalf("reconcile dropped job 9003, which GitHub reports as still running")
	}
}

func TestReconcile_PrunesAQueuedEntryWhoseTerminalWebhookWasLost(t *testing.T) {
	now := testClock()
	srv, _, gh := newReconcileServer(t, 6, 0, time.Hour, func() time.Time { return now })

	// A job cancelled while still queued (concurrency.cancel-in-progress) fires
	// completed and nothing else. Lose that delivery and the id sits in `queued`
	// until the TTL horizon.
	srv.state.mu.Lock()
	srv.state.queued[555] = jobEntry{since: now, repo: testRepo}
	srv.state.mu.Unlock()
	gh.answers[555] = jobFinished

	srv.reconcile(context.Background())

	if q, ip := trackedCount(srv); q != 0 || ip != 0 {
		t.Fatalf("want the leaked queued entry gone, got queued=%d inProgress=%d", q, ip)
	}
}

// --- the safety properties: what must NEVER be pruned -----------------------

func TestReconcile_KeepsAnEntryGitHubReportsStillRunning(t *testing.T) {
	now := testClock()
	srv, _, gh := newReconcileServer(t, 6, 2, time.Hour, func() time.Time { return now })
	gh.answers[9001] = jobActive
	gh.answers[9002] = jobActive

	srv.reconcile(context.Background())

	if q, ip := trackedCount(srv); q != 0 || ip != 2 {
		t.Fatalf("reconcile must not touch a running job: got queued=%d inProgress=%d, want 0/2", q, ip)
	}
}

func TestReconcile_KeepsEveryEntryWhenGitHubIsUnreachable(t *testing.T) {
	now := testClock()
	srv, _, gh := newReconcileServer(t, 6, 3, time.Hour, func() time.Time { return now })
	// Every id would answer "finished" — but the transport fails first, so the
	// answer is never obtained and nothing may be forgotten. A reconcile that
	// treated a failed lookup as a completion would delete the entire tracked
	// set the first time GitHub had an outage and contract the fleet under
	// every running job at once.
	for id := int64(9001); id <= 9003; id++ {
		gh.answers[id] = jobFinished
	}
	gh.failWith = errors.New("dial tcp: connection refused")

	srv.reconcile(context.Background())

	if q, ip := trackedCount(srv); q != 0 || ip != 3 {
		t.Fatalf("a failed lookup must not prune: got queued=%d inProgress=%d, want 0/3", q, ip)
	}
}

func TestReconcile_KeepsAnEntryGitHubAnswers404For(t *testing.T) {
	now := testClock()
	srv, _, _ := newReconcileServer(t, 6, 2, time.Hour, func() time.Time { return now })
	// No answers programmed at all → the fake returns errJobNotFound, exactly as
	// the live API does for a job id it does not recognise.
	//
	// This is the deliberate divergence from "ids GitHub no longer knows, drop
	// them". GitHub returns 404 — not 403 — for a private resource the token
	// cannot see, and it returns 404 for a job id queried against the wrong
	// repository (both verified against the live API). A token scoped to one
	// repo would therefore 404 on every job from every other repo in the org and
	// prune them while they are running, shrinking the fleet under live CI. The
	// leak this reconcile exists to fix is a lost `completed` webhook, and in
	// that case the job really did finish, so GitHub answers 200 + completed.
	// 404 keeps the StaleJobTTL reaper as its backstop.
	srv.reconcile(context.Background())

	if q, ip := trackedCount(srv); q != 0 || ip != 2 {
		t.Fatalf("a 404 must not prune: got queued=%d inProgress=%d, want 0/2", q, ip)
	}
}

func TestReconcile_SkipsAnEntryThatCarriesNoRepo(t *testing.T) {
	now := testClock()
	srv, _, gh := newReconcileServer(t, 6, 0, time.Hour, func() time.Time { return now })
	srv.state.mu.Lock()
	srv.state.inProgress[42] = jobEntry{since: now} // no repo: planted before the field existed
	srv.state.mu.Unlock()
	gh.answers[42] = jobFinished

	srv.reconcile(context.Background())

	if n := gh.callCount(); n != 0 {
		t.Fatalf("want no GitHub call for a repo-less entry, got %d", n)
	}
	if q, ip := trackedCount(srv); q != 0 || ip != 1 {
		t.Fatalf("a repo-less entry must be left to the TTL reaper: got queued=%d inProgress=%d", q, ip)
	}
}

// --- disabled without a token ----------------------------------------------

func TestReconcile_IsInertWithoutAGitHubClient(t *testing.T) {
	now := testClock()
	srv, rail := newTestServerWithLiveFleet(6, 4, time.Hour, func() time.Time { return now })
	seedStuckFleet(srv, 4)
	srv.github = nil // GITHUB_TOKEN unset

	before := len(rail.allCalls())
	srv.reconcile(context.Background())

	if q, ip := trackedCount(srv); q != 0 || ip != 4 {
		t.Fatalf("reconcile must be a no-op without a client: got queued=%d inProgress=%d, want 0/4", q, ip)
	}
	if after := len(rail.allCalls()); after != before {
		t.Fatalf("reconcile pushed %d replica change(s) with no client configured", after-before)
	}
}

func TestTick_RunsWithoutAGitHubClient(t *testing.T) {
	now := testClock()
	srv, _ := newTestServerWithLiveFleet(6, 4, time.Hour, func() time.Time { return now })
	srv.github = nil
	// The whole point of the nil-client path: an unconfigured deployment must
	// still run its background loop rather than panic on the new step.
	srv.tick(context.Background())
}

// --- the property this card is actually buying ------------------------------

func TestTick_AClearedLeakContractsTheFleetInTheSameCycle(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now }
	// A 6-wide fleet held up by 4 phantoms. The TTL is 7h and the boot horizon
	// has already lapsed, so today the only thing that would ever release these
	// is reapStaleJobs — 7 hours from now, at full fleet cost the whole time.
	srv, rail, gh := newReconcileServer(t, 6, 6, 7*time.Hour, clock)
	srv.state.mu.Lock()
	srv.state.bootFloorUntil = now.Add(-time.Minute)
	srv.state.mu.Unlock()

	q, ip := trackedCount(srv)
	if ip != 6 {
		t.Fatalf("fixture: want 6 phantoms, got queued=%d inProgress=%d", q, ip)
	}
	for id := int64(9001); id <= 9006; id++ {
		gh.answers[id] = jobFinished
	}

	now = now.Add(time.Minute) // past the coalesce window
	srv.tick(context.Background())

	if q, ip := trackedCount(srv); q != 0 || ip != 0 {
		t.Fatalf("want the leak cleared, got queued=%d inProgress=%d", q, ip)
	}
	calls := rail.allCalls()
	if len(calls) == 0 || calls[len(calls)-1] != 1 {
		t.Fatalf("want the fleet contracted to 1 in the same tick, Railway calls were %v", calls)
	}
}

func TestReconcile_CannotShrinkTheFleetBelowTheBootEraFloor(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now }
	// Boot-era jobs are invisible to this process forever. Reconcile knows
	// nothing about them either — it can only ask about ids it tracks — so
	// clearing every tracked id must NOT be read as "the fleet is idle" while
	// the boot horizon still stands.
	srv, rail, gh := newReconcileServer(t, 6, 5, 7*time.Hour, clock)
	for id := int64(9001); id <= 9005; id++ {
		gh.answers[id] = jobFinished
	}

	now = now.Add(time.Minute)
	srv.tick(context.Background())

	for _, n := range rail.allCalls() {
		if n < 5 {
			t.Fatalf("reconcile drove the fleet to %d while the boot floor was 5; calls %v", n, rail.allCalls())
		}
	}
}

// --- concurrency ------------------------------------------------------------

func TestReconcile_DoesNotDeleteAnIdReQueuedDuringTheLookup(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now }
	srv, _, gh := newReconcileServer(t, 6, 0, time.Hour, clock)
	srv.state.mu.Lock()
	srv.state.inProgress[77] = jobEntry{since: now, repo: testRepo}
	srv.state.mu.Unlock()
	gh.answers[77] = jobFinished

	// While the lookup is in flight (no locks held), a redelivered `queued`
	// webhook re-registers the same id. The snapshot reconcile is holding is now
	// stale, and committing it blind would delete an entry a webhook had just
	// created — and, worse, one whose replica the fleet was just scaled for.
	gh.duringCall = func() {
		srv.state.mu.Lock()
		srv.state.queued[77] = jobEntry{since: now.Add(time.Second), repo: testRepo}
		delete(srv.state.inProgress, 77)
		srv.state.mu.Unlock()
	}

	srv.reconcile(context.Background())

	srv.state.mu.Lock()
	_, present := srv.state.queued[77]
	srv.state.mu.Unlock()
	if !present {
		t.Fatalf("reconcile deleted an id that was re-registered during its lookup")
	}
}

func TestReconcile_DoesNotHoldTheScaleLockAcrossTheGitHubCall(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now }
	srv, _, gh := newReconcileServer(t, 6, 1, time.Hour, clock)
	gh.answers[9001] = jobActive

	// A webhook must not queue behind a slow GitHub round-trip: GitHub gives a
	// delivery ~10s before it gives up, and a delivery this process never
	// receives is a job it can never assert a replica for. seedFloorOnce solves
	// the same problem the same way.
	entered := make(chan struct{})
	gh.duringCall = func() { close(entered) }
	gh.delay = 300 * time.Millisecond

	done := make(chan struct{})
	go func() { srv.reconcile(context.Background()); close(done) }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatalf("reconcile never issued a GitHub lookup")
	}
	webhookDone := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		postWebhook(srv, "queued", 1234)
		webhookDone <- time.Since(start)
	}()

	select {
	case took := <-webhookDone:
		if took > 250*time.Millisecond {
			t.Fatalf("webhook blocked %s behind the GitHub round-trip", took)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("webhook never completed: it is blocked behind the GitHub round-trip")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("reconcile did not finish")
	}
}

// --- bounded work -----------------------------------------------------------

func TestReconcile_ChecksTheOldestEntriesFirstAndStopsAtTheCycleCap(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now }
	srv, _, gh := newReconcileServer(t, 6, 0, 7*time.Hour, clock)

	// More tracked ids than one cycle may look at. The oldest are the ones most
	// likely to be leaked, and are the ones nearing the TTL horizon, so they are
	// the ones a capped cycle must spend its budget on.
	total := reconcileMaxPerCycle + 10
	srv.state.mu.Lock()
	for i := 0; i < total; i++ {
		id := int64(1000 + i)
		// id 1000 is the oldest, 1000+total-1 the newest.
		srv.state.inProgress[id] = jobEntry{since: now.Add(time.Duration(i) * time.Second), repo: testRepo}
	}
	srv.state.mu.Unlock()

	srv.reconcile(context.Background())

	calls := gh.callsMade()
	if len(calls) != reconcileMaxPerCycle {
		t.Fatalf("want exactly %d lookups in one cycle, got %d", reconcileMaxPerCycle, len(calls))
	}
	for _, id := range calls {
		if id >= int64(1000+reconcileMaxPerCycle) {
			t.Fatalf("cycle spent a lookup on id %d while older entries went unchecked; calls %v", id, calls)
		}
	}
}

// --- config -----------------------------------------------------------------

func TestLoadConfig_GitHubTokenIsOptional(t *testing.T) {
	t.Setenv("GITHUB_WEBHOOK_SECRET", "sec")
	t.Setenv("RAILWAY_API_TOKEN", "tok")
	t.Setenv("RAILWAY_RUNNER_SERVICE_ID", "svc")
	t.Setenv("GITHUB_TOKEN", "")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("GITHUB_TOKEN must not be required — the service has to boot without it: %v", err)
	}
	if cfg.GitHubToken != "" {
		t.Fatalf("want empty GitHubToken, got %q", cfg.GitHubToken)
	}
}

func TestLoadConfig_ReadsGitHubToken(t *testing.T) {
	t.Setenv("GITHUB_WEBHOOK_SECRET", "sec")
	t.Setenv("RAILWAY_API_TOKEN", "tok")
	t.Setenv("RAILWAY_RUNNER_SERVICE_ID", "svc")
	t.Setenv("GITHUB_TOKEN", "ghp_example")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.GitHubToken != "ghp_example" {
		t.Fatalf("want the token read from the environment, got %q", cfg.GitHubToken)
	}
}

// --- the real GitHub client boundary ---------------------------------------

func TestGitHubClient_JobStatusMapsGitHubStatuses(t *testing.T) {
	cases := []struct {
		status string
		want   jobLiveness
	}{
		{"completed", jobFinished},
		{"in_progress", jobActive},
		{"queued", jobActive},
		{"waiting", jobActive},
	}
	for _, tc := range cases {
		t.Run(tc.status, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"id":42,"status":%q}`, tc.status)
			}))
			defer ts.Close()
			c := newGitHubClient("tok")
			c.baseURL = ts.URL
			got, err := c.JobStatus(context.Background(), testRepo, 42)
			if err != nil {
				t.Fatalf("JobStatus: %v", err)
			}
			if got != tc.want {
				t.Fatalf("status %q → %v, want %v", tc.status, got, tc.want)
			}
		})
	}
}

func TestGitHubClient_JobStatusSendsAuthAndTheRepoScopedPath(t *testing.T) {
	var gotPath, gotAuth, gotVersion, gotAccept string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotAuth = r.URL.Path, r.Header.Get("Authorization")
		gotVersion, gotAccept = r.Header.Get("X-GitHub-Api-Version"), r.Header.Get("Accept")
		fmt.Fprint(w, `{"status":"completed"}`)
	}))
	defer ts.Close()
	c := newGitHubClient("tok123")
	c.baseURL = ts.URL

	if _, err := c.JobStatus(context.Background(), "zipzoft/ambhot", 99); err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if want := "/repos/zipzoft/ambhot/actions/jobs/99"; gotPath != want {
		t.Fatalf("path %q, want %q", gotPath, want)
	}
	if want := "Bearer tok123"; gotAuth != want {
		t.Fatalf("Authorization %q, want %q", gotAuth, want)
	}
	if gotVersion != githubAPIVersion {
		t.Fatalf("X-GitHub-Api-Version %q, want %q", gotVersion, githubAPIVersion)
	}
	if gotAccept != "application/vnd.github+json" {
		t.Fatalf("Accept %q", gotAccept)
	}
}

func TestGitHubClient_JobStatus404IsNotFoundNotACompletion(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"message":"Not Found"}`)
	}))
	defer ts.Close()
	c := newGitHubClient("tok")
	c.baseURL = ts.URL

	got, err := c.JobStatus(context.Background(), testRepo, 42)
	if !errors.Is(err, errJobNotFound) {
		t.Fatalf("want errJobNotFound, got %v", err)
	}
	if got != jobUnknown {
		t.Fatalf("want jobUnknown, got %v", got)
	}
}

func TestGitHubClient_JobStatusNon200IsUnknown(t *testing.T) {
	for _, code := range []int{http.StatusForbidden, http.StatusUnauthorized, http.StatusInternalServerError} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
			}))
			defer ts.Close()
			c := newGitHubClient("tok")
			c.baseURL = ts.URL
			got, err := c.JobStatus(context.Background(), testRepo, 42)
			if err == nil {
				t.Fatalf("want an error for %d", code)
			}
			if got != jobUnknown {
				t.Fatalf("want jobUnknown for %d, got %v", code, got)
			}
		})
	}
}

func TestGitHubClient_JobStatusMissingStatusFieldIsUnknownNotFinished(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"id":42}`)
	}))
	defer ts.Close()
	c := newGitHubClient("tok")
	c.baseURL = ts.URL

	got, err := c.JobStatus(context.Background(), testRepo, 42)
	if err == nil {
		t.Fatalf("a 200 with no status field must be an error, not a silent completion")
	}
	if got != jobUnknown {
		t.Fatalf("want jobUnknown, got %v", got)
	}
}

func TestNewGitHubClient_HasTimeout(t *testing.T) {
	if to := newGitHubClient("tok").httpClient.Timeout; to <= 0 {
		t.Fatalf("client timeout is %v; a hung GitHub must not pin the reconcile goroutine", to)
	}
}

// --- the webhook must actually capture the repo -----------------------------

func TestHandleWebhook_RecordsTheRepositoryTheJobBelongsTo(t *testing.T) {
	now := testClock()
	srv, _ := newTestServer(6, time.Hour, func() time.Time { return now })

	postWebhook(srv, "queued", 4242)

	srv.state.mu.Lock()
	entry, ok := srv.state.queued[4242]
	srv.state.mu.Unlock()
	if !ok {
		t.Fatalf("job was not queued at all")
	}
	if entry.repo != testRepo {
		t.Fatalf("repo %q, want %q — reconcile cannot look up a job without it", entry.repo, testRepo)
	}
}

func TestMarkInProgress_CarriesTheRepoAcross(t *testing.T) {
	now := testClock()
	srv, _ := newTestServer(6, time.Hour, func() time.Time { return now })

	postWebhook(srv, "queued", 4242)
	postWebhook(srv, "in_progress", 4242)

	srv.state.mu.Lock()
	entry, ok := srv.state.inProgress[4242]
	srv.state.mu.Unlock()
	if !ok {
		t.Fatalf("job did not move to inProgress")
	}
	if entry.repo != testRepo {
		t.Fatalf("repo lost on the queued→inProgress move: got %q", entry.repo)
	}
}

// --- ATT-487 gate finding 1: the untracked live job -------------------------

func TestMarkInProgress_AdoptsAnUntrackedJobWhenReconcileIsEnabled(t *testing.T) {
	now := testClock()
	srv, _, _ := newReconcileServer(t, 6, 0, time.Hour, func() time.Time { return now })

	// No `queued` was ever seen for 4242 — that delivery was lost. The job is
	// running right now on a replica this process is not counting.
	postWebhook(srv, "in_progress", 4242)

	srv.state.mu.Lock()
	entry, ok := srv.state.inProgress[4242]
	srv.state.mu.Unlock()
	if !ok {
		t.Fatalf("an in_progress job was left untracked; nothing will hold a replica for it")
	}
	if entry.repo != testRepo {
		t.Fatalf("adopted entry has repo %q; reconcile could never retire it", entry.repo)
	}
}

func TestMarkInProgress_StillRefusesToAdoptWithoutReconcile(t *testing.T) {
	now := testClock()
	srv, _ := newTestServer(6, time.Hour, func() time.Time { return now })
	srv.github = nil

	postWebhook(srv, "in_progress", 4242)

	srv.state.mu.Lock()
	_, present := srv.state.inProgress[4242]
	srv.state.mu.Unlock()
	if present {
		t.Fatalf("adopted a phantom with no way to ever retire it before the TTL horizon")
	}
}

func TestReconcile_DoesNotShrinkTheFleetUnderAnUntrackedJob(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now }
	// A two-wide fleet: job 9001 is tracked and has LEAKED (its completed
	// webhook was lost), job 7777 is running but was never queued here.
	srv, rail, gh := newReconcileServer(t, 6, 2, 7*time.Hour, clock)
	srv.state.mu.Lock()
	srv.state.bootFloorUntil = now.Add(-time.Minute) // boot horizon long gone
	delete(srv.state.inProgress, 9002)
	srv.state.mu.Unlock()
	postWebhook(srv, "in_progress", 7777) // the untracked job announces itself

	gh.answers[9001] = jobFinished // the leak: really finished
	gh.answers[7777] = jobActive   // genuinely running

	now = now.Add(time.Minute)
	srv.tick(context.Background())

	// Clearing the leak must not be read as "the fleet is idle" while 7777 runs.
	for _, n := range rail.allCalls() {
		if n < 1 {
			t.Fatalf("impossible replica count %d", n)
		}
	}
	if last, ok := rail.lastCall(); ok && last < 2 {
		t.Fatalf("fleet contracted to %d while job 7777 was still running; calls %v", last, rail.allCalls())
	}
	srv.state.mu.Lock()
	_, stillTracked := srv.state.inProgress[7777]
	srv.state.mu.Unlock()
	if !stillTracked {
		t.Fatalf("the running job stopped being tracked")
	}
}

// --- ATT-487 gate finding 2: GitHub must not starve the tick ----------------

func TestTick_AssertDesiredStillRunsWhenGitHubHangs(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now }
	srv, rail, gh := newReconcileServer(t, 6, 1, 7*time.Hour, clock)
	srv.reconcileBudget = 150 * time.Millisecond
	srv.state.mu.Lock()
	srv.state.bootFloorUntil = now.Add(-time.Minute)
	srv.state.mu.Unlock()

	// GitHub answers nothing, ever. assertDesired is what makes the no-deadlock
	// guarantee unconditional; a hanging lookup must not be able to hold the
	// tick past its own interval and starve it.
	gh.delay = 10 * time.Second

	now = now.Add(time.Minute)
	before := len(rail.allCalls())
	done := make(chan struct{})
	go func() { srv.tick(context.Background()); close(done) }()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("tick never finished: reconcile has no wall-clock budget")
	}
	if len(rail.allCalls()) == before {
		t.Fatalf("assertDesired never ran: a hanging GitHub starved the rest of the tick")
	}
}

// --- ATT-487 gate finding 3: identity, not wall-clock -----------------------

func TestReconcile_DoesNotDeleteAnIdReAddedWithinTheSameClockTick(t *testing.T) {
	now := testClock()
	clock := func() time.Time { return now } // frozen, as every fixture here is
	srv, _, gh := newReconcileServer(t, 6, 0, time.Hour, clock)
	srv.state.mu.Lock()
	srv.state.inProgress[77] = jobEntry{since: now, repo: testRepo, seq: srv.state.nextSeq()}
	srv.state.mu.Unlock()
	gh.answers[77] = jobFinished

	// The job really did complete, its webhook arrives during the lookup, and a
	// NEW job with the same id is registered and starts running — landing back
	// in the SAME set, in the same clock tick, so its timestamp is byte-identical
	// to the one reconcile asked about. Map-qualifying the commit does not save
	// this case; only comparing the entry's identity does. A compare-and-delete
	// on `since` deletes a job that is running right now.
	gh.duringCall = func() {
		srv.state.mu.Lock()
		delete(srv.state.inProgress, 77)
		srv.state.inProgress[77] = jobEntry{since: now, repo: testRepo, seq: srv.state.nextSeq()}
		srv.state.mu.Unlock()
	}

	srv.reconcile(context.Background())

	srv.state.mu.Lock()
	_, present := srv.state.inProgress[77]
	srv.state.mu.Unlock()
	if !present {
		t.Fatalf("reconcile deleted an entry created during its lookup window")
	}
}

func TestState_EveryInsertionGetsADistinctIdentity(t *testing.T) {
	now := testClock()
	srv, _, _ := newReconcileServer(t, 6, 0, time.Hour, func() time.Time { return now })

	// seq is what reconcile compares against, so "this insertion" and "a later
	// insertion of the same id" must never share a value — including across the
	// queued→inProgress move, which is an insertion like any other. The frozen
	// clock these fixtures use makes `since` useless for this, which is the whole
	// reason seq exists.
	seen := map[uint64]string{}
	record := func(what string, e jobEntry) {
		if prev, dup := seen[e.seq]; dup {
			t.Fatalf("seq %d reused by %q and %q", e.seq, prev, what)
		}
		seen[e.seq] = what
	}

	postWebhook(srv, "queued", 1)
	srv.state.mu.Lock()
	record("queued job 1", srv.state.queued[1])
	srv.state.mu.Unlock()

	postWebhook(srv, "in_progress", 1)
	srv.state.mu.Lock()
	record("job 1 moved to inProgress", srv.state.inProgress[1])
	srv.state.mu.Unlock()

	postWebhook(srv, "queued", 2)
	srv.state.mu.Lock()
	record("queued job 2", srv.state.queued[2])
	srv.state.mu.Unlock()

	postWebhook(srv, "in_progress", 3) // adopted, never queued here
	srv.state.mu.Lock()
	record("adopted job 3", srv.state.inProgress[3])
	srv.state.mu.Unlock()
}

// --- ATT-487 gate finding 4: the feature must not fail silently -------------

func TestReconcile_WarnsAfterConsecutiveCyclesGitHubRecognisesNothing(t *testing.T) {
	now := testClock()
	srv, _, _ := newReconcileServer(t, 6, 2, time.Hour, func() time.Time { return now })

	// Every lookup 404s — the signature of a token without actions:read on the
	// repositories these jobs run in. Reconcile keeps every entry (correctly),
	// which means the only other evidence of the fault is a leak that never
	// clears. The blind-cycle counter is what makes it visible.
	for i := 0; i < 3; i++ {
		srv.reconcile(context.Background())
	}
	srv.state.mu.Lock()
	blind := srv.state.blindCycles
	srv.state.mu.Unlock()
	if blind != 3 {
		t.Fatalf("blindCycles = %d, want 3", blind)
	}

	// One resolved lookup clears the run: this is a "nothing is resolvable"
	// detector, not a "some job was deleted" detector.
	srv.github.(*fakeGitHubClient).answers[9001] = jobActive
	srv.reconcile(context.Background())
	srv.state.mu.Lock()
	blind = srv.state.blindCycles
	srv.state.mu.Unlock()
	if blind != 0 {
		t.Fatalf("blindCycles = %d after a resolvable cycle, want 0", blind)
	}
}

// --- ATT-487 gate finding 8: rate limiting ----------------------------------

func TestReconcile_AbandonsTheCycleWhenGitHubRateLimitsIt(t *testing.T) {
	now := testClock()
	srv, _, gh := newReconcileServer(t, 6, 5, time.Hour, func() time.Time { return now })
	gh.failWith = fmt.Errorf("%w: status 403", errRateLimited)

	srv.reconcile(context.Background())

	if n := gh.callCount(); n != 1 {
		t.Fatalf("made %d lookups after being rate-limited; want 1 then stop", n)
	}
	if q, ip := trackedCount(srv); q != 0 || ip != 5 {
		t.Fatalf("a rate limit must not prune: got queued=%d inProgress=%d", q, ip)
	}
}

func TestGitHubClient_RateLimitIsDistinguishableFromAPlainForbidden(t *testing.T) {
	cases := []struct {
		name        string
		code        int
		remaining   string
		retryAfter  string
		wantLimited bool
	}{
		{"primary rate limit", http.StatusForbidden, "0", "", true},
		{"secondary rate limit", http.StatusForbidden, "4999", "60", true},
		{"too many requests", http.StatusTooManyRequests, "", "30", true},
		{"genuinely forbidden", http.StatusForbidden, "4999", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.remaining != "" {
					w.Header().Set("x-ratelimit-remaining", tc.remaining)
				}
				if tc.retryAfter != "" {
					w.Header().Set("retry-after", tc.retryAfter)
				}
				w.WriteHeader(tc.code)
			}))
			defer ts.Close()
			c := newGitHubClient("tok")
			c.baseURL = ts.URL

			_, err := c.JobStatus(context.Background(), testRepo, 42)
			if got := errors.Is(err, errRateLimited); got != tc.wantLimited {
				t.Fatalf("errRateLimited = %v, want %v (err: %v)", got, tc.wantLimited, err)
			}
		})
	}
}

// --- the boot credential probe ---------------------------------------------

func TestGitHubClient_ProbeReportsABadCredential(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer ts.Close()
	c := newGitHubClient("bad")
	c.baseURL = ts.URL

	if err := c.probe(context.Background()); err == nil {
		t.Fatalf("a 401 from /rate_limit must be reported: it is the one unambiguous bad-token signal")
	}
}

func TestGitHubClient_ProbeAcceptsAWorkingCredential(t *testing.T) {
	var path, auth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, auth = r.URL.Path, r.Header.Get("Authorization")
		fmt.Fprint(w, `{"rate":{"remaining":4999}}`)
	}))
	defer ts.Close()
	c := newGitHubClient("good")
	c.baseURL = ts.URL

	if err := c.probe(context.Background()); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if path != "/rate_limit" {
		t.Fatalf("probe path %q, want /rate_limit (it needs no scope, which is the point)", path)
	}
	if auth != "Bearer good" {
		t.Fatalf("probe Authorization %q", auth)
	}
}

func TestReconcile_KeepsCompletionsAlreadyConfirmedWhenARateLimitCutsTheCycleShort(t *testing.T) {
	now := testClock()
	srv, _, gh := newReconcileServer(t, 6, 0, time.Hour, func() time.Time { return now })
	srv.state.mu.Lock()
	for i := 1; i <= 3; i++ {
		srv.state.inProgress[int64(i)] = jobEntry{
			since: now.Add(time.Duration(i) * time.Second), // 1 oldest, 3 newest
			repo:  testRepo, seq: srv.state.nextSeq(),
		}
	}
	srv.state.mu.Unlock()
	gh.answers[1] = jobFinished

	// Job 1 is answered — authoritatively, with a 200 — and only then does GitHub
	// start refusing. That answer does not become less true because a later
	// request was throttled, and discarding it would leave a known-dead entry
	// holding a replica for another cycle.
	var seen int
	gh.duringCall = func() {
		seen++
		if seen == 2 {
			gh.mu.Lock()
			gh.failWith = fmt.Errorf("%w: status 403", errRateLimited)
			gh.mu.Unlock()
		}
	}

	srv.reconcile(context.Background())

	srv.state.mu.Lock()
	_, one := srv.state.inProgress[1]
	_, two := srv.state.inProgress[2]
	srv.state.mu.Unlock()
	if one {
		t.Fatalf("discarded a completion GitHub had already confirmed before the rate limit")
	}
	if !two {
		t.Fatalf("pruned job 2, whose lookup was refused — a rate limit is not a completion")
	}
}
