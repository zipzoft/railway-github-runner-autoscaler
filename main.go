package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultMaxRunners  = 3
	defaultPort        = "8080"
	defaultRunnerLabel = "self-hosted,railway"
	// 7h. GitHub's own default job timeout is 360 minutes, so a 360 TTL leaves
	// ZERO margin: a job legitimately running to that default would be reaped
	// while alive, and reapStaleJobs would then scale the fleet down under it.
	// 420 keeps a real hour of headroom over the longest job GitHub will allow
	// by default. A workflow setting timeout-minutes above 420 needs
	// STALE_JOB_TTL_MINUTES raised to match.
	defaultStaleJobTTLMin = 420
	reapInterval          = 5 * time.Minute
	// coalesceWindow suppresses a re-push of an UNCHANGED replica count made
	// within this long of the last successful push. It exists to bound the call
	// amplification of asserting on every webhook (a 30-job burst would
	// otherwise issue 30 serialized Railway mutations behind scaleMu, and the
	// last webhook in the burst can outlive GitHub's delivery timeout). Recovery
	// does not depend on the suppressed calls: assertDesired re-pushes every
	// reapInterval for as long as any job is outstanding.
	coalesceWindow = 30 * time.Second
	// seedTimeout bounds the boot-time replica read. The read holds no locks, so
	// nothing waits on it — not the listener, not a webhook.
	seedTimeout = 10 * time.Second
	// reconcileBudget is the WALL-CLOCK ceiling on one reconcile pass, and it is
	// not the same bound as reconcileMaxPerCycle. That one caps requests against
	// GitHub's hourly budget; this one caps how long the background tick can be
	// held up. They protect different things and dropping either reintroduces a
	// failure: without this bound, a degraded GitHub (50 ids x a 10s timeout =
	// 500s) outlasts the 300s reapInterval, the ticker's single-slot channel
	// drops ticks, and assertDesired runs at half cadence or worse — precisely
	// when it is needed most, since the same provider that is failing to answer
	// lookups is the one failing to deliver webhooks. assertDesired is what makes
	// the no-deadlock guarantee unconditional (ATT-482); nothing new may make it
	// conditional on GitHub's latency.
	reconcileBudget = 60 * time.Second
	// reconcileMaxPerCycle bounds the GitHub lookups one reconcile pass may
	// issue. The tracked set is normally at most a handful of ids, so this is a
	// backstop against a pathological state rather than a routine limit: at one
	// pass per reapInterval it caps the service at 50 x 12 = 600 requests/hour
	// against a 5,000/hour authenticated budget. Entries are checked oldest
	// first, so a capped cycle spends its budget on the ids nearest the TTL
	// horizon — the ones most likely to be leaked.
	reconcileMaxPerCycle = 50
)

type Config struct {
	WebhookSecret string
	RailwayToken  string
	ServiceID     string
	EnvironmentID string
	MaxRunners    int
	Port          string
	RunnerLabels  []string
	StaleJobTTL   time.Duration
	// GitHubToken is OPTIONAL. Empty disables reconcile and leaves StaleJobTTL as
	// the only cleanup, which is exactly how this service behaved before
	// reconcile existed. It is optional on purpose: writing a credential onto
	// this shared service is a human-gated action, so the code has to be able to
	// ship and run before anyone has set the value.
	GitHubToken string
}

// State tracks each unfinished job by GitHub job ID. queued and inProgress
// record the time the job entered that state so reapStaleJobs can detect
// entries that never received a terminal webhook. A job that has completed is
// deleted outright and never recorded: it needs no runner, and keeping finished
// ids in a set that only cleared once inProgress hit zero was what let a single
// lost webhook grow the set without bound (ATT-482 — it reached 5972 entries).
//
// applied is the replica count last accepted by Railway. It is the floor that
// keeps a scale decision from shrinking the fleet out from under a running job,
// replacing the finished-job accounting that used to serve that purpose by
// accident. lastPush is when that value was pushed, and drives coalescing.
//
// bootFloor and bootFloorUntil cover the jobs that were already running when
// this process started. Those jobs are permanently invisible to it: markInProgress
// refuses to adopt an id that was never queued here, so a restart-era job is
// never tracked and its completion is never counted. An empty pair of counters
// therefore does NOT mean the fleet is idle until enough time has passed that no
// boot-era job could still be running.
//
// Tracking cannot close this. Deciding on "a drain I watched" only defers the
// harm by one job cycle: a new job arrives, runs, finishes, the counters return
// to zero, and SetReplicas(1) cancels every boot-era job still going. The only
// honest bound is time, so bootFloor holds until StaleJobTTL has elapsed since
// boot — the same horizon beyond which this service already declares a job dead.
//
// The cost is real and worth stating: a restart while the fleet is wide holds it
// wide for that horizon, even if the width was itself a leak. That is bounded
// over-provisioning, and it is the recoverable direction. Reconciling against
// GitHub's own view of in-progress jobs is what removes the horizon entirely.
type State struct {
	mu             sync.Mutex
	queued         map[int64]jobEntry
	inProgress     map[int64]jobEntry
	applied        int
	lastPush       time.Time
	bootFloor      int
	bootFloorUntil time.Time
	// blindCycles counts consecutive reconcile passes in which every single
	// lookup came back "not found". One such pass is unremarkable; a run of them
	// is the signature of a token that cannot read the repositories the jobs are
	// in, which makes reconcile a silent no-op — the exact 7h leak it exists to
	// remove, with nothing else to show for it.
	blindCycles int
	// seq hands out a fresh identity to every entry inserted into either set.
	// See jobEntry.seq for why reconcile cannot use the timestamp for this.
	seq uint64
}

// RailwayClient scales the runner service. It is an interface so tests can
// substitute a fake and assert on calls without making network requests.
type RailwayClient interface {
	SetReplicas(ctx context.Context, n int) error
	// Replicas reports the count Railway currently has configured. It is read
	// once at boot to seed the replica floor; see newState.
	Replicas(ctx context.Context) (int, error)
}

type Server struct {
	cfg    Config
	state  *State
	client RailwayClient
	// github answers "is this job still alive?" during reconcile. It is nil when
	// GITHUB_TOKEN is unset, and a nil github disables reconcile entirely: the
	// service then behaves exactly as it did before reconcile existed, with the
	// StaleJobTTL reaper as the only cleanup. That is what lets this ship before
	// a human has set the token on the service.
	github GitHubClient
	clock  func() time.Time
	// tickInterval overrides reapInterval for tests that need the background loop
	// to run at a testable cadence. Zero means reapInterval.
	tickInterval time.Duration
	// reconcileBudget overrides the wall-clock ceiling on one reconcile pass for
	// tests that need it to expire quickly. Zero means the package default.
	reconcileBudget time.Duration
	// scaleMu serializes the compute-and-apply of the replica count across
	// scaleUp/scaleDown/reapStaleJobs so concurrent webhooks and the reap loop
	// can't push a stale or out-of-order numReplicas to Railway. It is separate
	// from state.mu so the non-scaling paths (the in_progress webhook, state
	// reads) never block on the Railway network call. Lock order: scaleMu → state.mu.
	scaleMu sync.Mutex
}

func loadConfig() (Config, error) {
	secret := os.Getenv("GITHUB_WEBHOOK_SECRET")
	token := os.Getenv("RAILWAY_API_TOKEN")
	serviceID := os.Getenv("RAILWAY_RUNNER_SERVICE_ID")

	if secret == "" {
		return Config{}, fmt.Errorf("GITHUB_WEBHOOK_SECRET is required")
	}
	if token == "" {
		return Config{}, fmt.Errorf("RAILWAY_API_TOKEN is required")
	}
	if serviceID == "" {
		return Config{}, fmt.Errorf("RAILWAY_RUNNER_SERVICE_ID is required")
	}

	maxRunners := defaultMaxRunners
	if v := os.Getenv("MAX_RUNNERS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return Config{}, fmt.Errorf("MAX_RUNNERS must be a positive integer, got %q", v)
		}
		maxRunners = n
	}

	staleJobTTLMin := defaultStaleJobTTLMin
	if v := os.Getenv("STALE_JOB_TTL_MINUTES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return Config{}, fmt.Errorf("STALE_JOB_TTL_MINUTES must be a positive integer, got %q", v)
		}
		staleJobTTLMin = n
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	labelStr := os.Getenv("RUNNER_LABELS")
	if labelStr == "" {
		labelStr = defaultRunnerLabel
	}
	labels := strings.Split(labelStr, ",")
	for i, l := range labels {
		labels[i] = strings.TrimSpace(strings.ToLower(l))
	}

	return Config{
		WebhookSecret: secret,
		RailwayToken:  token,
		ServiceID:     serviceID,
		EnvironmentID: os.Getenv("RAILWAY_ENVIRONMENT_ID"),
		MaxRunners:    maxRunners,
		Port:          port,
		RunnerLabels:  labels,
		StaleJobTTL:   time.Duration(staleJobTTLMin) * time.Minute,
		GitHubToken:   os.Getenv("GITHUB_TOKEN"),
	}, nil
}

// newState builds the boot state. It exists so the floor cannot be seeded one
// way in production and another in a test: getting that seed wrong is the
// difference between reviving a dead fleet and cancelling live CI jobs.
//
// The floor starts at the cap and the boot horizon starts running immediately,
// so the fleet is protected from the very first webhook — before seedFloorOnce
// has had a chance to refine the value downward against reality.
func newState(applied int, now time.Time, ttl time.Duration) *State {
	return &State{
		queued:         make(map[int64]jobEntry),
		inProgress:     make(map[int64]jobEntry),
		applied:        applied,
		bootFloor:      applied,
		bootFloorUntil: now.Add(ttl),
	}
}

// seedFloor reads the replica count Railway currently has and returns it as the
// starting floor. The job counters legitimately start empty on every boot, but
// the floor must NOT: a fresh process cannot tell an idle replica from one
// running a job, and the two ways of being wrong are not symmetric. Assuming the
// fleet is healthy leaves a dead one asleep — that is ATT-482. Assuming it is
// idle makes the first scale decision push a count DOWN, and Railway is free to
// drop any replica when the count shrinks, including one mid-job: a deploy of
// this service during a busy CI batch would cancel the jobs it was supposed to
// be serving.
//
// On a read failure the floor falls back to the cap. That over-provisions for
// one batch, which is the recoverable direction; the floor releases itself at
// the first genuine drain either way.
func seedFloor(ctx context.Context, client RailwayClient, maxRunners int) int {
	n, err := client.Replicas(ctx)
	if err != nil {
		log.Printf("[WARN] could not read current replica count (%v); seeding the floor at the cap "+
			"%d so the first scale decision cannot shrink a fleet that may be mid-job", err, maxRunners)
		return maxRunners
	}
	if n > maxRunners {
		// The fleet is wider than the configured cap — normally because
		// MAX_RUNNERS was lowered since the last scale. The next decision will
		// pull it down to the cap, and Railway may drop a replica that is
		// mid-job to do it. Say so, because the alternative reading of a sudden
		// job cancellation is a much longer hunt.
		log.Printf("[WARN] Railway reports %d replicas, above MAX_RUNNERS=%d; the next scale "+
			"decision will contract the fleet to the cap and may drop a replica mid-job", n, maxRunners)
	}
	return max(1, min(n, maxRunners))
}

func main() {
	log.SetOutput(os.Stdout)

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &Server{
		cfg: cfg,
		// Start the floor at the cap. seedFloorOnce refines it from Railway's
		// live count moments later; until then the cap is the value that cannot
		// shrink a fleet which may be mid-job.
		state:  newState(cfg.MaxRunners, time.Now(), cfg.StaleJobTTL),
		client: newRailwayClient(cfg),
		clock:  time.Now,
	}
	if cfg.GitHubToken != "" {
		gh := newGitHubClient(cfg.GitHubToken)
		srv.github = gh
		log.Printf("reconcile enabled: tracked jobs are checked against GitHub every %s, so a lost "+
			"terminal webhook clears within one cycle instead of on the %s horizon", reapInterval, cfg.StaleJobTTL)
		// Confirm the credential works now rather than at the first leak. Off the
		// critical path for the same reason the Railway seed is: nothing about
		// serving webhooks may wait on an external round-trip.
		go func() {
			probeCtx, cancel := context.WithTimeout(ctx, seedTimeout)
			defer cancel()
			if err := gh.probe(probeCtx); err != nil {
				log.Printf("[WARN] GITHUB_TOKEN did not authenticate (%v); reconcile will run but every "+
					"lookup will fail, leaving the %s horizon as the only cleanup", err, cfg.StaleJobTTL)
				return
			}
			log.Printf("GITHUB_TOKEN authenticated")
		}()
	} else {
		log.Printf("reconcile DISABLED: GITHUB_TOKEN is not set, so a job whose terminal webhook is lost "+
			"holds a replica until the %s STALE_JOB_TTL horizon. Set GITHUB_TOKEN to a token with "+
			"actions:read to enable it", cfg.StaleJobTTL)
	}

	// The job counters start empty on every boot and are deliberately not
	// reconciled against GitHub: this process can't tell an idle replica from one
	// running a job, so an in_progress webhook for a job whose queued entry was
	// lost on restart is ignored rather than resurrected (see markInProgress),
	// leaving that job untracked until it completes.
	//
	// The replica floor is the opposite case and must NOT start empty — assuming
	// the fleet is idle would make the first scale decision shrink a fleet that
	// may be mid-job. It starts at the cap and is refined from Railway below.
	log.Printf("startup: counters initialised (queued=0 inProgress=0), replica floor at cap %d pending seed, staleJobTTL=%s",
		cfg.MaxRunners, cfg.StaleJobTTL)

	// reapLoop stops when ctx is cancelled by SIGINT/SIGTERM, the same signal
	// that drives the graceful HTTP shutdown below.
	go srv.reapLoop(ctx)

	// Seed the floor concurrently with serving rather than before it. Reading it
	// first would block the listener on a Railway round-trip, and two things go
	// wrong when that read is slow: the port opens too late for the deploy
	// healthcheck, and any webhook delivered in the meantime is refused. A
	// refused delivery is NOT safely retried — GitHub does not guarantee
	// automatic redelivery of a connection it could not establish — and a job
	// this process never records is a job assertDesired can never assert for,
	// which is the ATT-482 symptom class all over again. seedFloorOnce holds
	// scaleMu, so a webhook that arrives first is ordered, not raced.
	go func() {
		seedCtx, cancel := context.WithTimeout(ctx, seedTimeout)
		defer cancel()
		srv.seedFloorOnce(seedCtx)
	}()

	httpSrv := newHTTPServer(":"+cfg.Port, srv)

	log.Printf("starting on :%s | service=%s max=%d labels=%v",
		cfg.Port, cfg.ServiceID, cfg.MaxRunners, cfg.RunnerLabels)
	if err := serve(ctx, httpSrv); err != nil {
		log.Fatalf("server error: %v", err)
	}
	log.Printf("shutdown complete")
}

// serve runs httpSrv until ctx is cancelled, then drains in-flight requests via
// a bounded graceful shutdown. It binds httpSrv.Addr and delegates to
// serveListener so the shutdown behaviour is testable against a real listener.
func serve(ctx context.Context, httpSrv *http.Server) error {
	ln, err := net.Listen("tcp", httpSrv.Addr)
	if err != nil {
		return err
	}
	return serveListener(ctx, httpSrv, ln)
}

// serveListener serves on ln until ctx is cancelled, then calls Shutdown to let
// in-flight requests finish (bounded by a 15s deadline). It returns nil on a
// clean shutdown; a mid-flight bind/serve failure is returned as-is.
func serveListener(ctx context.Context, httpSrv *http.Server, ln net.Listener) error {
	errCh := make(chan error, 1)
	go func() {
		err := httpSrv.Serve(ln)
		if err == http.ErrServerClosed {
			err = nil
		}
		errCh <- err
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return httpSrv.Shutdown(shutdownCtx)
	}
}

// newHTTPServer builds the HTTP server with explicit timeouts. http.ListenAndServe
// uses a zero-value server with no read/write/idle bounds, which leaves the public
// webhook endpoint open to slow-client (Slowloris) connection exhaustion.
func newHTTPServer(addr string, srv *Server) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/webhook", srv.handleWebhook)
	mux.HandleFunc("/health", srv.handleHealth)
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		// Above the ~10s Railway client timeout the handler can block on while
		// scaling synchronously, so a slow-but-successful scale still returns a
		// client-visible 200 instead of a dropped write GitHub reads as a failure.
		WriteTimeout: 20 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
}
