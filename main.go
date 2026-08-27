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
	defaultMaxRunners     = 3
	defaultPort           = "8080"
	defaultRunnerLabel    = "self-hosted,railway"
	defaultStaleJobTTLMin = 360 // 6h: far beyond any realistic CI job duration on this fleet
	reapInterval          = 5 * time.Minute
	// coalesceWindow suppresses a re-push of an UNCHANGED replica count made
	// within this long of the last successful push. It exists to bound the call
	// amplification of asserting on every webhook (a 30-job burst would
	// otherwise issue 30 serialized Railway mutations behind scaleMu, and the
	// last webhook in the burst can outlive GitHub's delivery timeout). Recovery
	// does not depend on the suppressed calls: assertOutstanding re-pushes every
	// reapInterval for as long as any job is outstanding.
	coalesceWindow = 30 * time.Second
	// seedTimeout bounds the boot-time replica read. It runs before the HTTP
	// listener binds, so it must stay comfortably inside the deploy healthcheck
	// budget (the service is configured with healthcheckTimeout: 10).
	seedTimeout = 5 * time.Second
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
type State struct {
	mu         sync.Mutex
	queued     map[int64]time.Time
	inProgress map[int64]time.Time
	applied    int
	lastPush   time.Time
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
	clock  func() time.Time
	// tickInterval overrides reapInterval for tests that need the background loop
	// to run at a testable cadence. Zero means reapInterval.
	tickInterval time.Duration
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
	}, nil
}

// newState builds the boot state with the replica floor seeded to `applied`. It
// exists so the floor cannot be seeded one way in production and another in a
// test: getting that seed wrong is the difference between reviving a dead fleet
// and cancelling live CI jobs.
func newState(applied int) *State {
	return &State{
		queued:     make(map[int64]time.Time),
		inProgress: make(map[int64]time.Time),
		applied:    applied,
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

	client := newRailwayClient(cfg)

	// Bounded well under the service's healthcheck timeout: this read happens
	// BEFORE the listener binds, so a slow Railway backend would otherwise delay
	// the port opening past the healthcheck and fail the deploy. Timing out here
	// is not a failure — seedFloor falls back to the cap, which is the safe
	// direction. Webhooks cannot be missed during the wait either: nothing is
	// listening yet, so GitHub retries the delivery.
	seedCtx, seedCancel := context.WithTimeout(ctx, seedTimeout)
	floor := seedFloor(seedCtx, client, cfg.MaxRunners)
	seedCancel()

	srv := &Server{
		cfg:    cfg,
		state:  newState(floor),
		client: client,
		clock:  time.Now,
	}

	// The job counters start empty on every boot and are deliberately not
	// reconciled against GitHub: this process can't tell an idle replica from one
	// running a job, so an in_progress webhook for a job whose queued entry was
	// lost on restart is ignored rather than resurrected (see markInProgress),
	// leaving that job untracked until it completes.
	//
	// The replica floor is the opposite case and must NOT start empty — it is read
	// back from Railway instead (see seedFloor), because assuming the fleet is
	// idle would make the first scale decision shrink a fleet that may be mid-job.
	log.Printf("startup: counters initialised (queued=0 inProgress=0), replica floor seeded at %d from Railway, staleJobTTL=%s",
		floor, cfg.StaleJobTTL)

	// reapLoop stops when ctx is cancelled by SIGINT/SIGTERM, the same signal
	// that drives the graceful HTTP shutdown below.
	go srv.reapLoop(ctx)

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
