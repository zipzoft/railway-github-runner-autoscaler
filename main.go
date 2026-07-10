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

// State tracks each job by GitHub job ID across the queued/in-progress/completed
// lifecycle. queued and inProgress record the time the job entered that state so
// reapStaleJobs can detect entries that never received a terminal webhook.
type State struct {
	mu         sync.Mutex
	queued     map[int64]time.Time
	inProgress map[int64]time.Time
	completed  map[int64]struct{}
}

// RailwayClient scales the runner service. It is an interface so tests can
// substitute a fake and assert on calls without making network requests.
type RailwayClient interface {
	SetReplicas(ctx context.Context, n int) error
}

type Server struct {
	cfg    Config
	state  *State
	client RailwayClient
	clock  func() time.Time
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
		state: &State{
			queued:     make(map[int64]time.Time),
			inProgress: make(map[int64]time.Time),
			completed:  make(map[int64]struct{}),
		},
		client: newRailwayClient(cfg),
		clock:  time.Now,
	}

	// In-memory state starts empty on every boot and is deliberately not
	// reconciled against Railway's live replica count: after a mid-batch restart
	// the service can't tell an idle replica from one running a job, so forcing a
	// reset would kill in-flight runners. The next completed batch settles the
	// count back to 1 (see scaleDown); an orphaned high count with no further
	// jobs only costs idle memory. For the same reason, an in_progress webhook
	// for a job whose queued entry was lost on restart is ignored (see
	// markInProgress), leaving that job untracked until it completes.
	log.Printf("startup: counters initialised (queued=0 inProgress=0), base replica ready, staleJobTTL=%s", cfg.StaleJobTTL)

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
