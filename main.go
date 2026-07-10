package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
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

	log.Printf("startup: counters initialised (queued=0 inProgress=0), base replica ready, staleJobTTL=%s", cfg.StaleJobTTL)

	// No shutdown path cancels this: ListenAndServe below never returns until a
	// fatal error, and log.Fatalf exits the process immediately (skipping
	// defers), so the process exit itself is what ends this goroutine.
	go srv.reapLoop(context.Background())

	httpSrv := newHTTPServer(":"+cfg.Port, srv)

	log.Printf("starting on :%s | service=%s max=%d labels=%v",
		cfg.Port, cfg.ServiceID, cfg.MaxRunners, cfg.RunnerLabels)
	if err := httpSrv.ListenAndServe(); err != nil {
		log.Fatalf("server error: %v", err)
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
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}
