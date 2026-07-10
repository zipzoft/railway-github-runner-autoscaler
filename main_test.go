package main

import (
	"testing"
	"time"
)

func TestLoadConfig_RequiresWebhookSecret(t *testing.T) {
	t.Setenv("GITHUB_WEBHOOK_SECRET", "")
	t.Setenv("RAILWAY_API_TOKEN", "tok")
	t.Setenv("RAILWAY_RUNNER_SERVICE_ID", "svc")
	if _, err := loadConfig(); err == nil {
		t.Fatalf("expected error when GITHUB_WEBHOOK_SECRET is missing")
	}
}

func TestLoadConfig_RequiresRailwayToken(t *testing.T) {
	t.Setenv("GITHUB_WEBHOOK_SECRET", "sec")
	t.Setenv("RAILWAY_API_TOKEN", "")
	t.Setenv("RAILWAY_RUNNER_SERVICE_ID", "svc")
	if _, err := loadConfig(); err == nil {
		t.Fatalf("expected error when RAILWAY_API_TOKEN is missing")
	}
}

func TestLoadConfig_RequiresServiceID(t *testing.T) {
	t.Setenv("GITHUB_WEBHOOK_SECRET", "sec")
	t.Setenv("RAILWAY_API_TOKEN", "tok")
	t.Setenv("RAILWAY_RUNNER_SERVICE_ID", "")
	if _, err := loadConfig(); err == nil {
		t.Fatalf("expected error when RAILWAY_RUNNER_SERVICE_ID is missing")
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	t.Setenv("GITHUB_WEBHOOK_SECRET", "sec")
	t.Setenv("RAILWAY_API_TOKEN", "tok")
	t.Setenv("RAILWAY_RUNNER_SERVICE_ID", "svc")
	t.Setenv("MAX_RUNNERS", "")
	t.Setenv("STALE_JOB_TTL_MINUTES", "")
	t.Setenv("PORT", "")
	t.Setenv("RUNNER_LABELS", "")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.MaxRunners != defaultMaxRunners {
		t.Errorf("MaxRunners = %d, want %d", cfg.MaxRunners, defaultMaxRunners)
	}
	if cfg.StaleJobTTL != time.Duration(defaultStaleJobTTLMin)*time.Minute {
		t.Errorf("StaleJobTTL = %s, want %s", cfg.StaleJobTTL, time.Duration(defaultStaleJobTTLMin)*time.Minute)
	}
	if cfg.Port != defaultPort {
		t.Errorf("Port = %s, want %s", cfg.Port, defaultPort)
	}
	want := []string{"self-hosted", "railway"}
	if len(cfg.RunnerLabels) != len(want) || cfg.RunnerLabels[0] != want[0] || cfg.RunnerLabels[1] != want[1] {
		t.Errorf("RunnerLabels = %v, want %v", cfg.RunnerLabels, want)
	}
}

func TestLoadConfig_StaleJobTTLOverride(t *testing.T) {
	t.Setenv("GITHUB_WEBHOOK_SECRET", "sec")
	t.Setenv("RAILWAY_API_TOKEN", "tok")
	t.Setenv("RAILWAY_RUNNER_SERVICE_ID", "svc")
	t.Setenv("STALE_JOB_TTL_MINUTES", "30")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.StaleJobTTL != 30*time.Minute {
		t.Errorf("StaleJobTTL = %s, want 30m", cfg.StaleJobTTL)
	}
}

func TestLoadConfig_InvalidStaleJobTTL(t *testing.T) {
	t.Setenv("GITHUB_WEBHOOK_SECRET", "sec")
	t.Setenv("RAILWAY_API_TOKEN", "tok")
	t.Setenv("RAILWAY_RUNNER_SERVICE_ID", "svc")
	t.Setenv("STALE_JOB_TTL_MINUTES", "not-a-number")

	if _, err := loadConfig(); err == nil {
		t.Fatalf("expected error for invalid STALE_JOB_TTL_MINUTES")
	}
}

func TestLoadConfig_InvalidMaxRunners(t *testing.T) {
	t.Setenv("GITHUB_WEBHOOK_SECRET", "sec")
	t.Setenv("RAILWAY_API_TOKEN", "tok")
	t.Setenv("RAILWAY_RUNNER_SERVICE_ID", "svc")
	t.Setenv("MAX_RUNNERS", "0")

	if _, err := loadConfig(); err == nil {
		t.Fatalf("expected error for MAX_RUNNERS=0")
	}
}

func TestNewHTTPServer_SetsTimeouts(t *testing.T) {
	srv, _ := newTestServer(6, time.Hour, testClock)
	hs := newHTTPServer(":0", srv)
	if hs.ReadHeaderTimeout == 0 || hs.ReadTimeout == 0 || hs.WriteTimeout == 0 || hs.IdleTimeout == 0 {
		t.Fatalf("server timeouts must be non-zero: readHeader=%s read=%s write=%s idle=%s",
			hs.ReadHeaderTimeout, hs.ReadTimeout, hs.WriteTimeout, hs.IdleTimeout)
	}
}
