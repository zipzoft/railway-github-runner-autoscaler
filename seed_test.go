package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// --- seedFloor: the fallback direction ---
//
// The whole argument for reading the floor at boot rests on the failure path
// being safe. Nothing pinned it: a refactor returning 1 on error would keep CI
// green and then shrink a live fleet on every degraded boot — and a degraded
// Railway is exactly when the read fails.

func TestSeedFloor_ReadFailureFallsBackToTheCapNotToOne(t *testing.T) {
	client := &fakeRailwayClient{replicasErr: fmt.Errorf("railway down")}
	if got := seedFloor(context.Background(), client, 6); got != 6 {
		t.Fatalf("a failed read seeded the floor at %d; it must fall back to the cap (6), because "+
			"seeding low shrinks a fleet that may be mid-job", got)
	}
}

func TestSeedFloor_TimeoutFallsBackToTheCap(t *testing.T) {
	client := &fakeRailwayClient{replicas: 1, respectCtx: true}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := seedFloor(ctx, client, 6); got != 6 {
		t.Fatalf("a cancelled read seeded the floor at %d; expected the cap 6", got)
	}
}

func TestSeedFloor_ClampsAnObservedFleetToTheCap(t *testing.T) {
	client := &fakeRailwayClient{replicas: 9}
	if got := seedFloor(context.Background(), client, 6); got != 6 {
		t.Fatalf("expected a fleet wider than the cap clamped to 6, got %d", got)
	}
}

func TestSeedFloor_NeverSeedsBelowOne(t *testing.T) {
	client := &fakeRailwayClient{replicas: 0}
	if got := seedFloor(context.Background(), client, 6); got != 1 {
		t.Fatalf("expected the floor never below 1, got %d", got)
	}
}

// --- railwayClient.Replicas against a real HTTP surface ---

func newReplicasServer(t *testing.T, body string, status int) *railwayClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Project-Access-Token"); got != "tok" {
			t.Errorf("expected project-token auth, got header %q", got)
		}
		var req gqlRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if req.Variables["serviceId"] != "svc" || req.Variables["environmentId"] != "env" {
			t.Errorf("unexpected variables: %v", req.Variables)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return &railwayClient{
		token:         "tok",
		serviceID:     "svc",
		environmentID: "env",
		baseURL:       srv.URL,
		httpClient:    &http.Client{Timeout: 2 * time.Second},
	}
}

func TestRailwayClient_ReplicasReadsTheCount(t *testing.T) {
	c := newReplicasServer(t, `{"data":{"serviceInstance":{"numReplicas":5}}}`, http.StatusOK)
	n, err := c.Replicas(context.Background())
	if err != nil {
		t.Fatalf("Replicas: %v", err)
	}
	if n != 5 {
		t.Fatalf("expected 5, got %d", n)
	}
}

// Railway reports null for a service with no replica override. Unmarshalling
// that into an int would silently yield 0, which reads as "idle fleet" — the one
// value that must never be inferred. It has to surface as an error so seedFloor
// takes the cap instead.
func TestRailwayClient_ReplicasTreatsNullAsUnknownNotZero(t *testing.T) {
	c := newReplicasServer(t, `{"data":{"serviceInstance":{"numReplicas":null}}}`, http.StatusOK)
	if _, err := c.Replicas(context.Background()); err == nil {
		t.Fatal("expected null numReplicas to be an error, not a silent 0")
	}
}

func TestRailwayClient_ReplicasSurfacesGraphQLErrors(t *testing.T) {
	c := newReplicasServer(t, `{"errors":[{"message":"Project not found"}]}`, http.StatusOK)
	if _, err := c.Replicas(context.Background()); err == nil {
		t.Fatal("expected a graphql error to surface")
	}
}

func TestRailwayClient_ReplicasSurfacesNon200(t *testing.T) {
	c := newReplicasServer(t, `nope`, http.StatusInternalServerError)
	if _, err := c.Replicas(context.Background()); err == nil {
		t.Fatal("expected a non-200 response to surface")
	}
}

// --- seedFloorOnce ordering ---

// A webhook that lands before the seed completes has already pushed a value
// based on real work. Overwriting the floor with the pre-boot count would undo
// that and let a later decision shrink under the job it just scaled up for.
func TestSeedFloorOnce_DoesNotOverwriteAFloorAScaleDecisionAlreadySet(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	ctx := context.Background()
	for id := int64(1); id <= 3; id++ {
		if err := srv.scaleUp(ctx, id, testRepo); err != nil {
			t.Fatalf("scaleUp(%d): %v", id, err)
		}
	}
	client.replicas = 1 // what Railway had before this process booted

	srv.seedFloorOnce(ctx)

	srv.state.mu.Lock()
	applied := srv.state.applied
	srv.state.mu.Unlock()
	if applied != 3 {
		t.Fatalf("the boot seed overwrote a floor of 3 set by real work with %d", applied)
	}
}

func TestSeedFloorOnce_SeedsWhenNothingHasPushedYet(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	client.replicas = 4

	srv.seedFloorOnce(context.Background())

	srv.state.mu.Lock()
	applied := srv.state.applied
	srv.state.mu.Unlock()
	if applied != 4 {
		t.Fatalf("expected the floor seeded to Railway's 4, got %d", applied)
	}
}
