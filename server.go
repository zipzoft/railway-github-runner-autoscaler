package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

const (
	railwayGQLURL = "https://backboard.railway.app/graphql/v2"
	maxBodyBytes  = 5 * 1024 * 1024 // 5MB
)

type WorkflowJobEvent struct {
	Action      string      `json:"action"`
	WorkflowJob WorkflowJob `json:"workflow_job"`
}

type WorkflowJob struct {
	ID     int64    `json:"id"`
	Labels []string `json:"labels"`
}

type gqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}

type gqlResponse struct {
	Data   json.RawMessage `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}

	if !validateHMAC(body, r.Header.Get("X-Hub-Signature-256"), s.cfg.WebhookSecret) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	if r.Header.Get("X-GitHub-Event") != "workflow_job" {
		w.WriteHeader(http.StatusOK)
		return
	}

	var event WorkflowJobEvent
	if err := json.Unmarshal(body, &event); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if !hasLabels(event.WorkflowJob.Labels, s.cfg.RunnerLabels) {
		log.Printf("webhook ignored: labels %v do not match required %v", event.WorkflowJob.Labels, s.cfg.RunnerLabels)
		w.WriteHeader(http.StatusOK)
		return
	}

	log.Printf("webhook received: action=%s labels=%v", event.Action, event.WorkflowJob.Labels)

	id := event.WorkflowJob.ID
	switch event.Action {
	case "queued":
		if err := s.scaleUp(r.Context(), id); err != nil {
			log.Printf("scale up error: %v", err)
			http.Error(w, "failed to scale up", http.StatusInternalServerError)
			return
		}
	case "in_progress":
		s.markInProgress(id)
	case "completed":
		// completed is GitHub's only terminal workflow_job action: it fires whether
		// the job ran to completion, failed, or was cancelled before ever starting
		// (e.g. superseded by concurrency.cancel-in-progress). scaleDown must retire
		// the id from every in-flight set here, since no other event will.
		if err := s.scaleDown(r.Context(), id); err != nil {
			log.Printf("scale down error: %v", err)
			http.Error(w, "failed to scale down", http.StatusInternalServerError)
			return
		}
	default:
		log.Printf("webhook ignored: action=%s not handled", event.Action)
	}

	w.WriteHeader(http.StatusOK)
}

func validateHMAC(body []byte, sigHeader, secret string) bool {
	const prefix = "sha256="
	if !strings.HasPrefix(sigHeader, prefix) {
		return false
	}
	provided, err := hex.DecodeString(sigHeader[len(prefix):])
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hmac.Equal(mac.Sum(nil), provided)
}

// hasLabels returns true if every label in required appears in jobLabels (case-insensitive).
func hasLabels(jobLabels, required []string) bool {
	lower := make(map[string]struct{}, len(jobLabels))
	for _, l := range jobLabels {
		lower[strings.ToLower(l)] = struct{}{}
	}
	for _, req := range required {
		if _, ok := lower[req]; !ok {
			return false
		}
	}
	return true
}

func (s *Server) markInProgress(id int64) {
	s.state.mu.Lock()
	delete(s.state.queued, id)
	s.state.inProgress[id] = s.clock()
	queued := len(s.state.queued)
	inProgress := len(s.state.inProgress)
	s.state.mu.Unlock()
	log.Printf("job in progress: id=%d queued=%d inProgress=%d", id, queued, inProgress)
}

func (s *Server) scaleUp(ctx context.Context, id int64) error {
	s.state.mu.Lock()
	s.state.queued[id] = s.clock()
	total := len(s.state.queued) + len(s.state.inProgress) + len(s.state.completed)
	queued := len(s.state.queued)
	inProgress := len(s.state.inProgress)
	completed := len(s.state.completed)
	s.state.mu.Unlock()

	if total == 1 {
		log.Printf("scaled up: replicas=1 (base replica handles first job, id=%d)", id)
		return nil
	}

	if total > s.cfg.MaxRunners {
		log.Printf("at max runners (%d), job %d queued and waiting (queued=%d inProgress=%d completed=%d)",
			s.cfg.MaxRunners, id, queued, inProgress, completed)
		return nil
	}

	if err := s.client.SetReplicas(ctx, total); err != nil {
		return err
	}
	log.Printf("scaled up: replicas=%d (job id=%d, queued=%d inProgress=%d completed=%d)", total, id, queued, inProgress, completed)
	return nil
}

func (s *Server) scaleDown(ctx context.Context, id int64) error {
	s.state.mu.Lock()
	// A job that is cancelled while still queued (e.g. superseded by
	// concurrency.cancel-in-progress) never fires in_progress, so it must be
	// retired from queued here too - otherwise its id is never removed from any
	// set and the queued count leaks upward forever.
	delete(s.state.queued, id)
	delete(s.state.inProgress, id)
	s.state.completed[id] = struct{}{}
	queued := len(s.state.queued)
	inProgress := len(s.state.inProgress)
	s.state.mu.Unlock()

	if inProgress > 0 {
		// Decreasing the replicas while jobs are still in progress can cause them to be killed before completion, so we wait until all in-progress jobs are done before scaling down.
		log.Printf("scaled down: job %d complete, queued=%d inProgress=%d, replicas unchanged", id, queued, inProgress)
		return nil
	}

	next := max(1, min(queued, s.cfg.MaxRunners))
	if err := s.client.SetReplicas(ctx, next); err != nil {
		return err
	}

	// completed jobs are no longer using up inactive replicas we need to count for
	s.state.mu.Lock()
	s.state.completed = make(map[int64]struct{})
	s.state.mu.Unlock()

	if queued == 0 {
		log.Printf("scaled down: all jobs complete, reset to 1 replica")
	} else {
		log.Printf("scaled down: in-progress batch done, resuming %d pending job(s) with %d replica(s)", queued, next)
	}
	return nil
}

// reapLoop periodically calls reapStaleJobs until ctx is cancelled.
func (s *Server) reapLoop(ctx context.Context) {
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reapStaleJobs(ctx)
		}
	}
}

// reapStaleJobs is a defense-in-depth safety net, not the primary leak fix
// (that's the delete-from-queued-in-scaleDown change above). It protects
// against a webhook delivery that is lost entirely - e.g. GitHub retries
// exhausted while the service was down - which would otherwise leak an id
// forever with no terminal event to clean it up. Any queued/inProgress entry
// older than cfg.StaleJobTTL is treated as abandoned and purged; the TTL
// default (6h) is far beyond any realistic job duration on this fleet so it
// never fires during normal operation.
func (s *Server) reapStaleJobs(ctx context.Context) {
	now := s.clock()
	s.state.mu.Lock()
	var reaped []int64
	for id, t := range s.state.queued {
		if now.Sub(t) > s.cfg.StaleJobTTL {
			delete(s.state.queued, id)
			reaped = append(reaped, id)
		}
	}
	for id, t := range s.state.inProgress {
		if now.Sub(t) > s.cfg.StaleJobTTL {
			delete(s.state.inProgress, id)
			reaped = append(reaped, id)
		}
	}
	queued := len(s.state.queued)
	inProgress := len(s.state.inProgress)
	s.state.mu.Unlock()

	if len(reaped) == 0 {
		return
	}
	log.Printf("reaped %d stale job id(s), no terminal webhook received within %s: %v", len(reaped), s.cfg.StaleJobTTL, reaped)

	if inProgress > 0 {
		// still real work tracked as in-flight; leave replicas alone
		return
	}

	next := max(1, min(queued, s.cfg.MaxRunners))
	if err := s.client.SetReplicas(ctx, next); err != nil {
		log.Printf("reap: scale error: %v", err)
		return
	}

	s.state.mu.Lock()
	s.state.completed = make(map[int64]struct{})
	s.state.mu.Unlock()
	log.Printf("reap: replicas set to %d after stale-job cleanup (queued=%d)", next, queued)
}

// railwayClient is the production RailwayClient: it calls Railway's GraphQL
// API using a project-scoped access token.
type railwayClient struct {
	token         string
	serviceID     string
	environmentID string
	baseURL       string
	httpClient    *http.Client
}

func newRailwayClient(cfg Config) *railwayClient {
	return &railwayClient{
		token:         cfg.RailwayToken,
		serviceID:     cfg.ServiceID,
		environmentID: cfg.EnvironmentID,
		baseURL:       railwayGQLURL,
		// An explicit timeout so a hung Railway backend can't block a webhook
		// goroutine indefinitely; http.DefaultClient has none.
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *railwayClient) SetReplicas(ctx context.Context, n int) error {
	const mutation = `
mutation UpdateReplicas($serviceId: String!, $environmentId: String!, $input: ServiceInstanceUpdateInput!) {
  serviceInstanceUpdate(serviceId: $serviceId, environmentId: $environmentId, input: $input)
}`
	return c.gqlDo(ctx, gqlRequest{
		Query: mutation,
		Variables: map[string]any{
			"serviceId":     c.serviceID,
			"environmentId": c.environmentID,
			"input":         map[string]any{"numReplicas": n},
		},
	}, nil)
}

func (c *railwayClient) gqlDo(ctx context.Context, req gqlRequest, out any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	// Project-scoped Railway tokens authenticate via Project-Access-Token, not
	// Authorization: Bearer (that header is for account/workspace/OAuth tokens).
	// This service is deployed with a project token - keep this header as-is.
	httpReq.Header.Set("Project-Access-Token", c.token)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("railway api: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("[ERROR] railway api %d | request: %s | response: %s", resp.StatusCode, body, respBody)
		return fmt.Errorf("railway api returned %d", resp.StatusCode)
	}

	var gqlResp gqlResponse
	if err := json.Unmarshal(respBody, &gqlResp); err != nil {
		log.Printf("[ERROR] railway api unmarshal | request: %s | response: %s", body, respBody)
		return fmt.Errorf("unmarshal response: %w", err)
	}
	if len(gqlResp.Errors) > 0 {
		log.Printf("[ERROR] railway graphql | request: %s | response: %s", body, respBody)
		return fmt.Errorf("railway graphql error: %s", gqlResp.Errors[0].Message)
	}

	if out != nil && gqlResp.Data != nil {
		if err := json.Unmarshal(gqlResp.Data, out); err != nil {
			return fmt.Errorf("unmarshal data: %w", err)
		}
	}
	return nil
}
