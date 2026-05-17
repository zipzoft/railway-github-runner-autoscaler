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

type getReplResponse struct {
	Service struct {
		ServiceInstances struct {
			Edges []struct {
				Node struct {
					NumReplicas int `json:"numReplicas"`
				} `json:"node"`
			} `json:"edges"`
		} `json:"serviceInstances"`
	} `json:"service"`
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

	switch event.Action {
	case "queued":
		if err := s.scaleUp(r.Context()); err != nil {
			log.Printf("scale up error: %v", err)
			http.Error(w, "failed to scale up", http.StatusInternalServerError)
			return
		}
	case "completed":
		if err := s.scaleDown(r.Context()); err != nil {
			log.Printf("scale down error: %v", err)
			http.Error(w, "failed to scale down", http.StatusInternalServerError)
			return
		}
	default:
		log.Printf("webhook ignored: action=%s not handled", event.Action)
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	actual, err := s.getReplicas(r.Context())
	if err != nil {
		log.Printf("sync error: %v", err)
		http.Error(w, `{"error":"failed to query Railway"}`, http.StatusBadGateway)
		return
	}

	s.state.mu.Lock()
	s.state.count = actual
	s.state.mu.Unlock()

	log.Printf("synced: replicas=%d", actual)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"replicas":%d,"synced":true}`, actual)
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

func (s *Server) scaleUp(ctx context.Context) error {
	s.state.mu.Lock()
	newCount := s.state.count + 1
	if newCount > s.cfg.MaxRunners {
		s.state.mu.Unlock()
		log.Printf("at max runners (%d), not scaling up", s.cfg.MaxRunners)
		return nil
	}
	s.state.count = newCount
	s.state.mu.Unlock()

	if newCount == 1 {
		// Base replica is already running — no API call needed.
		log.Printf("scaled up: replicas=1 (base replica handles first job)")
		return nil
	}

	if err := s.setReplicas(ctx, newCount); err != nil {
		s.state.mu.Lock()
		s.state.count--
		s.state.mu.Unlock()
		return err
	}
	log.Printf("scaled up: replicas=%d", newCount)
	return nil
}

func (s *Server) scaleDown(ctx context.Context) error {
	s.state.mu.Lock()
	prev := s.state.count
	newCount := prev - 1
	if newCount < 0 {
		newCount = 0
	}
	s.state.count = newCount
	s.state.mu.Unlock()

	if prev == 0 {
		log.Printf("scaled down: already at 0, ignoring")
		return nil
	}

	if newCount == 0 {
		if err := s.setReplicas(ctx, 1); err != nil {
			s.state.mu.Lock()
			s.state.count++
			s.state.mu.Unlock()
			return err
		}
		log.Printf("scaled down: all jobs complete, reset to 1 replica")
		return nil
	}

	log.Printf("scaled down: %d job(s) still running", newCount)
	return nil
}

func (s *Server) setReplicas(ctx context.Context, n int) error {
	const mutation = `
mutation UpdateReplicas($serviceId: String!, $environmentId: String!, $input: ServiceInstanceUpdateInput!) {
  serviceInstanceUpdate(serviceId: $serviceId, environmentId: $environmentId, input: $input)
}`
	return s.gqlDo(ctx, gqlRequest{
		Query: mutation,
		Variables: map[string]any{
			"serviceId":     s.cfg.ServiceID,
			"environmentId": s.cfg.EnvironmentID,
			"input":         map[string]any{"numReplicas": n},
		},
	}, nil)
}

func (s *Server) getReplicas(ctx context.Context) (int, error) {
	const query = `
query GetReplicas($serviceId: String!) {
  service(id: $serviceId) {
    serviceInstances { edges { node { numReplicas } } }
  }
}`
	var data getReplResponse
	if err := s.gqlDo(ctx, gqlRequest{
		Query:     query,
		Variables: map[string]any{"serviceId": s.cfg.ServiceID},
	}, &data); err != nil {
		return 0, err
	}
	edges := data.Service.ServiceInstances.Edges
	if len(edges) == 0 {
		return 0, nil
	}
	return edges[0].Node.NumReplicas, nil
}

func (s *Server) gqlDo(ctx context.Context, req gqlRequest, out any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, railwayGQLURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+s.cfg.RailwayToken)

	resp, err := http.DefaultClient.Do(httpReq)
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
