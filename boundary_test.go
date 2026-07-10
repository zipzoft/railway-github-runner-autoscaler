package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// --- railwayClient HTTP layer (previously untested) ---

func TestRailwayClient_SetReplicas_SendsTokenAndVariables(t *testing.T) {
	var gotToken, gotContentType string
	var gotBody []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("Project-Access-Token")
		gotContentType = r.Header.Get("Content-Type")
		gotBody, _ = readAll(r)
		_, _ = w.Write([]byte(`{"data":{"serviceInstanceUpdate":true}}`))
	}))
	defer ts.Close()

	c := &railwayClient{token: "tok", serviceID: "svc", environmentID: "env", baseURL: ts.URL, httpClient: ts.Client()}
	if err := c.SetReplicas(context.Background(), 3); err != nil {
		t.Fatalf("SetReplicas: %v", err)
	}

	if gotToken != "tok" {
		t.Errorf("Project-Access-Token = %q, want tok", gotToken)
	}
	if gotContentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", gotContentType)
	}
	var req gqlRequest
	if err := json.Unmarshal(gotBody, &req); err != nil {
		t.Fatalf("unmarshal request body: %v", err)
	}
	if req.Variables["serviceId"] != "svc" {
		t.Errorf("serviceId = %v, want svc", req.Variables["serviceId"])
	}
	if req.Variables["environmentId"] != "env" {
		t.Errorf("environmentId = %v, want env", req.Variables["environmentId"])
	}
	input, ok := req.Variables["input"].(map[string]any)
	if !ok {
		t.Fatalf("input variable missing or wrong type: %v", req.Variables["input"])
	}
	if got := input["numReplicas"]; got != float64(3) {
		t.Errorf("numReplicas = %v, want 3", got)
	}
}

func TestRailwayClient_SetReplicas_NonOKStatusIsError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"boom"}`))
	}))
	defer ts.Close()

	c := &railwayClient{baseURL: ts.URL, httpClient: ts.Client()}
	if err := c.SetReplicas(context.Background(), 1); err == nil {
		t.Fatal("expected an error for a non-200 response")
	}
}

func TestRailwayClient_SetReplicas_GraphQLErrorsAreError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"errors":[{"message":"service not found"}]}`))
	}))
	defer ts.Close()

	c := &railwayClient{baseURL: ts.URL, httpClient: ts.Client()}
	err := c.SetReplicas(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "service not found") {
		t.Fatalf("expected a graphql error mentioning the message, got %v", err)
	}
}

func TestRailwayClient_SetReplicas_MalformedResponseIsError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{not valid json`))
	}))
	defer ts.Close()

	c := &railwayClient{baseURL: ts.URL, httpClient: ts.Client()}
	if err := c.SetReplicas(context.Background(), 1); err == nil {
		t.Fatal("expected an error for a malformed JSON response")
	}
}

func TestNewRailwayClient_HasTimeout(t *testing.T) {
	c := newRailwayClient(Config{})
	if c.httpClient.Timeout == 0 {
		t.Fatal("the railway client must set a non-zero request timeout")
	}
}

func TestRailwayClient_SetReplicas_TimesOut(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte(`{"data":{}}`))
	}))
	defer ts.Close()

	c := &railwayClient{baseURL: ts.URL, httpClient: &http.Client{Timeout: 20 * time.Millisecond}}
	start := time.Now()
	if err := c.SetReplicas(context.Background(), 1); err == nil {
		t.Fatal("expected a timeout error when the backend is slower than the client timeout")
	}
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("client timeout did not bound the call: took %s", elapsed)
	}
}

// --- scale-failure propagation to the handler ---

func TestScaleUp_PropagatesClientError(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	if err := srv.scaleUp(context.Background(), 1); err != nil { // total==1, base replica, no call
		t.Fatalf("scaleUp(1): %v", err)
	}
	client.err = fmt.Errorf("railway down")
	if err := srv.scaleUp(context.Background(), 2); err == nil { // total==2, real SetReplicas
		t.Fatal("expected scaleUp to propagate the client error")
	}
}

func TestScaleDown_PropagatesClientError(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	if err := srv.scaleUp(context.Background(), 1); err != nil {
		t.Fatalf("scaleUp(1): %v", err)
	}
	srv.markInProgress(1)
	client.err = fmt.Errorf("railway down")
	if err := srv.scaleDown(context.Background(), 1); err == nil { // inProgress→0, real SetReplicas
		t.Fatal("expected scaleDown to propagate the client error")
	}
}

func TestHandleWebhook_ScaleFailureReturns500(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	if rec := postWebhook(srv, "queued", 1); rec.Code != http.StatusOK { // base replica, no call
		t.Fatalf("queued(1): status=%d", rec.Code)
	}
	client.err = fmt.Errorf("railway down")
	rec := postWebhook(srv, "queued", 2) // total==2 → SetReplicas fails
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when scaling fails, got %d body=%s", rec.Code, rec.Body)
	}
}

// --- webhook handler validation responses (previously only unit-tested) ---

func TestHandleWebhook_ValidationResponses(t *testing.T) {
	srv, client := newTestServer(6, time.Hour, testClock)
	secret := srv.cfg.WebhookSecret
	sign := func(body []byte) string {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(body)
		return "sha256=" + hex.EncodeToString(mac.Sum(nil))
	}

	t.Run("wrong method is 405", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/webhook", nil)
		rec := httptest.NewRecorder()
		srv.handleWebhook(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status=%d, want 405", rec.Code)
		}
	})

	t.Run("bad signature is 401", func(t *testing.T) {
		body := []byte(`{"action":"queued","workflow_job":{"id":1,"labels":["self-hosted","railway"]}}`)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
		req.Header.Set("X-Hub-Signature-256", "sha256="+strings.Repeat("00", 32))
		rec := httptest.NewRecorder()
		srv.handleWebhook(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status=%d, want 401", rec.Code)
		}
	})

	t.Run("oversize body is 413", func(t *testing.T) {
		big := bytes.Repeat([]byte("a"), maxBodyBytes+1)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(big))
		req.Header.Set("X-Hub-Signature-256", sign(big))
		rec := httptest.NewRecorder()
		srv.handleWebhook(rec, req)
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status=%d, want 413", rec.Code)
		}
	})

	t.Run("invalid JSON is 400", func(t *testing.T) {
		body := []byte(`not json`)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
		req.Header.Set("X-GitHub-Event", "workflow_job")
		req.Header.Set("X-Hub-Signature-256", sign(body))
		rec := httptest.NewRecorder()
		srv.handleWebhook(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status=%d, want 400", rec.Code)
		}
	})

	t.Run("non-workflow_job event is ignored with 200", func(t *testing.T) {
		body := []byte(`{"action":"queued"}`)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
		req.Header.Set("X-GitHub-Event", "push")
		req.Header.Set("X-Hub-Signature-256", sign(body))
		rec := httptest.NewRecorder()
		srv.handleWebhook(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d, want 200", rec.Code)
		}
		if _, ok := client.lastCall(); ok {
			t.Fatal("a non-workflow_job event must not trigger scaling")
		}
	})

	t.Run("label mismatch is ignored with 200", func(t *testing.T) {
		body := []byte(`{"action":"queued","workflow_job":{"id":9,"labels":["ubuntu-latest"]}}`)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
		req.Header.Set("X-GitHub-Event", "workflow_job")
		req.Header.Set("X-Hub-Signature-256", sign(body))
		rec := httptest.NewRecorder()
		srv.handleWebhook(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d, want 200", rec.Code)
		}
		if _, ok := client.lastCall(); ok {
			t.Fatal("a label mismatch must not trigger scaling")
		}
	})
}

func TestHandleHealth(t *testing.T) {
	srv, _ := newTestServer(6, time.Hour, testClock)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	srv.handleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if body := strings.TrimSpace(rec.Body.String()); body != `{"status":"ok"}` {
		t.Errorf("body = %q, want the ok status JSON", body)
	}
}

// readAll drains an *http.Request body for assertions.
func readAll(r *http.Request) ([]byte, error) {
	var buf bytes.Buffer
	_, err := buf.ReadFrom(r.Body)
	return buf.Bytes(), err
}
