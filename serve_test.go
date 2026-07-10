package main

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestServeListener_ReturnsOnContextCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	hs := &http.Server{Handler: http.NewServeMux()}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- serveListener(ctx, hs, ln) }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected a clean shutdown, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serveListener did not return after context cancel")
	}
}

func TestServeListener_DrainsInFlightRequestOnShutdown(t *testing.T) {
	reqStarted := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		close(reqStarted)
		time.Sleep(150 * time.Millisecond) // still running when shutdown is triggered
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("done"))
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	hs := &http.Server{Handler: mux}
	ctx, cancel := context.WithCancel(context.Background())

	serveDone := make(chan error, 1)
	go func() { serveDone <- serveListener(ctx, hs, ln) }()

	url := "http://" + ln.Addr().String() + "/slow"
	respCh := make(chan *http.Response, 1)
	getErr := make(chan error, 1)
	go func() {
		resp, err := http.Get(url)
		if err != nil {
			getErr <- err
			return
		}
		respCh <- resp
	}()

	<-reqStarted // the request is now in-flight in the handler
	cancel()     // trigger graceful shutdown mid-request

	select {
	case resp := <-respCh:
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("in-flight request should drain with 200, got %d", resp.StatusCode)
		}
		_ = resp.Body.Close()
	case err := <-getErr:
		t.Fatalf("in-flight request was dropped instead of drained: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight request never completed")
	}

	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("serveListener returned an error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serveListener did not return after shutdown")
	}
}
