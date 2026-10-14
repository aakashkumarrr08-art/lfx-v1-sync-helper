// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"
)

type v1TestRequest struct {
	Method      string
	Path        string
	ContentType string
	Body        map[string]any
}

// syncTestHarness shares the existing CAS-capable fakeMappingStore with a real
// HTTP client/server pair. Tests using it must not run in parallel: production
// dependencies are package globals, restored by Cleanup after each test.
type syncTestHarness struct {
	store    *fakeMappingStore
	mu       sync.Mutex
	requests []v1TestRequest
}

func setupSyncTest(t *testing.T, status int, response string) *syncTestHarness {
	t.Helper()
	h := &syncTestHarness{store: newFakeMappingStore()}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := v1TestRequest{Method: r.Method, Path: r.URL.Path, ContentType: r.Header.Get("Content-Type")}
		if r.Method != http.MethodDelete {
			if err := json.NewDecoder(r.Body).Decode(&request.Body); err != nil {
				t.Errorf("decode v1 request: %v", err)
			}
		} else if body, err := io.ReadAll(r.Body); err != nil || len(body) != 0 {
			t.Errorf("DELETE body = %q, error = %v; want empty body", body, err)
		}
		h.mu.Lock()
		h.requests = append(h.requests, request)
		h.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if response != "" {
			_, _ = io.WriteString(w, response)
		}
	}))
	t.Cleanup(srv.Close)
	gateway, err := url.Parse(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	origCfg, origClient, origStore := cfg, v1HTTPClient, mappingStore
	origGetBackoff, origPutBackoff := mappingGetInitialBackoff, mappingPutInitialBackoff
	cfg = &Config{LFXAPIGateway: gateway}
	v1HTTPClient = srv.Client()
	v1HTTPClient.Timeout = 5 * time.Second
	mappingStore = h.store
	mappingGetInitialBackoff, mappingPutInitialBackoff = time.Microsecond, time.Microsecond
	t.Cleanup(func() {
		cfg, v1HTTPClient, mappingStore = origCfg, origClient, origStore
		mappingGetInitialBackoff, mappingPutInitialBackoff = origGetBackoff, origPutBackoff
	})
	return h
}

func (h *syncTestHarness) recordedRequests() []v1TestRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]v1TestRequest(nil), h.requests...)
}

func (h *syncTestHarness) seed(t *testing.T, key, value string) {
	t.Helper()
	if _, err := h.store.Put(t.Context(), key, []byte(value)); err != nil {
		t.Fatal(err)
	}
}

func (h *syncTestHarness) assertMapping(t *testing.T, key, want string) {
	t.Helper()
	entry, err := h.store.Get(t.Context(), key)
	if err != nil || string(entry.Value) != want {
		t.Errorf("mapping %s = %q, error = %v; want %q", key, entry.Value, err, want)
	}
}
