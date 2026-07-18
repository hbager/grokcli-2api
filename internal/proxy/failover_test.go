package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/hm2899/grokcli-2api/internal/upstream/grok"
)

func TestOpenWithFailoverRetriesEveryUpstreamHTTPError(t *testing.T) {
	for _, status := range []int{400, 404, 418, 429, 500, 502, 503} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			attempts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts++
				if r.Header.Get("Authorization") == "Bearer bad" {
					w.WriteHeader(status)
					_ = json.NewEncoder(w).Encode(map[string]any{"error": "scripted"})
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("data: [DONE]\n\n"))
			}))
			defer server.Close()

			client := &grok.Client{BaseURL: server.URL + "/v1", HTTP: server.Client()}
			result, err := OpenWithFailover(context.Background(), client, []grok.Account{
				{ID: "bad", Token: "bad"}, {ID: "ok", Token: "ok"},
			}, "grok", map[string]any{}, &CommitState{})
			if err != nil {
				t.Fatal(err)
			}
			defer result.Body.Close()
			if result.Account.ID != "ok" || attempts != 2 {
				t.Fatalf("account=%s attempts=%d", result.Account.ID, attempts)
			}
		})
	}
}

func TestOpenWithFailoverRetriesBeforeCommit(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if r.Header.Get("Authorization") == "Bearer bad" {
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "rate"})
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	client := &grok.Client{BaseURL: server.URL + "/v1", HTTP: server.Client()}
	result, err := OpenWithFailover(context.Background(), client, []grok.Account{
		{ID: "bad", Token: "bad"}, {ID: "ok", Token: "ok"},
	}, "grok", map[string]any{}, &CommitState{})
	if err != nil {
		t.Fatal(err)
	}
	defer result.Body.Close()
	if result.Account.ID != "ok" || attempts != 2 {
		t.Fatalf("account=%s attempts=%d", result.Account.ID, attempts)
	}
}
