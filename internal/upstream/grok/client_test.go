package grok

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenForcesStreamAndCompatibilityHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for name, want := range map[string]string{
			"Authorization": "Bearer token", "X-Xai-Token-Auth": "xai-grok-cli",
			"X-Grok-Model-Override": "grok-4.5", "X-Grok-Client-Version": "0.2.93",
		} {
			if got := r.Header.Get(name); got != want {
				t.Errorf("%s=%q want %q", name, got, want)
			}
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["stream"] != true {
			t.Fatalf("stream not forced: %#v", body)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	client := &Client{BaseURL: server.URL + "/v1", HTTP: server.Client()}
	response, err := client.Open(context.Background(), Account{ID: "a", Token: "token"}, "grok-4.5", map[string]any{"stream": false})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
}

type countingSSEReader struct {
	reads atomic.Int32
}

func (r *countingSSEReader) Read(p []byte) (int, error) {
	read := r.reads.Add(1)
	if read > 1 {
		time.Sleep(200 * time.Millisecond)
		return 0, io.EOF
	}
	return copy(p, "data: {\"x\":1}\n\n"), nil
}

func TestReadSSEWithIdleStopsProducerWhenEmitReturns(t *testing.T) {
	reader := &countingSSEReader{}
	stop := errors.New("stop")
	err := ReadSSEWithIdle(reader, time.Second, func(Event) error {
		return stop
	}, func() error { return nil })
	if !errors.Is(err, stop) {
		t.Fatalf("error = %v", err)
	}
	time.Sleep(25 * time.Millisecond)
	if got := reader.reads.Load(); got != 1 {
		t.Fatalf("producer continued reading after consumer stopped: reads=%d", got)
	}
}

func TestReadSSE(t *testing.T) {
	var events []Event
	err := ReadSSE(strings.NewReader("data: {\"x\":1}\n\ndata: [DONE]\n\n"), func(event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil || len(events) != 2 || events[0].Done || !events[1].Done {
		t.Fatalf("events=%#v err=%v", events, err)
	}
}
