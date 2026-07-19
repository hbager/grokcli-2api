package redis

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMarkInflightDoesNotRetryAmbiguousPipeline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var incrementCommands atomic.Int64
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go func(connection net.Conn) {
				defer connection.Close()
				_ = connection.SetDeadline(time.Now().Add(time.Second))
				buffer := make([]byte, 4096)
				n, _ := connection.Read(buffer)
				if strings.Contains(strings.ToUpper(string(buffer[:n])), "INCR") {
					incrementCommands.Add(1)
				}
				_, _ = connection.Write([]byte(":1\r\n"))
			}(connection)
		}
	}()

	client := New("redis://"+listener.Addr().String()+"/0", "ambiguous")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := client.MarkInflight(ctx, "account-1", 90); err == nil {
		t.Fatal("ambiguous pipeline unexpectedly retried and succeeded")
	}
	deadline := time.Now().Add(250 * time.Millisecond)
	for incrementCommands.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := incrementCommands.Load(); got != 1 {
		t.Fatalf("INCR commands=%d want 1", got)
	}
}
