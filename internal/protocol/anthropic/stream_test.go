package anthropic

import (
	"strings"
	"testing"
)

func TestStreamEmitsNormalizedEditArgs(t *testing.T) {
	// Grok path/search/replace aliases must become Claude Edit keys in partial_json.
	assembler := NewStreamAssembler("m", "g", true, 1, []string{"Edit"})
	frames := assembler.Feed("", "", []ToolDelta{{
		Index: 0, ID: "t", Name: "Update",
		Arguments: `{"path":"/x.go","search":"old","replace":"new"}`,
	}})
	frames = append(frames, assembler.Finish("tool_calls", Usage{})...)
	joined := strings.Join(frames, "")
	// partial_json is JSON-encoded inside the SSE data frame, so keys appear escaped.
	for _, marker := range []string{`"type":"tool_use"`, `"name":"Edit"`, `\"file_path\"`, `\"old_string\"`, `\"new_string\"`, "message_stop"} {
		if !strings.Contains(joined, marker) {
			t.Fatalf("missing %q in %s", marker, joined)
		}
	}
	if !strings.Contains(joined, `\"file_path\":\"/x.go\"`) {
		t.Fatalf("expected rewritten file_path in partial_json: %s", joined)
	}
}

func TestStreamIncompleteToolKeepsTerminalWithoutToolBlock(t *testing.T) {
	assembler := NewStreamAssembler("m", "g", true, 1, []string{"Edit"})
	frames := assembler.Feed("", "", []ToolDelta{{Index: 0, ID: "t", Name: "Update", Arguments: `{"file_path":"/x"}`}})
	frames = append(frames, assembler.Finish("tool_calls", Usage{})...)
	events := ParseEvents(frames)
	sawTool, sawStop := false, false
	for _, payload := range events {
		if payload["type"] == "content_block_start" {
			block, _ := payload["content_block"].(map[string]any)
			if block["type"] == "tool_use" {
				sawTool = true
			}
		}
		if payload["type"] == "message_stop" {
			sawStop = true
		}
	}
	if sawTool || !sawStop {
		t.Fatalf("tool=%v stop=%v events=%#v", sawTool, sawStop, events)
	}
}

func TestStreamCompleteUpdateIsDenseEditTool(t *testing.T) {
	assembler := NewStreamAssembler("m", "g", true, 1, []string{"Edit"})
	frames := assembler.Feed("preface", "", []ToolDelta{{
		Index: 2, ID: "t", Name: "Update",
		Arguments: `{"file_path":"/x","old_string":"a","new_string":""}`,
	}})
	frames = append(frames, assembler.Finish("tool_calls", Usage{PromptTokens: 2, CompletionTokens: 1})...)
	events := ParseEvents(frames)
	toolIndex := -1
	stopReason := ""
	for _, payload := range events {
		if payload["type"] == "content_block_start" {
			block, _ := payload["content_block"].(map[string]any)
			if block["type"] == "tool_use" {
				toolIndex = int(payload["index"].(float64))
				if block["name"] != "Edit" {
					t.Fatalf("unexpected block %#v", block)
				}
			}
		}
		if payload["type"] == "message_delta" {
			delta := payload["delta"].(map[string]any)
			stopReason, _ = delta["stop_reason"].(string)
		}
	}
	if toolIndex != 0 || stopReason != "tool_use" {
		t.Fatalf("index=%d stop=%q events=%#v", toolIndex, stopReason, events)
	}
}

func TestOutOfOrderAnthropicToolsStillEmit(t *testing.T) {
	assembler := NewStreamAssembler("m", "g", true, 2, []string{"Bash", "Read"})
	// Incomplete tool at index 0 should not block complete tool at index 1.
	incomplete := "{\"command\":"
	complete := "{\"file_path\":\"/a\"}"
	frames := assembler.Feed("", "", []ToolDelta{
		{Index: 0, ID: "t0", Name: "Bash", Arguments: incomplete},
		{Index: 1, ID: "t1", Name: "Read", Arguments: complete},
	})
	frames = append(frames, assembler.Finish("tool_calls", Usage{})...)
	events := ParseEvents(frames)
	sawRead, sawStop := false, false
	for _, payload := range events {
		if payload["type"] == "content_block_start" {
			block, _ := payload["content_block"].(map[string]any)
			if block["type"] == "tool_use" && block["name"] == "Read" {
				sawRead = true
			}
		}
		if payload["type"] == "message_stop" {
			sawStop = true
		}
	}
	if !sawRead || !sawStop {
		t.Fatalf("read=%v stop=%v events=%#v", sawRead, sawStop, events)
	}
}

func TestStreamLiveReasoningWhileToolsPending(t *testing.T) {
	// toolsRequested holds TEXT but must stream REASONING live so long thinking
	// turns keep the SSE pipe warm (~60s idle kills otherwise).
	assembler := NewStreamAssembler("m", "g", true, 1, []string{"Read"})
	frames := assembler.Feed("", "thinking…", nil)
	events := ParseEvents(frames)
	sawThinking := false
	for _, payload := range events {
		if payload["type"] == "content_block_delta" {
			delta, _ := payload["delta"].(map[string]any)
			if delta["type"] == "thinking_delta" {
				sawThinking = true
			}
		}
	}
	if !sawThinking {
		t.Fatalf("expected live thinking_delta under toolsRequested, events=%#v", events)
	}
	// Text still held until Finish or tool.
	frames2 := assembler.Feed("hello", "", nil)
	for _, payload := range ParseEvents(frames2) {
		if payload["type"] == "content_block_delta" {
			delta, _ := payload["delta"].(map[string]any)
			if delta["type"] == "text_delta" {
				t.Fatalf("text should be held before tools/finish: %#v", payload)
			}
		}
	}
	// Finish without tools flushes held text.
	frames3 := assembler.Finish("stop", Usage{})
	joined := ""
	for _, f := range frames3 {
		joined += f
	}
	if !contains(joined, "hello") || !contains(joined, "message_stop") {
		t.Fatalf("expected flushed text + stop, got %q", joined)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		(func() bool {
			for i := 0; i+len(sub) <= len(s); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
			return false
		})())
}

func TestLongThinkingThenMultiToolsClosesCleanly(t *testing.T) {
	// Simulates Claude Code: long thinking stream with tools declared, then
	// multi-tool finish. Reasoning must be live; text held; tools dense; always
	// terminal message_stop.
	a := NewStreamAssembler("msg_long", "grok-4.5", true, 2, []string{"Read", "Bash", "Edit"})

	// Phase 1: long thinking (many chunks) — must emit thinking_delta live.
	var live []string
	for i := 0; i < 40; i++ {
		live = append(live, a.Feed("", "think-chunk-", nil)...)
	}
	liveJoined := strings.Join(live, "")
	if !strings.Contains(liveJoined, "thinking_delta") {
		t.Fatalf("long thinking produced no live thinking_delta: %q", liveJoined[:min(200, len(liveJoined))])
	}
	// Held text should not appear yet.
	heldText := a.Feed("preface that must wait", "", nil)
	for _, f := range heldText {
		if strings.Contains(f, "text_delta") {
			t.Fatalf("text leaked before tools/finish: %q", f)
		}
	}

	// Phase 2: out-of-order tools — incomplete Bash@0, complete Read@1, complete Edit@2
	// maxTools=2 so only first two complete/forced slots emit.
	incompleteBash := `{"command":`
	completeRead := `{"file_path":"/a.go"}`
	completeEdit := `{"file_path":"/b.go","old_string":"x","new_string":"y"}`
	toolFrames := a.Feed("", "", []ToolDelta{
		{Index: 0, ID: "t0", Name: "Bash", Arguments: incompleteBash}, // incomplete
		{Index: 1, ID: "t1", Name: "Read", Arguments: completeRead},
		{Index: 2, ID: "t2", Name: "Edit", Arguments: completeEdit},
	})
	// Finish must close envelope even with incomplete Bash.
	all := append(toolFrames, a.Finish("tool_calls", Usage{PromptTokens: 10, CompletionTokens: 5})...)
	events := ParseEvents(all)

	var tools []string
	sawStop, sawDelta := false, false
	for _, ev := range events {
		switch ev["type"] {
		case "content_block_start":
			block, _ := ev["content_block"].(map[string]any)
			if block["type"] == "tool_use" {
				if name, _ := block["name"].(string); name != "" {
					tools = append(tools, name)
				}
			}
		case "message_delta":
			sawDelta = true
		case "message_stop":
			sawStop = true
		}
	}
	if !sawStop || !sawDelta {
		t.Fatalf("missing terminal delta/stop tools=%v events=%#v", tools, events)
	}
	// Read must have been emitted (complete, not blocked by incomplete Bash@0).
	foundRead := false
	for _, n := range tools {
		if n == "Read" {
			foundRead = true
		}
	}
	if !foundRead {
		t.Fatalf("Read tool blocked by incomplete earlier index; tools=%v", tools)
	}
	// Incomplete Bash must NOT be forced as tool_use (contract).
	for _, n := range tools {
		if n == "Bash" {
			t.Fatalf("incomplete Bash should not emit tool_use; tools=%v", tools)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
