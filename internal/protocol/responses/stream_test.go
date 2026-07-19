package responses

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestEmptyCompleteCanStillFail(t *testing.T) {
	stream := NewLiveStreamer("resp", "grok", nil)
	stream.Start()
	if frames := stream.Complete(nil); len(frames) != 0 {
		t.Fatalf("empty complete emitted %#v", frames)
	}
	failed := stream.Fail("empty upstream", "")
	if len(failed) != 2 || !strings.Contains(failed[0], "response.failed") || failed[1] != "data: [DONE]\n\n" {
		t.Fatalf("unexpected failure %#v", failed)
	}
}

func TestToolStreamUsesStableIDsAndMonotonicSequence(t *testing.T) {
	stream := NewLiveStreamer("resp", "grok", []string{"Edit"})
	frames := stream.ToolDeltas([]ToolDelta{{
		Index: 3, ID: "call", Name: "Update",
		Arguments: `{"file_path":"/x","old_string":"a","new_string":""}`,
	}})
	frames = append(frames, stream.Complete(&Usage{InputTokens: 2, OutputTokens: 1})...)
	sequence := 0
	itemID := ""
	for _, frame := range frames {
		if frame == "data: [DONE]\n\n" {
			continue
		}
		parts := strings.SplitN(frame, "data: ", 2)
		var payload map[string]any
		if len(parts) != 2 || json.Unmarshal([]byte(strings.TrimSpace(parts[1])), &payload) != nil {
			t.Fatalf("invalid SSE %q", frame)
		}
		if int(payload["sequence_number"].(float64)) != sequence {
			t.Fatalf("sequence %v want %d", payload, sequence)
		}
		sequence++
		if value, ok := payload["item_id"].(string); ok {
			if itemID == "" {
				itemID = value
			} else if value != itemID {
				t.Fatalf("item id changed %q to %q", itemID, value)
			}
		}
	}
	if itemID == "" {
		t.Fatal("no function item id observed")
	}
}

func TestTextThenToolUsesDistinctOutputIndexes(t *testing.T) {
	stream := NewLiveStreamer("resp", "grok", []string{"lookup"})
	frames := stream.Text("x")
	frames = append(frames, stream.ToolDeltas([]ToolDelta{{
		Index: 0, ID: "call_1", Name: "lookup", Arguments: `{}`,
	}})...)

	indexes := map[string]int{}
	for _, frame := range frames {
		parts := strings.SplitN(frame, "data: ", 2)
		if len(parts) != 2 {
			continue
		}
		var payload map[string]any
		if json.Unmarshal([]byte(strings.TrimSpace(parts[1])), &payload) != nil || payload["type"] != "response.output_item.added" {
			continue
		}
		item, _ := payload["item"].(map[string]any)
		itemType, _ := item["type"].(string)
		indexes[itemType] = int(payload["output_index"].(float64))
	}
	if indexes["message"] != 0 || indexes["function_call"] != 1 {
		t.Fatalf("output indexes=%#v", indexes)
	}
}

func TestReasoningOnlyCompleteClosesEnvelope(t *testing.T) {
	stream := NewLiveStreamer("resp", "grok", nil)
	frames := stream.Reasoning("think")
	frames = append(frames, stream.Complete(&Usage{InputTokens: 1, OutputTokens: 1})...)
	joined := strings.Join(frames, "")
	if !strings.Contains(joined, "response.completed") || !strings.Contains(joined, "data: [DONE]") {
		t.Fatalf("reasoning-only stream missing terminal: %q", joined)
	}
	if !strings.Contains(joined, "reasoning_summary") {
		t.Fatalf("missing reasoning frames: %q", joined)
	}
}

func TestIncompleteToolStillClosesEnvelope(t *testing.T) {
	// Incomplete required fields: do not emit a broken function_call, but still
	// close the envelope so Codex/Claude Code leave "running".
	stream := NewLiveStreamer("resp", "grok", []string{"Bash"})
	_ = stream.Reasoning("planning")
	incomplete := "{\"command\":"
	_ = stream.ToolDeltas([]ToolDelta{
		{Index: 0, ID: "call1", Name: "Bash", Arguments: incomplete},
	})
	frames := stream.Complete(&Usage{InputTokens: 1, OutputTokens: 1})
	joined := strings.Join(frames, "")
	if !strings.Contains(joined, "response.completed") || !strings.Contains(joined, "data: [DONE]") {
		t.Fatalf("expected terminal completed, got %q", joined)
	}
	if strings.Contains(joined, "function_call") && strings.Contains(joined, "Bash") {
		t.Fatalf("incomplete Bash should not emit function_call: %q", joined)
	}
}

func TestOutOfOrderToolsStillEmit(t *testing.T) {
	stream := NewLiveStreamer("resp", "grok", []string{"Bash", "Read"})
	incomplete := "{\"command\":"
	complete := "{\"file_path\":\"/a\"}"
	frames := stream.ToolDeltas([]ToolDelta{
		{Index: 0, ID: "c0", Name: "Bash", Arguments: incomplete},
		{Index: 1, ID: "c1", Name: "Read", Arguments: complete},
	})
	joined := strings.Join(frames, "")
	if !strings.Contains(joined, "function_call") || !strings.Contains(joined, "Read") {
		t.Fatalf("expected ready tool index 1 emitted, got %q", joined)
	}
}

func TestCompletedIncludesFunctionCallOutput(t *testing.T) {
	s := NewLiveStreamerWithMaxTools("resp_x", "grok", []string{"shell"}, 0)
	frames := s.ToolDeltas([]ToolDelta{{Index: 0, ID: "call_1", Name: "shell", Arguments: `{"command":"echo hi"}`}})
	frames = append(frames, s.Complete(&Usage{InputTokens: 1, OutputTokens: 1})...)
	joined := strings.Join(frames, "\n")
	if !strings.Contains(joined, "function_call") {
		t.Fatalf("missing function_call frames: %s", joined)
	}
	// Find response.completed payload and ensure output has the tool.
	found := false
	for _, frame := range frames {
		for _, line := range strings.Split(frame, "\n") {
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			payload := strings.TrimSpace(line[5:])
			if payload == "[DONE]" || payload == "" {
				continue
			}
			var obj map[string]any
			if json.Unmarshal([]byte(payload), &obj) != nil {
				continue
			}
			if obj["type"] != "response.completed" {
				continue
			}
			resp, _ := obj["response"].(map[string]any)
			out, _ := resp["output"].([]any)
			if len(out) == 0 {
				t.Fatalf("completed output empty: %s", payload)
			}
			for _, item := range out {
				m, _ := item.(map[string]any)
				if m["type"] == "function_call" && m["name"] == "shell" {
					found = true
					if m["arguments"] != `{"command":"echo hi"}` && !strings.Contains(fmt.Sprint(m["arguments"]), "echo hi") {
						t.Fatalf("args=%v", m["arguments"])
					}
				}
			}
		}
	}
	if !found {
		t.Fatalf("completed missing shell function_call:\n%s", joined)
	}
}

func TestCompletedDropsNestedEmptyShell(t *testing.T) {
	s := NewLiveStreamerWithMaxTools("resp_y", "grok", []string{"shell"}, 0)
	_ = s.ToolDeltas([]ToolDelta{{Index: 0, ID: "call_bad", Name: "shell", Arguments: `{"command":[[""]]}`}})
	frames := s.Complete(&Usage{})
	joined := strings.Join(frames, "\n")
	// Must not emit a broken shell function_call.
	if strings.Contains(joined, `"name":"shell"`) || strings.Contains(joined, `"name": "shell"`) {
		// allow only if not present as function_call item; strict: fail if function_call with shell
		if strings.Contains(joined, "function_call") && strings.Contains(joined, "shell") {
			t.Fatalf("empty nested shell should be dropped:\n%s", joined)
		}
	}
}

func TestShellArgsProjectedToCmdForCodex(t *testing.T) {
	s := NewLiveStreamerWithMaxTools("resp_cmd", "grok", []string{"shell"}, 0)
	s.SetShellArgKeys(map[string]string{"shell": "cmd"})
	frames := s.ToolDeltas([]ToolDelta{{Index: 0, ID: "call_1", Name: "shell", Arguments: `{"command":"echo hi"}`}})
	frames = append(frames, s.Complete(&Usage{InputTokens: 1, OutputTokens: 1})...)
	joined := strings.Join(frames, "\n")
	if !strings.Contains(joined, `"cmd"`) && !strings.Contains(joined, `"cmd":`) {
		// JSON may compact as {"cmd":"echo hi"}
		if !strings.Contains(joined, "cmd") {
			t.Fatalf("expected cmd in frames:\n%s", joined)
		}
	}
	// Must not leave only command key for Codex-preferring clients.
	// Allow "command" only if both appear; require cmd present in completed args.
	foundCmd := false
	for _, frame := range frames {
		for _, line := range strings.Split(frame, "\n") {
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			payload := strings.TrimSpace(line[5:])
			if payload == "" || payload == "[DONE]" {
				continue
			}
			var obj map[string]any
			if json.Unmarshal([]byte(payload), &obj) != nil {
				continue
			}
			if obj["type"] == "response.function_call_arguments.done" {
				args := fmt.Sprint(obj["arguments"])
				if strings.Contains(args, `"cmd"`) || strings.Contains(args, `"cmd":`) || (strings.Contains(args, "cmd") && strings.Contains(args, "echo hi")) {
					foundCmd = true
				}
				if strings.Contains(args, `"command"`) && !strings.Contains(args, `"cmd"`) {
					t.Fatalf("Codex client got command instead of cmd: %s", args)
				}
			}
			item, _ := obj["item"].(map[string]any)
			if item != nil && item["type"] == "function_call" && item["status"] == "completed" {
				args := fmt.Sprint(item["arguments"])
				if strings.Contains(args, "cmd") {
					foundCmd = true
				}
			}
		}
	}
	if !foundCmd {
		t.Fatalf("cmd not found in tool args:\n%s", joined)
	}
}

func TestShellArgsDefaultCmdWithoutKeyMap(t *testing.T) {
	// No SetShellArgKeys: shell-family tools must still project command→cmd for Codex.
	s := NewLiveStreamerWithMaxTools("resp_cmd_default", "grok", []string{"Shell"}, 0)
	frames := s.ToolDeltas([]ToolDelta{{Index: 0, ID: "call_1", Name: "Shell", Arguments: `{"command":"pwd"}`}})
	frames = append(frames, s.Complete(&Usage{InputTokens: 1, OutputTokens: 1})...)
	foundCmd := false
	for _, frame := range frames {
		for _, line := range strings.Split(frame, "\n") {
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			payload := strings.TrimSpace(line[5:])
			var obj map[string]any
			if json.Unmarshal([]byte(payload), &obj) != nil {
				continue
			}
			if obj["type"] == "response.function_call_arguments.done" {
				args := fmt.Sprint(obj["arguments"])
				if strings.Contains(args, `"command"`) && !strings.Contains(args, `"cmd"`) {
					t.Fatalf("default shell projection still command: %s", args)
				}
				if strings.Contains(args, `"cmd"`) {
					foundCmd = true
				}
			}
			item, _ := obj["item"].(map[string]any)
			if item != nil && item["type"] == "function_call" && item["status"] == "completed" {
				args := fmt.Sprint(item["arguments"])
				if strings.Contains(args, `"command"`) && !strings.Contains(args, `"cmd"`) {
					t.Fatalf("completed item still command: %s", args)
				}
				if strings.Contains(args, `"cmd"`) {
					foundCmd = true
				}
			}
		}
	}
	if !foundCmd {
		t.Fatalf("expected default cmd projection:\n%s", strings.Join(frames, "\n"))
	}
}

func TestExecCommandArgsProjectedToCmd(t *testing.T) {
	// Codex get_goal/exec_command path: tool name is exec_command, schema wants cmd.
	s := NewLiveStreamerWithMaxTools("resp_exec", "grok", []string{"exec_command"}, 0)
	s.SetShellArgKeys(map[string]string{"exec_command": "cmd", "execcommand": "cmd"})
	frames := s.ToolDeltas([]ToolDelta{{Index: 0, ID: "call_1", Name: "exec_command", Arguments: `{"command":"pwd"}`}})
	frames = append(frames, s.Complete(&Usage{InputTokens: 1, OutputTokens: 1})...)
	found := false
	for _, frame := range frames {
		for _, line := range strings.Split(frame, "\n") {
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			var obj map[string]any
			if json.Unmarshal([]byte(strings.TrimSpace(line[5:])), &obj) != nil {
				continue
			}
			if obj["type"] == "response.function_call_arguments.done" {
				args := fmt.Sprint(obj["arguments"])
				if strings.Contains(args, `"command"`) && !strings.Contains(args, `"cmd"`) {
					t.Fatalf("exec_command still command: %s", args)
				}
				if strings.Contains(args, `"cmd"`) {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatalf("exec_command cmd projection missing:\n%s", strings.Join(frames, "\n"))
	}
}

func TestExecCommandDefaultProjectionWithoutKeyMap(t *testing.T) {
	s := NewLiveStreamerWithMaxTools("resp_exec2", "grok", []string{"exec_command"}, 0)
	// No SetShellArgKeys — IsShellTool(exec_command) + default cmd.
	frames := s.ToolDeltas([]ToolDelta{{Index: 0, ID: "c", Name: "exec_command", Arguments: `{"command":"ls"}`}})
	frames = append(frames, s.Complete(&Usage{})...)
	joined := strings.Join(frames, "\n")
	if strings.Contains(joined, `"command":"ls"`) && !strings.Contains(joined, `"cmd"`) {
		t.Fatalf("default exec_command projection failed:\n%s", joined)
	}
	if !strings.Contains(joined, "cmd") {
		t.Fatalf("expected cmd somewhere:\n%s", joined)
	}
}

func TestHasPendingTools(t *testing.T) {
	s := NewLiveStreamerWithMaxTools("resp_pend", "grok", []string{"exec_command"}, 0)
	if s.HasPendingTools() {
		t.Fatal("empty streamer should not be pending")
	}
	// Incomplete JSON → buffered, not emitted.
	_ = s.ToolDeltas([]ToolDelta{{Index: 0, ID: "c", Name: "exec_command", Arguments: `{"cmd":`}})
	if !s.HasPendingTools() {
		t.Fatal("incomplete tool should be pending")
	}
	// Complete it → emitted, no longer pending.
	_ = s.ToolDeltas([]ToolDelta{{Index: 0, Arguments: `"pwd"}`}})
	if s.HasPendingTools() {
		t.Fatal("complete tool should not stay pending")
	}
}

func TestExecCommandFullPathProjectsCmdAlways(t *testing.T) {
	// Full path: EffectiveJSON(command) -> streamer ToolDeltas -> completed args must be cmd.
	for _, tool := range []string{"exec_command", "Shell", "default_api.exec_command"} {
		raw := `{"command":"pwd"}`
		// Streamer receives already-normalized internal args in production too.
		s := NewLiveStreamerWithMaxTools("resp_full", "grok", []string{tool}, 0)
		// Explicitly empty key map — must still default to cmd.
		s.SetShellArgKeys(map[string]string{})
		frames := s.ToolDeltas([]ToolDelta{{Index: 0, ID: "c1", Name: tool, Arguments: raw}})
		frames = append(frames, s.Complete(&Usage{InputTokens: 1, OutputTokens: 1})...)
		foundCmd, foundCommandOnly := false, false
		for _, frame := range frames {
			for _, line := range strings.Split(frame, "\n") {
				if !strings.HasPrefix(line, "data:") {
					continue
				}
				payload := strings.TrimSpace(line[5:])
				var obj map[string]any
				if json.Unmarshal([]byte(payload), &obj) != nil {
					continue
				}
				if obj["type"] == "response.function_call_arguments.done" {
					args := fmt.Sprint(obj["arguments"])
					if strings.Contains(args, `"cmd"`) {
						foundCmd = true
					}
					if strings.Contains(args, `"command"`) && !strings.Contains(args, `"cmd"`) {
						foundCommandOnly = true
					}
				}
				if item, _ := obj["item"].(map[string]any); item != nil && item["type"] == "function_call" && item["status"] == "completed" {
					args := fmt.Sprint(item["arguments"])
					if strings.Contains(args, `"cmd"`) {
						foundCmd = true
					}
					if strings.Contains(args, `"command"`) && !strings.Contains(args, `"cmd"`) {
						foundCommandOnly = true
					}
				}
			}
		}
		if !foundCmd || foundCommandOnly {
			t.Fatalf("tool=%s foundCmd=%v commandOnly=%v frames=\n%s", tool, foundCmd, foundCommandOnly, strings.Join(frames, "\n"))
		}
	}
}
