package anthropic

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/hm2899/grokcli-2api/internal/protocol/toolcall"
)

type ToolDelta struct {
	Index     int
	ID        string
	Name      string
	Arguments string
}

type toolState struct {
	id        string
	name      string
	arguments string
	block     int
	started   bool
	stopped   bool
}

// StreamAssembler converts chat-completion deltas into an Anthropic event
// sequence while preserving dense block indexes and one active tool block.
type StreamAssembler struct {
	messageID string
	model     string
	allowed   []string
	maxTools  int

	started        bool
	nextBlock      int
	textBlock      int
	thinkingBlock  int
	tools          map[int]*toolState
	toolsStarted   int
	sawTool        bool
	toolsRequested bool
	held           []heldDelta
	outputRunes    int
}

type heldDelta struct {
	content   string
	reasoning string
}

func NewStreamAssembler(messageID, model string, toolsRequested bool, maxTools int, allowed []string) *StreamAssembler {
	return &StreamAssembler{
		messageID:      messageID,
		model:          model,
		allowed:        append([]string(nil), allowed...),
		maxTools:       maxTools,
		textBlock:      -1,
		thinkingBlock:  -1,
		tools:          make(map[int]*toolState),
		toolsRequested: toolsRequested,
	}
}

func (s *StreamAssembler) Start(inputTokens int) []string {
	if s.started {
		return nil
	}
	s.started = true
	return []string{event("message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id": s.messageID, "type": "message", "role": "assistant",
			"content": []any{}, "model": s.model,
			"stop_reason": nil, "stop_sequence": nil,
			"usage": map[string]any{
				"input_tokens": inputTokens, "output_tokens": 0,
				"cache_creation_input_tokens": 0, "cache_read_input_tokens": 0,
			},
		},
	})}
}

func (s *StreamAssembler) Feed(content, reasoning string, calls []ToolDelta) []string {
	frames := s.Start(0)
	// When tools are declared, keep TEXT held until a tool arrives / Finish.
	// REASONING must stream live so long Claude Code thinking turns keep the
	// SSE pipe warm (idle proxies ~60s otherwise cut the connection).
	if s.toolsRequested && !s.sawTool {
		if content != "" {
			s.held = append(s.held, heldDelta{content: content})
			s.outputRunes += len([]rune(content))
			content = ""
		}
		// reasoning falls through to emitText below (live)
	} else if s.toolsRequested && s.sawTool {
		// After first tool, drop further text/reasoning for this turn (tool-only).
		content, reasoning = "", ""
	}
	if content != "" || reasoning != "" {
		frames = append(frames, s.emitText(reasoning, content)...)
	}
	if len(calls) == 0 {
		return frames
	}
	frames = append(frames, s.closeThinking()...)
	frames = append(frames, s.closeText()...)
	for _, call := range calls {
		state := s.tools[call.Index]
		if state == nil {
			id := call.ID
			if id == "" {
				id = fmt.Sprintf("toolu_go_%d", call.Index)
			}
			state = &toolState{id: id, block: -1}
			s.tools[call.Index] = state
		}
		if state.stopped {
			continue
		}
		if state.id == "" && call.ID != "" {
			state.id = call.ID
		}
		if call.Name != "" {
			state.name = mergeName(state.name, call.Name)
			state.name = toolcall.CanonicalName(state.name, s.allowed)
		}
		if call.Arguments != "" {
			state.arguments = toolcall.Merge(state.arguments, call.Arguments, state.name)
		}
	}
	frames = append(frames, s.emitReadyTools()...)
	return frames
}

// HasClientPayload reports whether any user-visible content was or will be
// emitted: text, thinking, held text, or tool_use. Envelope-only message_start
// is NOT a client payload (empty upstream must still fail).
func (s *StreamAssembler) HasClientPayload() bool {
	if s == nil {
		return false
	}
	if s.sawTool || s.toolsStarted > 0 || s.outputRunes > 0 {
		return true
	}
	if s.textBlock >= 0 || s.thinkingBlock >= 0 {
		return true
	}
	if len(s.held) > 0 {
		return true
	}
	for _, state := range s.tools {
		if state == nil {
			continue
		}
		if state.started || state.name != "" || strings.TrimSpace(state.arguments) != "" {
			return true
		}
	}
	return false
}

// HasPendingTools reports buffered but not-yet-emitted tool arguments.
func (s *StreamAssembler) HasPendingTools() bool {
	if s == nil {
		return false
	}
	for _, state := range s.tools {
		if state == nil || state.started || state.stopped {
			continue
		}
		if state.name != "" || strings.TrimSpace(state.arguments) != "" {
			return true
		}
	}
	return false
}

// HasHeldContent reports text held for tool-only turns (toolsRequested && !sawTool).
// Upstream may still be streaming held text without reasoning — client sees silence
// and reverse proxies cut Claude Code mid-turn. Callers should force SSE keepalive.
func (s *StreamAssembler) HasHeldContent() bool {
	if s == nil {
		return false
	}
	return len(s.held) > 0
}

// NeedsClientKeepalive is true when the assembler is buffering work the client
// cannot yet see (held text and/or incomplete tools).
func (s *StreamAssembler) NeedsClientKeepalive() bool {
	return s.HasHeldContent() || s.HasPendingTools()
}

func (s *StreamAssembler) Finish(finishReason string, usage Usage) []string {
	frames := s.Start(usage.PromptTokens)
	for _, state := range s.tools {
		if state.started || state.stopped {
			continue
		}
		// Name may have arrived late; ensure CanonicalName + edit aliases re-apply.
		if state.name != "" {
			state.name = toolcall.CanonicalName(state.name, s.allowed)
		}
		if state.name == "" {
			continue
		}
		// Force-finish recovery: trailing junk / mild truncation / late Update→Edit
		// rename should not drop otherwise-valid tools intermittently.
		state.arguments = toolcall.CoerceCompleteJSON(state.arguments, state.name)
		// If still incomplete, retry under Edit schema (Update→Edit rename race:
		// name may have been Update when first args arrived; after CanonicalName
		// it's Edit and edit-only aliases/defaults must re-apply).
		if !toolcall.CompleteJSON(state.arguments, state.name) {
			for _, tryName := range []string{
				toolcall.CanonicalName("Edit", s.allowed),
				toolcall.CanonicalName("Update", s.allowed),
				"Edit",
			} {
				tryName = strings.TrimSpace(tryName)
				if tryName == "" || tryName == state.name {
					continue
				}
				if coerced := toolcall.CoerceCompleteJSON(state.arguments, tryName); toolcall.CompleteJSON(coerced, tryName) {
					state.name = tryName
					state.arguments = coerced
					break
				}
			}
		}
	}
	hasReady := false
	for _, state := range s.tools {
		if !state.stopped && state.name != "" && toolcall.CompleteJSON(state.arguments, state.name) {
			hasReady = true
			break
		}
	}
	if s.sawTool || hasReady {
		s.held = nil
	} else {
		for _, delta := range s.held {
			frames = append(frames, s.emitText(delta.reasoning, delta.content)...)
		}
		s.held = nil
	}
	frames = append(frames, s.closeThinking()...)
	frames = append(frames, s.closeText()...)
	frames = append(frames, s.emitReadyTools()...)
	frames = append(frames, s.closeTools()...)

	outputTokens := usage.CompletionTokens
	if outputTokens <= 0 && s.outputRunes > 0 {
		outputTokens = s.outputRunes / 4
		if outputTokens == 0 {
			outputTokens = 1
		}
	}
	// Always emit message_delta + message_stop so Claude Code can leave "running"
	// even when tools were incomplete/dropped.
	frames = append(frames, event("message_delta", map[string]any{
		"type": "message_delta",
		"delta": map[string]any{
			"stop_reason": StopReason(finishReason, s.sawTool), "stop_sequence": nil,
		},
		"usage": map[string]any{
			"output_tokens":               outputTokens,
			"input_tokens":                usage.PromptTokens,
			"cache_read_input_tokens":     usage.CacheReadTokens,
			"cache_creation_input_tokens": usage.CacheCreationTokens,
		},
	}))
	frames = append(frames, event("message_stop", map[string]any{"type": "message_stop"}))
	return frames
}

func (s *StreamAssembler) emitReadyTools() []string {
	indexes := make([]int, 0, len(s.tools))
	for index := range s.tools {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	frames := make([]string, 0)
	for _, index := range indexes {
		state := s.tools[index]
		if state.stopped || state.started {
			continue
		}
		if state.name == "" {
			continue
		}
		// Live path: normalize but do NOT invent missing new_string, and use
		// CompleteJSONStrict so truncation repair cannot mark an unterminated
		// fragment complete (otherwise Claude Code receives {"file_path":"/a"}
		// and the still-arriving ".go"} is discarded).
		// Force-finish CoerceCompleteJSON runs in Finish() so path+old without
		// replace wait for the real replacement instead of emitting delete-match
		// mid-stream (Claude Code then ignores the late args → Update "errors").
		normalized := toolcall.NormalizeJSON(state.arguments, state.name)
		if normalized == "" {
			normalized = state.arguments
		}
		if !toolcall.CompleteJSONStrict(normalized, state.name) {
			// Do NOT break: a lower index may be incomplete while a higher
			// index already has complete JSON. Breaking here hung Claude Code
			// tasks that received tools out of order / partial early slots.
			continue
		}
		if s.maxTools > 0 && s.toolsStarted >= s.maxTools {
			break
		}
		state.arguments = normalized
		state.block = s.nextBlock
		s.nextBlock++
		state.started = true
		s.toolsStarted++
		s.sawTool = true
		s.held = nil
		argsJSON := state.arguments
		if strings.TrimSpace(argsJSON) == "" {
			argsJSON = "{}"
		}
		frames = append(frames, event("content_block_start", map[string]any{
			"type": "content_block_start", "index": state.block,
			"content_block": map[string]any{
				"type": "tool_use", "id": state.id, "name": state.name, "input": map[string]any{},
			},
		}))
		frames = append(frames, event("content_block_delta", map[string]any{
			"type": "content_block_delta", "index": state.block,
			"delta": map[string]any{"type": "input_json_delta", "partial_json": argsJSON},
		}))
		s.outputRunes += len([]rune(argsJSON))
		frames = append(frames, event("content_block_stop", map[string]any{
			"type": "content_block_stop", "index": state.block,
		}))
		state.stopped = true
	}
	return frames
}

func (s *StreamAssembler) emitText(reasoning, content string) []string {
	frames := make([]string, 0, 4)
	if reasoning != "" {
		frames = append(frames, s.closeTools()...)
		frames = append(frames, s.closeText()...)
		if s.thinkingBlock < 0 {
			s.thinkingBlock = s.nextBlock
			s.nextBlock++
			frames = append(frames, event("content_block_start", map[string]any{
				"type": "content_block_start", "index": s.thinkingBlock,
				"content_block": map[string]any{"type": "thinking", "thinking": ""},
			}))
		}
		frames = append(frames, event("content_block_delta", map[string]any{
			"type": "content_block_delta", "index": s.thinkingBlock,
			"delta": map[string]any{"type": "thinking_delta", "thinking": reasoning},
		}))
		s.outputRunes += len([]rune(reasoning))
	}
	if content != "" {
		frames = append(frames, s.closeTools()...)
		frames = append(frames, s.closeThinking()...)
		if s.textBlock < 0 {
			s.textBlock = s.nextBlock
			s.nextBlock++
			frames = append(frames, event("content_block_start", map[string]any{
				"type": "content_block_start", "index": s.textBlock,
				"content_block": map[string]any{"type": "text", "text": ""},
			}))
		}
		frames = append(frames, event("content_block_delta", map[string]any{
			"type": "content_block_delta", "index": s.textBlock,
			"delta": map[string]any{"type": "text_delta", "text": content},
		}))
		s.outputRunes += len([]rune(content))
	}
	return frames
}

func (s *StreamAssembler) closeText() []string {
	if s.textBlock < 0 {
		return nil
	}
	index := s.textBlock
	s.textBlock = -1
	return []string{event("content_block_stop", map[string]any{"type": "content_block_stop", "index": index})}
}

func (s *StreamAssembler) closeThinking() []string {
	if s.thinkingBlock < 0 {
		return nil
	}
	index := s.thinkingBlock
	s.thinkingBlock = -1
	return []string{event("content_block_stop", map[string]any{"type": "content_block_stop", "index": index})}
}

func (s *StreamAssembler) closeTools() []string {
	frames := make([]string, 0)
	for _, state := range s.tools {
		if state.started && !state.stopped {
			frames = append(frames, event("content_block_stop", map[string]any{
				"type": "content_block_stop", "index": state.block,
			}))
			state.stopped = true
		}
	}
	return frames
}

func mergeName(current, incoming string) string {
	if current == "" {
		return incoming
	}
	if incoming == "" || current == incoming || len(current) > len(incoming) && current[:len(incoming)] == incoming {
		return current
	}
	if len(incoming) > len(current) && incoming[:len(current)] == current {
		return incoming
	}
	return incoming
}

func ParseEvents(frames []string) []map[string]any {
	out := make([]map[string]any, 0, len(frames))
	for _, frame := range frames {
		for _, line := range splitLines(frame) {
			if len(line) < 5 || line[:5] != "data:" {
				continue
			}
			var payload map[string]any
			if json.Unmarshal([]byte(trimSpace(line[5:])), &payload) == nil {
				out = append(out, payload)
			}
		}
	}
	return out
}

func splitLines(value string) []string {
	var lines []string
	start := 0
	for index, r := range value {
		if r == '\n' {
			lines = append(lines, value[start:index])
			start = index + 1
		}
	}
	if start < len(value) {
		lines = append(lines, value[start:])
	}
	return lines
}

func trimSpace(value string) string {
	for len(value) > 0 && (value[0] == ' ' || value[0] == '\t') {
		value = value[1:]
	}
	return value
}
