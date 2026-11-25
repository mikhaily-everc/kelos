package sessionruntime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestClaudeSessionStartKeepsTheConversationAcrossARestart(t *testing.T) {
	state, configDir := t.TempDir(), t.TempDir()
	config := ProviderConfig{StateDir: state, Environment: []string{"CLAUDE_CONFIG_DIR=" + configDir}}

	// First start, no turn ever finishes: the ID must already be on disk for the next Pod.
	first, resume, err := claudeSessionStart(config)
	if err != nil || resume {
		t.Fatalf("first start = (%q, %t, %v), want a fresh session", first, resume, err)
	}
	saved, err := os.ReadFile(filepath.Join(state, "claude-session-id"))
	if err != nil || strings.TrimSpace(string(saved)) != first {
		t.Fatalf("session ID was not saved at start: %q, %v", saved, err)
	}

	// Restart before Claude wrote a transcript: same ID, fresh (--resume would fail).
	second, resume, err := claudeSessionStart(config)
	if err != nil || second != first || resume {
		t.Fatalf("restart without a transcript = (%q, %t, %v), want (%q, false)", second, resume, err, first)
	}

	// Restart once the transcript exists: same ID, resumed.
	project := filepath.Join(configDir, "projects", "-workspace-repo")
	if err := os.MkdirAll(project, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(project, first+".jsonl"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	third, resume, err := claudeSessionStart(config)
	if err != nil || third != first || !resume {
		t.Fatalf("restart with a transcript = (%q, %t, %v), want (%q, true)", third, resume, err, first)
	}
}

type channelWriteCloser struct {
	writes chan []byte
}

func (w *channelWriteCloser) Write(data []byte) (int, error) {
	w.writes <- append([]byte(nil), data...)
	return len(data), nil
}

func (w *channelWriteCloser) Close() error {
	return nil
}

func TestClaudeProviderIncludesShellCommandWithNextPrompt(t *testing.T) {
	stdin := &channelWriteCloser{writes: make(chan []byte, 1)}
	provider := &ClaudeProvider{
		ctx:       context.Background(),
		stdin:     stdin,
		sessionID: "session-1",
	}
	record := shellCommandRecord{
		command:  "printf shell-output",
		exitCode: 0,
		duration: 1234 * time.Millisecond,
		output:   "shell-output",
	}
	if err := provider.recordShellCommand(t.Context(), record); err != nil {
		t.Fatalf("recordShellCommand() error = %v", err)
	}
	select {
	case message := <-stdin.writes:
		t.Fatalf("recordShellCommand() wrote a Claude message: %s", message)
	default:
	}

	provider.SetEventSink(newProviderTestSink(nil))
	if err := provider.StartTurn(t.Context(), TurnInput{Text: "continue"}); err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}

	var message struct {
		Message struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"message"`
	}
	select {
	case data := <-stdin.writes:
		if err := json.Unmarshal(data, &message); err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Claude prompt was not submitted")
	}
	wantContext := "<user_shell_command>\n<command>\nprintf shell-output\n</command>\n<result>\nExit code: 0\nDuration: 1.2340 seconds\nOutput:\nshell-output\n</result>\n</user_shell_command>"
	if len(message.Message.Content) != 2 || message.Message.Content[0].Type != "text" || message.Message.Content[0].Text != wantContext || message.Message.Content[1].Text != "continue" {
		t.Fatalf("Claude prompt content = %#v", message.Message.Content)
	}
	provider.shellContextMu.Lock()
	defer provider.shellContextMu.Unlock()
	if len(provider.pendingShellCommands) != 0 {
		t.Fatalf("pending shell commands = %#v", provider.pendingShellCommands)
	}
}

// TestClaudeProviderClosesEachTextBlock verifies that a streamed turn with two
// text blocks emits an assistant.message after each block's deltas, so clients
// render them as separate bubbles instead of concatenating the whole turn.
func TestClaudeProviderClosesEachTextBlock(t *testing.T) {
	provider := &ClaudeProvider{blockText: map[int]*strings.Builder{}}
	sink := newProviderTestSink(nil)

	events := []string{
		`{"type":"content_block_start","index":0,"content_block":{"type":"text"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" there"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"text"}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"Second block"}}`,
		`{"type":"content_block_stop","index":1}`,
	}
	for _, raw := range events {
		provider.emitClaudeStreamEvent(json.RawMessage(raw), sink)
	}

	want := []Event{
		{Type: EventAssistantDelta, Text: "Hello"},
		{Type: EventAssistantDelta, Text: " there"},
		{Type: EventAssistantMessage, Text: "Hello there"},
		{Type: EventAssistantDelta, Text: "Second block"},
		{Type: EventAssistantMessage, Text: "Second block"},
	}
	if got := sink.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Claude events = %#v, want %#v", got, want)
	}
}

// TestClaudeProviderStreamedToolBlockEmitsNoMessage verifies that closing a
// non-text content block (a tool_use) does not emit an empty assistant.message.
func TestClaudeProviderStreamedToolBlockEmitsNoMessage(t *testing.T) {
	provider := &ClaudeProvider{blockText: map[int]*strings.Builder{}}
	sink := newProviderTestSink(nil)

	events := []string{
		`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool-1","name":"Bash"}}`,
		`{"type":"content_block_stop","index":0}`,
	}
	for _, raw := range events {
		provider.emitClaudeStreamEvent(json.RawMessage(raw), sink)
	}

	want := []Event{
		{Type: EventToolStarted, ToolID: "tool-1", ToolName: "Bash", Status: "running"},
	}
	if got := sink.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Claude events = %#v, want %#v", got, want)
	}
}

// TestClaudeProviderEmitsMessageWithoutStreaming verifies that when no text
// deltas were streamed, the assembled assistant message still emits one
// assistant.message per text block.
func TestClaudeProviderEmitsMessageWithoutStreaming(t *testing.T) {
	provider := &ClaudeProvider{}
	sink := newProviderTestSink(nil)

	message := `{"content":[{"type":"text","text":"First"},{"type":"text","text":"Second"}]}`
	provider.emitClaudeMessage("assistant", json.RawMessage(message), sink)

	want := []Event{
		{Type: EventAssistantMessage, Text: "First"},
		{Type: EventAssistantMessage, Text: "Second"},
	}
	if got := sink.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Claude events = %#v, want %#v", got, want)
	}
}

// TestClaudeProviderRuntimeStatusMapping verifies that the model, token usage,
// and context window reported by Claude Code become runtime status updates,
// that repeated per-block usage for one assistant message is counted once, and
// that a result event's session-cumulative usage replaces the per-message sums
// (it also covers API calls that never appear as assistant events).
func TestClaudeProviderRuntimeStatusMapping(t *testing.T) {
	provider := &ClaudeProvider{config: ProviderConfig{Effort: "high"}}
	sink := newProviderTestSink(nil)

	// An init event records status even when no consumer is attached.
	if _, err := provider.handleClaudeLine([]byte(`{"type":"system","subtype":"init","model":"claude-opus-4-6","session_id":"ses-1"}`), nil); err != nil {
		t.Fatalf("handleClaudeLine() error = %v", err)
	}

	lines := []string{
		`{"type":"assistant","message":{"id":"msg-1","model":"claude-opus-4-6","usage":{"input_tokens":10,"cache_creation_input_tokens":40,"cache_read_input_tokens":50,"output_tokens":5},"content":[]}}`,
		`{"type":"assistant","message":{"id":"msg-1","model":"claude-opus-4-6","usage":{"input_tokens":10,"cache_creation_input_tokens":40,"cache_read_input_tokens":50,"output_tokens":5},"content":[]}}`,
		`{"type":"assistant","message":{"id":"msg-2","model":"claude-opus-4-6","usage":{"input_tokens":20,"cache_creation_input_tokens":0,"cache_read_input_tokens":100,"output_tokens":10},"content":[]}}`,
		`{"type":"result","subtype":"success","usage":{"input_tokens":40,"cache_creation_input_tokens":60,"cache_read_input_tokens":200,"output_tokens":25},"modelUsage":{"claude-opus-4-6":{"contextWindow":200000}}}`,
	}
	for _, line := range lines {
		if _, err := provider.handleClaudeLine([]byte(line), sink); err != nil {
			t.Fatalf("handleClaudeLine(%s) error = %v", line, err)
		}
	}

	events := sink.snapshot()
	if len(events) != 3 {
		t.Fatalf("runtime status events = %#v", events)
	}
	for _, event := range events {
		if event.Type != EventRuntimeStatus || event.Runtime == nil {
			t.Fatalf("runtime status event = %#v", event)
		}
	}
	// The per-message sums (220 in, 15 out) provide live updates mid-turn.
	live := events[1].Runtime
	if live.Usage == nil || live.Usage.InputTokens != 220 || live.Usage.OutputTokens != 15 {
		t.Fatalf("Claude mid-turn runtime status = %#v", live)
	}
	// The result event's cumulative usage exceeds the visible message sums and
	// replaces them.
	status := events[2].Runtime
	if status.Model != "claude-opus-4-6" ||
		status.Effort != "high" ||
		status.Usage == nil ||
		status.Usage.InputTokens != 300 ||
		status.Usage.OutputTokens != 25 ||
		status.Usage.TotalTokens != 325 ||
		status.Usage.ContextTokens != 130 ||
		status.Usage.ContextWindow != 200000 {
		t.Fatalf("Claude runtime status = %#v", status)
	}

	server := NewServer(Config{AgentType: "claude-code"}, NewJournal(), provider)
	t.Cleanup(func() { server.journal.Close() })
	initial := server.runtimeStatusSnapshot()
	if initial.Model != "claude-opus-4-6" || initial.Usage == nil || initial.Usage.TotalTokens != 325 {
		t.Fatalf("initial server runtime status = %#v", initial)
	}
}

// TestClaudeProviderStreamingSuppressesAssembledMessage verifies that once text
// deltas have streamed (and been closed per block), the assembled assistant
// message does not re-emit the same text as duplicate assistant.message events.
func TestClaudeProviderStreamingSuppressesAssembledMessage(t *testing.T) {
	provider := &ClaudeProvider{blockText: map[int]*strings.Builder{}}
	sink := newProviderTestSink(nil)

	stream := []string{
		`{"type":"content_block_start","index":0,"content_block":{"type":"text"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Streamed"}}`,
		`{"type":"content_block_stop","index":0}`,
	}
	for _, raw := range stream {
		provider.emitClaudeStreamEvent(json.RawMessage(raw), sink)
	}
	provider.emitClaudeMessage("assistant", json.RawMessage(`{"content":[{"type":"text","text":"Streamed"}]}`), sink)

	want := []Event{
		{Type: EventAssistantDelta, Text: "Streamed"},
		{Type: EventAssistantMessage, Text: "Streamed"},
	}
	if got := sink.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Claude events = %#v, want %#v", got, want)
	}
}
