package sessionruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"k8s.io/utils/ptr"

	"github.com/kelos-dev/kelos/internal/sessionupdate"
)

func waitForSessionEvent(t *testing.T, journal *Journal, match func(Event) bool) Event {
	t.Helper()
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		for _, event := range journal.Snapshot() {
			if match(event) {
				return event
			}
		}
		select {
		case <-deadline:
			t.Fatalf("Session event missing: %#v", journal.Snapshot())
		case <-tick.C:
		}
	}
}

func TestClaudeBackgroundCompletionReachesSessionWithoutPrompt(t *testing.T) {
	ctx := t.Context()
	reader, writer := io.Pipe()
	provider := &ClaudeProvider{ctx: ctx, config: ProviderConfig{StateDir: t.TempDir()}, done: make(chan struct{})}
	journal := NewJournal()
	defer journal.Close()
	server := NewServer(Config{}, journal, provider)
	server.publishSessionStatus = func(context.Context, bool, bool) error { return nil }
	server.idleDrainRequest = &sessionupdate.Request{ID: "idle-drain"}
	go provider.readLoop(reader)
	defer func() { _ = writer.Close(); <-provider.done }()
	write := func(line string) {
		t.Helper()
		if _, err := fmt.Fprintln(writer, line); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"type":"assistant","session_id":"session-1","message":{"content":[{"type":"text","text":"I will report back"}]}}`)
	write(`{"type":"system","subtype":"task_started","task_id":"task-1","task_type":"local_bash"}`)
	write(`{"type":"result","subtype":"success"}`)
	waitForSessionEvent(t, journal, func(e Event) bool { return e.Type == EventTurnCompleted && e.TurnID == "turn-1" })
	if !server.backgroundActive.Load() {
		t.Fatal("Background work was not retained across the turn boundary")
	}
	_, drain := server.sessionDrainReports()
	if drain.Phase != sessionupdate.PhaseDraining {
		t.Fatalf("Idle drain = %#v", drain)
	}

	write(`{"type":"system","subtype":"task_notification","task_id":"task-1","status":"completed"}`)
	write(`{"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"text"}}}`)
	write(`{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Background work finished"}}}`)
	write(`{"type":"stream_event","event":{"type":"content_block_stop","index":0}}`)
	write(`{"type":"assistant","message":{"content":[{"type":"text","text":"Background work finished"}]}}`)
	write(`{"type":"result","subtype":"success"}`)
	waitForSessionEvent(t, journal, func(e Event) bool { return e.Type == EventTurnCompleted && e.TurnID == "turn-2" })
	if server.backgroundActive.Load() {
		t.Fatal("Completed task still keeps the Session active")
	}
	assertAutonomousHistory(t, journal, "Background work finished")
}

func TestServerBackgroundTasksOnlyBlockIdleDrain(t *testing.T) {
	for _, test := range []struct {
		name             string
		outstanding      int
		backgroundActive bool
		pendingInputs    int64
		updatePhase      sessionupdate.Phase
		idlePhase        sessionupdate.Phase
	}{
		{name: "turn waiting for input", outstanding: 1, pendingInputs: 1, updatePhase: sessionupdate.PhaseDrained, idlePhase: sessionupdate.PhaseDraining},
		{name: "input wait with a queued turn", outstanding: 2, pendingInputs: 1, updatePhase: sessionupdate.PhaseDraining, idlePhase: sessionupdate.PhaseDraining},
		{name: "idle", updatePhase: sessionupdate.PhaseDrained, idlePhase: sessionupdate.PhaseDrained},
		{name: "turn", outstanding: 1, updatePhase: sessionupdate.PhaseDraining, idlePhase: sessionupdate.PhaseDraining},
		{name: "background task", backgroundActive: true, updatePhase: sessionupdate.PhaseDrained, idlePhase: sessionupdate.PhaseDraining},
		{name: "turn and background task", outstanding: 1, backgroundActive: true, updatePhase: sessionupdate.PhaseDraining, idlePhase: sessionupdate.PhaseDraining},
	} {
		t.Run(test.name, func(t *testing.T) {
			journal := NewJournal()
			defer journal.Close()
			server := NewServer(Config{}, journal, &fakeProvider{})
			server.updateRequest = &sessionupdate.Request{ID: "runtime-update"}
			server.idleDrainRequest = &sessionupdate.Request{ID: "idle-drain"}
			server.outstanding = test.outstanding
			server.pendingInputCount.Store(test.pendingInputs)
			if test.backgroundActive {
				server.events.Emit(Event{Type: EventRuntimeStatus, Runtime: &RuntimeStatus{BackgroundTasks: ptr.To(1)}})
			}
			update, idle := server.sessionDrainReports()
			if update.Phase != test.updatePhase || idle.Phase != test.idlePhase {
				t.Fatalf("Drain phases = (%s, %s), want (%s, %s)", update.Phase, idle.Phase, test.updatePhase, test.idlePhase)
			}
		})
	}
}

func TestCodexAutonomousTurnUsesSessionEvents(t *testing.T) {
	provider := &CodexProvider{ctx: t.Context(), threadID: "root"}
	journal := NewJournal()
	defer journal.Close()
	NewServer(Config{}, journal, provider)
	send := func(method, params string) { provider.handleNotification(method, json.RawMessage(params)) }
	send("turn/started", `{"threadId":"root","turn":{"id":"first"}}`)
	send("item/agentMessage/delta", `{"threadId":"root","delta":"I will report back"}`)
	send("turn/completed", `{"threadId":"root","turn":{"id":"first","status":"completed"}}`)
	send("turn/started", `{"threadId":"child","turn":{"id":"child-turn"}}`)
	send("item/agentMessage/delta", `{"threadId":"child","delta":"Child output"}`)
	send("turn/completed", `{"threadId":"child","turn":{"id":"child-turn","status":"completed"}}`)
	send("turn/started", `{"threadId":"root","turn":{"id":"continuation"}}`)
	send("item/agentMessage/delta", `{"threadId":"root","delta":"Background work finished"}`)
	send("turn/completed", `{"threadId":"root","turn":{"id":"continuation","status":"completed"}}`)
	assertAutonomousHistory(t, journal, "Background work finished")
}

func TestCodexLateSubmissionResponsePreservesAutonomousTurn(t *testing.T) {
	journal := NewJournal()
	defer journal.Close()
	provider := &CodexProvider{ctx: t.Context(), threadID: "root"}
	server := NewServer(Config{}, journal, provider)
	defer server.events.close()
	err := provider.startInteraction(t.Context(), codexInteractionMessage, func(ready chan struct{}) (bool, error) {
		provider.handleNotification("turn/started", json.RawMessage(`{"threadId":"root","turn":{"id":"first"}}`))
		provider.handleNotification("turn/completed", json.RawMessage(`{"threadId":"root","turn":{"id":"first","status":"completed"}}`))
		provider.handleNotification("turn/started", json.RawMessage(`{"threadId":"root","turn":{"id":"continuation"}}`))
		provider.setActiveTurn("first", ready)
		return false, errors.New("Submission response arrived after continuation")
	})
	if err == nil {
		t.Fatal("Submission error was not returned")
	}
	if provider.activeTurn != "continuation" {
		t.Fatalf("Active native turn = %q", provider.activeTurn)
	}
	_, state, _ := projectHistory(journal.Snapshot())
	if state.ActiveTurnID != "turn-2" {
		t.Fatalf("Continuation no longer active: %#v", state)
	}
}

func TestOpenCodeAutonomousTurnRequiresBusyStatus(t *testing.T) {
	provider := &OpenCodeProvider{ctx: t.Context(), sessionID: "root"}
	journal := NewJournal()
	defer journal.Close()
	NewServer(Config{}, journal, provider)
	send := func(kind, props string) {
		provider.handleEvent([]byte(fmt.Sprintf(`{"type":%q,"properties":%s}`, kind, props)))
	}
	reply := func(messageID, text string) {
		send("message.updated", fmt.Sprintf(`{"info":{"id":%q,"sessionID":"root","role":"assistant"}}`, messageID))
		send("message.part.updated", fmt.Sprintf(`{"part":{"id":"part-1","messageID":%q,"sessionID":"root","type":"text","text":%q}}`, messageID, text))
	}
	reply("metadata", "Metadata must not start a turn")
	if len(journal.Snapshot()) != 0 {
		t.Fatal("Idle metadata started a turn")
	}
	send("session.status", `{"sessionID":"root","status":{"type":"busy"}}`)
	reply("first", "I will report back")
	send("session.idle", `{"sessionID":"root"}`)
	send("session.status", `{"sessionID":"other","status":{"type":"busy"}}`)
	send("session.status", `{"sessionID":"root","status":{"type":"busy"}}`)
	reply("continuation", "Background work finished")
	send("session.status", `{"sessionID":"root","status":{"type":"idle"}}`)
	assertAutonomousHistory(t, journal, "Background work finished")
}

func assertAutonomousHistory(t *testing.T, journal *Journal, want string) {
	t.Helper()
	started, completed := 0, 0
	for _, event := range journal.Snapshot() {
		switch event.Type {
		case EventTurnStarted:
			started++
		case EventTurnCompleted:
			completed++
		case EventUserMessage:
			t.Fatal("Autonomous reply invented a user message")
		}
		if (event.Type == EventAssistantDelta || event.Type == EventAssistantMessage) && event.Text == want && event.TurnID != "turn-2" {
			t.Fatalf("Follow-up belongs to %q", event.TurnID)
		}
	}
	if started != 2 || completed != 2 {
		t.Fatalf("Turn boundaries = %d/%d, want 2/2", started, completed)
	}
	items, state, _ := projectHistory(journal.Snapshot())
	if state.ActiveTurnID != "" {
		t.Fatalf("Completed reply still active: %#v", state)
	}
	count := 0
	for _, item := range items {
		for _, event := range item.events {
			if event.Type == EventAssistantMessage && event.Text == want {
				count++
			}
			if strings.Contains(event.Text, "Child output") {
				t.Fatal("Child turn leaked into root history")
			}
		}
	}
	if count != 1 {
		t.Fatalf("Replay contains %d follow-ups, want 1", count)
	}
}

func TestSessionQueuesUserPromptBehindAutonomousTurn(t *testing.T) {
	provider := &fakeProvider{}
	journal := NewJournal()
	defer journal.Close()
	server := NewServer(Config{}, journal, provider)
	server.events.Emit(Event{Type: EventTurnStarted})
	if err := server.submitMessage("next task", "request-1"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go server.runTurns(ctx)
	if err := server.editMessage("turn-2", "edited task", 1, "edit"); err != nil {
		t.Fatal(err)
	}
	if err := server.submitMessage("additional detail", "merge"); err != nil {
		t.Fatal(err)
	}
	server.events.Emit(Event{Type: EventAssistantMessage, Text: "Background work finished"})
	server.events.Emit(Event{Type: EventTurnCompleted, Status: "completed"})
	waitForSessionEvent(t, journal, func(e Event) bool { return e.Type == EventTurnCompleted && e.TurnID == "turn-2" })
	var sequence []string
	for _, event := range journal.Snapshot() {
		if event.Type == EventTurnStarted || event.Type == EventTurnCompleted {
			sequence = append(sequence, event.Type+":"+event.TurnID)
		}
	}
	if got := strings.Join(sequence, ","); got != "turn.started:turn-1,turn.completed:turn-1,turn.started:turn-2,turn.completed:turn-2" {
		t.Fatalf("Turn order = %s", got)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if len(provider.prompts) != 1 || provider.prompts[0] != "edited task\n\nadditional detail" {
		t.Fatalf("Prompts = %#v", provider.prompts)
	}
}

func TestSessionRemovesPromptQueuedBehindAutonomousTurn(t *testing.T) {
	provider := &fakeProvider{}
	journal := NewJournal()
	defer journal.Close()
	server := NewServer(Config{}, journal, provider)
	server.events.Emit(Event{Type: EventTurnStarted})
	if err := server.submitMessage("cancelled task", "request"); err != nil {
		t.Fatal(err)
	}
	request := <-server.turns
	done := make(chan struct{})
	go func() {
		server.runTurn(t.Context(), request)
		close(done)
	}()
	if err := server.removePendingMessage("turn-2", 1, "remove"); err != nil {
		t.Fatal(err)
	}
	server.events.Emit(Event{Type: EventTurnCompleted, Status: "completed"})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Removed turn did not settle")
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if len(provider.prompts) != 0 {
		t.Fatalf("Removed prompt reached the provider: %#v", provider.prompts)
	}
}

type failingProvider struct {
	fakeProvider
	err error
}

func (p *failingProvider) providerError() error { return p.err }

func TestSessionRecordsProviderFailureBeforeShutdown(t *testing.T) {
	for _, autonomous := range []bool{false, true} {
		t.Run(fmt.Sprintf("autonomous=%t", autonomous), func(t *testing.T) {
			stateDir := shortRuntimeTempDir(t)
			journal := NewJournal()
			failure := errors.New("decoding provider output: invalid JSON")
			provider := &failingProvider{fakeProvider: fakeProvider{resume: make(chan struct{})}, err: failure}
			server := NewServer(Config{SocketPath: filepath.Join(stateDir, "runtime.sock")}, journal, provider)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- server.Serve(ctx) }()
			waitForRuntime(t, server.config.SocketPath)
			if autonomous {
				server.events.Emit(Event{Type: EventTurnStarted})
			} else {
				if err := server.submitMessage("work", "request"); err != nil {
					t.Fatal(err)
				}
				waitForSessionEvent(t, journal, func(e Event) bool { return e.Type == EventAssistantDelta })
			}
			if err := provider.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if !errors.Is(err, failure) {
					t.Fatalf("Serve() error = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Failed provider did not stop the Session")
			}
			waitForSessionEvent(t, journal, func(e Event) bool {
				return e.Type == EventError && e.TurnID == "turn-1" && strings.Contains(e.Text, failure.Error())
			})
			waitForSessionEvent(t, journal, func(e Event) bool {
				return e.Type == EventTurnCompleted && e.TurnID == "turn-1" && e.Status == "failed"
			})
		})
	}
}

func TestSessionReceivesEventsWhileCollectingWorkspaceDiff(t *testing.T) {
	journal := NewJournal()
	defer journal.Close()
	server := NewServer(Config{AgentType: "claude-code"}, journal, &fakeProvider{})
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	server.readWorkspaceDiff = func(ctx context.Context, _ string) string {
		started <- struct{}{}
		select {
		case <-release:
			return "+change"
		case <-ctx.Done():
			return ""
		}
	}
	server.events.Emit(Event{Type: EventTurnStarted})
	done := make(chan struct{})
	go func() {
		server.events.Emit(Event{Type: EventTurnCompleted, Status: "completed"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Provider stream blocked on workspace diff")
	}
	<-started
	server.events.Emit(Event{Type: EventRuntimeStatus, Runtime: &RuntimeStatus{BackgroundTasks: ptr.To(1)}})
	server.events.Emit(Event{Type: EventTurnStarted})
	server.events.Emit(Event{Type: EventAssistantMessage, Text: "Background work finished"})
	server.events.Emit(Event{Type: EventTurnCompleted, Status: "completed"})
	if !server.backgroundActive.Load() {
		t.Fatal("Workspace diff blocked background activity")
	}
	close(release)
	waitForSessionEvent(t, journal, func(e Event) bool { return e.Type == EventTurnCompleted && e.TurnID == "turn-2" })
	assertAutonomousHistory(t, journal, "Background work finished")
	diffs := 0
	for _, event := range journal.Snapshot() {
		if event.Type == EventFileDiff && event.Diff == "+change" {
			diffs++
		}
	}
	if diffs != 2 {
		t.Fatalf("Workspace diffs = %d, want 2", diffs)
	}
}

func TestSessionDefersProviderReplyUntilShellCompletes(t *testing.T) {
	journal := NewJournal()
	defer journal.Close()
	server := NewServer(Config{}, journal, &fakeProvider{})
	server.nextTurnID.Store(1)
	shell, _ := server.events.begin(t.Context(), turnRequest{id: "turn-1", command: sessionCommand{kind: sessionCommandShell}})
	server.events.Emit(Event{Type: EventTurnStarted})
	server.events.Emit(Event{Type: EventAssistantMessage, Text: "Background work finished"})
	server.events.Emit(Event{Type: EventTurnCompleted, Status: "completed"})
	if shell.ctx.Err() != nil {
		t.Fatal("Provider completion cancelled the shell")
	}
	if events := journal.Snapshot(); len(events) != 1 || events[0].Type != EventTurnStarted {
		t.Fatalf("Provider events escaped the shell turn: %#v", events)
	}
	server.events.finish(shell, nil)
	assertAutonomousHistory(t, journal, "Background work finished")
}

func TestSessionForcesInterruptedAutonomousTurnToFinish(t *testing.T) {
	journal := NewJournal()
	defer journal.Close()
	provider := &stuckInterruptProvider{
		interruptCalled: make(chan struct{}), runStopped: make(chan struct{}), done: make(chan struct{}),
		interruptErr: errors.New("Provider cannot interrupt"),
	}
	server := NewServer(Config{}, journal, provider)
	server.events.Emit(Event{Type: EventTurnStarted})
	result := make(chan error, 1)
	go func() { result <- server.interruptTurn(t.Context(), "stop-background") }()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Autonomous interruption did not settle")
	}
	waitForSessionEvent(t, journal, func(e Event) bool { return e.Type == EventTurnCompleted && e.Status == "interrupted" })
	_, state, _ := projectHistory(journal.Snapshot())
	if state.ActiveTurnID != "" {
		t.Fatalf("Interrupted autonomous turn still active: %#v", state)
	}
}

func TestSessionAutonomousTurnRequestsInput(t *testing.T) {
	for _, finishBeforeAnswer := range []bool{false, true} {
		t.Run(fmt.Sprintf("finishBeforeAnswer=%t", finishBeforeAnswer), func(t *testing.T) {
			journal := NewJournal()
			defer journal.Close()
			server := NewServer(Config{}, journal, &fakeProvider{})
			server.events.Emit(Event{Type: EventTurnStarted})
			result := make(chan error, 1)
			go func() {
				answers, err := server.events.RequestInput(t.Context(), InputRequest{
					ID: "background-input", Questions: []InputQuestion{{ID: "continue", Question: "Continue?"}},
				})
				if err == nil && strings.Join(answers["continue"], ",") != "yes" {
					err = fmt.Errorf("Answers = %#v", answers)
				}
				result <- err
			}()
			waitForSessionEvent(t, journal, func(e Event) bool { return e.Type == EventInputRequested && e.TurnID == "turn-1" })
			if finishBeforeAnswer {
				server.events.Emit(Event{Type: EventTurnCompleted, Status: "completed"})
			} else if err := server.resolveInput("background-input", map[string][]string{"continue": {"yes"}}, false, "answer"); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-result:
				if finishBeforeAnswer && !errors.Is(err, context.Canceled) || !finishBeforeAnswer && err != nil {
					t.Fatalf("Input result = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Input request did not settle")
			}
			server.events.close()
		})
	}
}

func TestClaudeTaskActivityTracksReportedTasks(t *testing.T) {
	for _, status := range []string{"running", "completed", "failed", "killed"} {
		t.Run(status, func(t *testing.T) {
			provider := &ClaudeProvider{ctx: t.Context(), done: make(chan struct{})}
			journal := NewJournal()
			defer journal.Close()
			server := NewServer(Config{}, journal, provider)
			provider.readLoop(strings.NewReader(strings.Join([]string{
				`{"type":"system","subtype":"task_started","task_id":"first"}`,
				`{"type":"system","subtype":"task_started","task_id":"second"}`,
				`{"type":"system","subtype":"task_updated","task_id":"first","patch":{"status":"completed"}}`,
				fmt.Sprintf(`{"type":"system","subtype":"task_updated","task_id":"second","patch":{"status":%q}}`, status),
			}, "\n")))
			if provider.readErr != nil {
				t.Fatal(provider.readErr)
			}
			if active := server.backgroundActive.Load(); active != (status == "running") {
				t.Fatalf("Task status %q: background activity = %t", status, active)
			}
			want := 0
			if status == "running" {
				want = 1
			}
			if count := server.runtimeStatusSnapshot().BackgroundTasks; count == nil || *count != want {
				t.Fatalf("Background task count = %v, want %d", count, want)
			}
		})
	}
}

func TestClaudeReportsBackgroundTaskCountChanges(t *testing.T) {
	provider := &ClaudeProvider{ctx: t.Context(), done: make(chan struct{})}
	sink := newProviderTestSink(nil)
	provider.SetEventSink(sink)
	provider.readLoop(strings.NewReader(strings.Join([]string{
		`{"type":"system","subtype":"task_started","task_id":"first"}`,
		`{"type":"system","subtype":"task_started","task_id":"first"}`,
		`{"type":"system","subtype":"task_started","task_id":"second"}`,
		`{"type":"system","subtype":"task_updated","task_id":"unknown","patch":{"status":"completed"}}`,
		`{"type":"system","subtype":"task_updated","task_id":"first","patch":{"status":"completed"}}`,
		`{"type":"system","subtype":"task_notification","task_id":"second","status":"completed"}`,
	}, "\n")))
	if provider.readErr != nil {
		t.Fatal(provider.readErr)
	}
	var counts []int
	for _, event := range sink.snapshot() {
		if event.Type == EventRuntimeStatus && event.Runtime != nil && event.Runtime.BackgroundTasks != nil {
			counts = append(counts, *event.Runtime.BackgroundTasks)
		}
	}
	if want := []int{1, 2, 1, 0}; !reflect.DeepEqual(counts, want) {
		t.Fatalf("Background task counts = %v, want %v", counts, want)
	}
}

func TestServerBackgroundTaskCountStreamsAndReconnects(t *testing.T) {
	journal := NewJournal()
	defer journal.Close()
	server := NewServer(Config{}, journal, &fakeProvider{})
	report := func(count int) {
		server.events.Emit(Event{Type: EventRuntimeStatus, Runtime: &RuntimeStatus{BackgroundTasks: &count}})
	}
	connect := func() *json.Decoder {
		serverConnection, clientConnection := net.Pipe()
		t.Cleanup(func() { _ = clientConnection.Close() })
		if err := clientConnection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		go server.handleConnection(t.Context(), serverConnection)
		if err := json.NewEncoder(clientConnection).Encode(ClientRequest{Type: "subscribe", HistoryBounds: true}); err != nil {
			t.Fatal(err)
		}
		return json.NewDecoder(clientConnection)
	}
	readCount := func(decoder *json.Decoder, want int) {
		t.Helper()
		for {
			var event Event
			if err := decoder.Decode(&event); err != nil {
				t.Fatal(err)
			}
			if event.Type != EventRuntimeStatus {
				continue
			}
			if event.Runtime == nil || event.Runtime.BackgroundTasks == nil || *event.Runtime.BackgroundTasks != want {
				t.Fatalf("Runtime status = %#v, want background task count %d", event.Runtime, want)
			}
			return
		}
	}
	report(1)
	first := connect()
	readCount(first, 1)
	report(2)
	readCount(first, 2)
	reconnected := connect()
	readCount(reconnected, 2)
	server.updateProviderRuntimeStatus(RuntimeStatus{Model: "test-model"})
	readCount(reconnected, 2)
	report(0)
	readCount(reconnected, 0)
	if events := journal.Snapshot(); len(events) != 0 {
		t.Fatalf("Background status added conversation events: %#v", events)
	}
	if server.backgroundActive.Load() {
		t.Fatal("Completed background work still keeps the Session active")
	}
}

func TestClaudeHousekeepingEventsLeaveSessionIdle(t *testing.T) {
	for _, flag := range []string{"skip_transcript", "ambient"} {
		for _, completed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/completed=%t", flag, completed), func(t *testing.T) {
				provider := &ClaudeProvider{ctx: t.Context(), done: make(chan struct{})}
				journal := NewJournal()
				defer journal.Close()
				server := NewServer(Config{}, journal, provider)
				server.idleDrainRequest = &sessionupdate.Request{ID: "idle-drain"}
				lines := []string{
					fmt.Sprintf(`{"type":"system","subtype":"task_started","task_id":"monitor","task_type":"monitor_ws",%q:true}`, flag),
					`{"type":"system","subtype":"task_updated","task_id":"monitor","patch":{"status":"completed"}}`,
					fmt.Sprintf(`{"type":"system","subtype":"task_notification","task_id":"monitor","status":"completed",%q:true}`, flag),
				}
				if !completed {
					lines = lines[:1]
				}
				provider.readLoop(strings.NewReader(strings.Join(lines, "\n")))
				if provider.readErr != nil {
					t.Fatal(provider.readErr)
				}
				if events := journal.Snapshot(); len(events) != 0 {
					t.Fatalf("Housekeeping events started a turn: %#v", events)
				}
				if count := server.runtimeStatusSnapshot().BackgroundTasks; count != nil && *count != 0 {
					t.Fatalf("Housekeeping counted as background work: %v", count)
				}
				_, drain := server.sessionDrainReports()
				if drain.Phase != sessionupdate.PhaseDrained {
					t.Fatalf("Idle drain = %#v, want Drained", drain)
				}
			})
		}
	}
}

func TestClaudeChildOutputStaysWithinActiveParentTurn(t *testing.T) {
	provider := &ClaudeProvider{ctx: t.Context(), config: ProviderConfig{StateDir: t.TempDir()}, done: make(chan struct{})}
	journal := NewJournal()
	defer journal.Close()
	NewServer(Config{}, journal, provider)
	provider.readLoop(strings.NewReader(strings.Join([]string{
		`{"type":"assistant","parent_tool_use_id":"child","message":{"content":[{"type":"text","text":"Orphan output"}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"text","text":"Root started"}]}}`,
		`{"type":"assistant","parent_tool_use_id":"child","message":{"content":[{"type":"text","text":"Child progress"}]}}`,
		`{"type":"result","parent_tool_use_id":"child","subtype":"success"}`,
		`{"type":"assistant","message":{"content":[{"type":"text","text":"Root finished"}]}}`,
		`{"type":"result","subtype":"success"}`,
	}, "\n")))
	started, completed, childOutput := 0, 0, false
	for _, event := range journal.Snapshot() {
		switch event.Type {
		case EventTurnStarted:
			started++
		case EventTurnCompleted:
			completed++
		case EventAssistantMessage:
			if event.Text == "Orphan output" {
				t.Fatal("Child output started a turn without its parent")
			}
			if event.Text == "Child progress" && event.TurnID == "turn-1" {
				childOutput = true
			}
		}
	}
	if started != 1 || completed != 1 || !childOutput {
		t.Fatalf("Parent turn boundaries = %d/%d, child output = %t", started, completed, childOutput)
	}
}
