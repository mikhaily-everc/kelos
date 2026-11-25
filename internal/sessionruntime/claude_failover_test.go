package sessionruntime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClaude struct {
	tier    string
	sink    EventSink
	turns   []string
	closed  bool
	done    chan struct{}
	onStart func(f *fakeClaude, input TurnInput)
}

func (f *fakeClaude) SetEventSink(EventSink) {}
func (f *fakeClaude) StartTurn(_ context.Context, input TurnInput) error {
	f.turns = append(f.turns, input.Text)
	f.sink.Emit(Event{Type: EventTurnStarted, Status: "running"})
	f.onStart(f, input)
	return nil
}
func (f *fakeClaude) Interrupt(context.Context) error { return nil }
func (f *fakeClaude) Done() <-chan struct{}           { return f.done }
func (f *fakeClaude) Close() error {
	f.closed = true
	close(f.done)
	return nil
}
func (f *fakeClaude) recordShellCommand(context.Context, shellCommandRecord) error { return nil }
func (f *fakeClaude) runtimeStatusSnapshot() RuntimeStatus                         { return RuntimeStatus{} }
func (f *fakeClaude) providerError() error                                         { return nil }

type failoverHarness struct {
	t         *testing.T
	dir       string
	mu        sync.Mutex
	started   []*fakeClaude
	behaviour map[string]string
	now       time.Time
}

func (h *failoverHarness) factory(_ context.Context, config ProviderConfig) (claudeInner, error) {
	tier := "bifrost"
	if token := environmentValue(config.Environment, "CLAUDE_CODE_OAUTH_TOKEN"); token != "" {
		tier = token
	}
	f := &fakeClaude{tier: tier, sink: config.EventSink, done: make(chan struct{})}
	f.onStart = func(f *fakeClaude, _ TurnInput) {
		h.mu.Lock()
		failure := h.behaviour[f.tier]
		h.mu.Unlock()
		if failure != "" {
			f.sink.Emit(Event{Type: EventTurnCompleted, Status: "failed", Text: failure})
			return
		}
		f.sink.Emit(Event{Type: EventTurnCompleted, Status: "completed"})
	}
	h.mu.Lock()
	h.started = append(h.started, f)
	h.mu.Unlock()
	return f, nil
}

func (h *failoverHarness) write(name, token string) {
	h.t.Helper()
	if err := os.WriteFile(filepath.Join(h.dir, name), []byte(token+"\n"), 0600); err != nil {
		h.t.Fatal(err)
	}
}

func newFailoverHarness(t *testing.T, bifrost bool) (*failoverHarness, Provider, *providerTestSink) {
	t.Helper()
	h := &failoverHarness{t: t, dir: t.TempDir(), behaviour: map[string]string{}, now: time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)}
	h.write("primary", "tok-primary")
	h.write("secondary", "tok-secondary")
	environment := []string{"PATH=/bin", claudeTiersDirEnv + "=" + h.dir}
	if bifrost {
		environment = append(environment, claudeBifrostEnvEnv+`={"ANTHROPIC_BASE_URL":"http://sidecar","ANTHROPIC_AUTH_TOKEN":"sidecar"}`)
	}
	tiers, ok := claudeTierConfigFromEnvironment(environment)
	if !ok {
		t.Fatal("tier config not recognised")
	}
	sink := newProviderTestSink(nil)
	sink.completed = make(chan Event, 8)
	config := ProviderConfig{StateDir: t.TempDir(), Environment: environment, EventSink: sink}
	provider, err := newFailoverClaudeProvider(t.Context(), config, tiers, h.factory, func() time.Time { return h.now })
	if err != nil {
		t.Fatal(err)
	}
	return h, provider, sink
}

func (h *failoverHarness) turnsOn(tier string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var turns []string
	for _, f := range h.started {
		if f.tier == tier {
			turns = append(turns, f.turns...)
		}
	}
	return turns
}

func waitCompleted(t *testing.T, sink *providerTestSink) Event {
	t.Helper()
	select {
	case event := <-sink.completed:
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("turn did not complete")
		return Event{}
	}
}

const limitText = "You've hit your session limit · resets 1:40pm (UTC)"

func TestFailoverRetriesTheSameTurnOnTheSecondTier(t *testing.T) {
	h, provider, sink := newFailoverHarness(t, true)
	h.behaviour["tok-primary"] = limitText
	if err := provider.StartTurn(t.Context(), TurnInput{Text: "do the thing"}); err != nil {
		t.Fatal(err)
	}
	if event := waitCompleted(t, sink); event.Status != "completed" {
		t.Fatalf("turn finished %q (%s), want completed", event.Status, event.Text)
	}
	if got := h.turnsOn("tok-secondary"); len(got) != 1 || got[0] != "do the thing" {
		t.Fatalf("secondary ran %v, want the same message once", got)
	}
	if len(h.turnsOn("bifrost")) != 0 {
		t.Fatal("bifrost ran although the secondary served the turn")
	}
	starts := 0
	for _, e := range sink.snapshot() {
		if e.Type == EventTurnStarted {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("the relay saw %d turn starts, want 1", starts)
	}
}

func TestFailoverFallsThroughToBifrostWhenBothTokensAreExhausted(t *testing.T) {
	h, provider, sink := newFailoverHarness(t, true)
	h.behaviour["tok-primary"] = limitText
	h.behaviour["tok-secondary"] = "Claude AI usage limit reached"
	if err := provider.StartTurn(t.Context(), TurnInput{Text: "go"}); err != nil {
		t.Fatal(err)
	}
	if event := waitCompleted(t, sink); event.Status != "completed" {
		t.Fatalf("turn finished %q (%s), want completed", event.Status, event.Text)
	}
	if got := h.turnsOn("bifrost"); len(got) != 1 || got[0] != "go" {
		t.Fatalf("bifrost ran %v, want the message once", got)
	}
}

func TestFailoverSurfacesTheFailureWhenTheChainIsExhausted(t *testing.T) {
	h, provider, sink := newFailoverHarness(t, false)
	h.behaviour["tok-primary"] = limitText
	h.behaviour["tok-secondary"] = limitText
	if err := provider.StartTurn(t.Context(), TurnInput{Text: "go"}); err != nil {
		t.Fatal(err)
	}
	event := waitCompleted(t, sink)
	if event.Status != "failed" || !strings.Contains(event.Text, "session limit") {
		t.Fatalf("got %q (%s), want the limit failure", event.Status, event.Text)
	}
}

func TestFailoverDoesNotRetryOtherFailures(t *testing.T) {
	h, provider, sink := newFailoverHarness(t, true)
	h.behaviour["tok-primary"] = "tool crashed"
	if err := provider.StartTurn(t.Context(), TurnInput{Text: "go"}); err != nil {
		t.Fatal(err)
	}
	if event := waitCompleted(t, sink); event.Status != "failed" {
		t.Fatalf("got %q, want failed", event.Status)
	}
	if len(h.turnsOn("tok-secondary")) != 0 {
		t.Fatal("an unrelated failure moved to the next tier")
	}
}

func TestFailoverReturnsToThePrimaryAfterItsReset(t *testing.T) {
	h, provider, sink := newFailoverHarness(t, false)
	h.behaviour["tok-primary"] = limitText
	if err := provider.StartTurn(t.Context(), TurnInput{Text: "one"}); err != nil {
		t.Fatal(err)
	}
	waitCompleted(t, sink)
	h.mu.Lock()
	h.behaviour["tok-primary"] = ""
	h.mu.Unlock()
	if err := provider.StartTurn(t.Context(), TurnInput{Text: "two"}); err != nil {
		t.Fatal(err)
	}
	waitCompleted(t, sink)
	if got := h.turnsOn("tok-secondary"); len(got) != 2 {
		t.Fatalf("before the reset the secondary should serve every turn, got %v", got)
	}
	h.now = time.Date(2026, 11, 25, 13, 41, 0, 0, time.UTC)
	if err := provider.StartTurn(t.Context(), TurnInput{Text: "three"}); err != nil {
		t.Fatal(err)
	}
	waitCompleted(t, sink)
	if got := h.turnsOn("tok-primary"); got[len(got)-1] != "three" {
		t.Fatalf("after the reset the primary should serve again, got %v", got)
	}
}

func TestFailoverPicksUpASwappedTokenBeforeTheNextTurn(t *testing.T) {
	h, provider, sink := newFailoverHarness(t, false)
	if err := provider.StartTurn(t.Context(), TurnInput{Text: "one"}); err != nil {
		t.Fatal(err)
	}
	waitCompleted(t, sink)
	h.write("primary", "tok-primary-v2")
	if err := provider.StartTurn(t.Context(), TurnInput{Text: "two"}); err != nil {
		t.Fatal(err)
	}
	waitCompleted(t, sink)
	if got := h.turnsOn("tok-primary-v2"); len(got) != 1 || got[0] != "two" {
		t.Fatalf("the rotated token ran %v, want the second turn", got)
	}
}

func TestFailoverSkipsATierWithNoToken(t *testing.T) {
	h, provider, sink := newFailoverHarness(t, false)
	if err := os.Remove(filepath.Join(h.dir, "primary")); err != nil {
		t.Fatal(err)
	}
	if err := provider.StartTurn(t.Context(), TurnInput{Text: "one"}); err != nil {
		t.Fatal(err)
	}
	waitCompleted(t, sink)
	if got := h.turnsOn("tok-secondary"); len(got) != 1 {
		t.Fatalf("secondary ran %v, want the turn", got)
	}
}

func TestClaudeResetTime(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	cases := map[string]time.Time{
		"resets 1:40pm (UTC)":                  time.Date(2026, 11, 25, 13, 40, 0, 0, time.UTC),
		"resets 9am (UTC)":                     time.Date(2026, 11, 26, 9, 0, 0, 0, time.UTC),
		"resets 12:30am (UTC)":                 time.Date(2026, 11, 26, 0, 30, 0, 0, time.UTC),
		"resets 2pm (America/New_York)":        time.Date(2026, 11, 25, 19, 0, 0, 0, time.UTC),
		"limit reached, no reset time shown":   now.Add(time.Hour),
		"resets 3pm (Nowhere/Invalid)":         now.Add(time.Hour),
	}
	for text, want := range cases {
		if got := claudeResetTime(text, now); !got.Equal(want) {
			t.Errorf("claudeResetTime(%q) = %v, want %v", text, got, want)
		}
	}
}

func TestClaudeTierFailure(t *testing.T) {
	for text, want := range map[string]bool{
		limitText: true,
		"API Error: 401 {\"type\":\"authentication_error\"}": true,
		"OAuth token has expired":                           true,
		"Claude AI usage limit reached":                     true,
		"tool crashed":                                      false,
		"the turn finished with status \"failed\"":          false,
	} {
		if got := claudeTierFailure(text); got != want {
			t.Errorf("claudeTierFailure(%q) = %v, want %v", text, got, want)
		}
	}
}
