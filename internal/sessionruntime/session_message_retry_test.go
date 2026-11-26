package sessionruntime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kelos "github.com/kelos-dev/kelos/api/v1alpha2"
)

const testBifrostFailure = `API Error: 502 {"error":"bad_gateway","detail":"could not obtain the Bifrost key"}`

// flakyProvider fails one turn per queued failure text, then completes turns.
type flakyProvider struct {
	fakeProvider
	failures []string
}

func (p *flakyProvider) StartTurn(ctx context.Context, input TurnInput) error {
	p.mu.Lock()
	p.prompts = append(p.prompts, input.Text)
	failure := ""
	if len(p.failures) > 0 {
		failure, p.failures = p.failures[0], p.failures[1:]
	}
	p.mu.Unlock()
	return p.startTurn(ctx, func() error {
		if failure != "" {
			return errors.New(failure)
		}
		p.sink.Emit(Event{Type: EventAssistantMessage, Text: "done"})
		return nil
	})
}

func (p *flakyProvider) promptCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.prompts)
}

func (h *sessionMessageHarness) advance(by time.Duration) {
	h.relay.now = func() time.Time { return time.Now().Add(by) }
}

func (h *sessionMessageHarness) noTurnQueued() {
	h.t.Helper()
	if queued := len(h.server.turns); queued != 0 {
		h.t.Fatalf("a turn was queued before the retry delay passed")
	}
}

func TestSessionTransientFailure(t *testing.T) {
	for _, tt := range []struct {
		text string
		want bool
	}{
		{testBifrostFailure, true},
		{`API Error: 502 {"error":"bad_gateway","detail":"Bifrost unreachable"}`, true},
		{`API Error: 502 {"error":"bad_gateway","detail":"gateway unreachable"}`, true},
		{`API Error: 502 {"error":"bad_gateway","detail":"could not obtain a gateway access token"}`, true},
		{"API Error: 503 upstream connect error or disconnect/reset before headers. reset reason: connection failure", true},
		{`API Error: 529 {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`, true},
		{"API Error: 504 Gateway Timeout", true},
		{"read ECONNRESET", true},
		{`API Error: 401 {"type":"error","error":{"type":"authentication_error"}}`, false},
		{"API Error: 403 forbidden", false},
		{"You've hit your weekly limit · resets 1:40pm (UTC)", false},
		{`API Error: 400 {"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long"}}`, false},
		{"API Error: 429 rate_limit_error", false},
		{"the turn finished with status \"failed\"", false},
	} {
		if got := sessionTransientFailure(tt.text); got != tt.want {
			t.Errorf("sessionTransientFailure(%q) = %v, want %v", tt.text, got, tt.want)
		}
	}
}

func TestSessionMessageAttemptRoundTrips(t *testing.T) {
	for attempt := 0; attempt <= sessionMessageMaxRetries(); attempt++ {
		id := "sessionmessage-b-retried-uid" + sessionMessageAttemptSuffix(attempt)
		if got := sessionMessageAttempt(id); got != attempt {
			t.Fatalf("sessionMessageAttempt(%q) = %d, want %d", id, got, attempt)
		}
	}
	if got := sessionMessageAttemptSuffix(1); got != "-retry" {
		t.Fatalf("attempt 1 suffix = %q; older journals record -retry", got)
	}
}

func TestSessionMessageRelayRetriesTransientFailureAfterDelay(t *testing.T) {
	provider := &flakyProvider{failures: []string{testBifrostFailure}}
	h := newSessionMessageHarness(t, nil, provider)
	h.create(testSessionMessage("m-1", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "hello"}))
	h.sync()
	h.runPendingTurn()
	h.sync()

	status := h.status("m-1")
	if status.Phase != kelos.SessionMessagePhaseAccepted || status.Reason != kelos.SessionMessageReasonTransientRetry ||
		!strings.Contains(status.Message, "retry 1 of 3") || !strings.Contains(status.Message, "could not obtain the Bifrost key") {
		t.Fatalf("waiting status = %#v", status)
	}
	h.advance(29 * time.Second)
	h.sync()
	h.noTurnQueued()
	if status := h.status("m-1"); status.Reason != kelos.SessionMessageReasonTransientRetry || status.RequestID != "sessionmessage-m-1-uid" {
		t.Fatalf("message was redelivered before its delay: %#v", status)
	}

	h.advance(31 * time.Second)
	h.sync()
	if status := h.status("m-1"); status.Phase != kelos.SessionMessagePhaseAccepted || status.RequestID != "sessionmessage-m-1-uid-retry" || status.Reason != "" {
		t.Fatalf("retried status = %#v", status)
	}
	h.runPendingTurn()
	h.sync()
	if status := h.status("m-1"); status.Phase != kelos.SessionMessagePhaseCompleted || status.Result == nil || status.Result.Text != "done" {
		t.Fatalf("final status = %#v", status)
	}
	if count := provider.promptCount(); count != 2 {
		t.Fatalf("provider ran %d turns, want 2", count)
	}
}

func TestSessionMessageRelayFailsNonTransientFailureImmediately(t *testing.T) {
	failure := `API Error: 401 {"type":"error","error":{"type":"authentication_error"}}`
	provider := &flakyProvider{failures: []string{failure, testBifrostFailure}}
	h := newSessionMessageHarness(t, nil, provider)
	h.create(testSessionMessage("m-1", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "hello"}))
	h.sync()
	h.runPendingTurn()
	h.sync()
	status := h.status("m-1")
	if status.Phase != kelos.SessionMessagePhaseFailed || status.Reason != kelos.SessionMessageReasonTurnFailed || status.Message != failure {
		t.Fatalf("non-transient status = %#v", status)
	}
	h.create(testSessionMessage("m-2", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "again"}))
	h.sync()
	h.runPendingTurn()
	h.sync()
	if status := h.status("m-2"); status.Reason != kelos.SessionMessageReasonTransientRetry {
		t.Fatalf("the same path must still retry a transient failure, got %#v", status)
	}
	h.advance(time.Hour)
	h.sync()
	if status := h.status("m-1"); status.Phase != kelos.SessionMessagePhaseFailed {
		t.Fatalf("non-transient message changed later: %#v", status)
	}
	if count := provider.promptCount(); count != 2 {
		t.Fatalf("provider ran %d turns, want 2 (m-1 must not be retried)", count)
	}
}

func TestSessionMessageRelayFailsAfterTransientRetriesAreExhausted(t *testing.T) {
	last := "API Error: 503 upstream connect error or disconnect/reset before headers"
	provider := &flakyProvider{failures: []string{testBifrostFailure, testBifrostFailure, testBifrostFailure, last}}
	h := newSessionMessageHarness(t, nil, provider)
	h.create(testSessionMessage("m-1", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "hello"}))
	h.sync()
	wantIDs := []string{"sessionmessage-m-1-uid", "sessionmessage-m-1-uid-retry", "sessionmessage-m-1-uid-retry2", "sessionmessage-m-1-uid-retry3"}
	for index, want := range wantIDs {
		if status := h.status("m-1"); status.RequestID != want || status.Phase != kelos.SessionMessagePhaseAccepted {
			t.Fatalf("attempt %d status = %#v, want request %s", index, status, want)
		}
		h.runPendingTurn()
		h.sync()
		h.advance(time.Duration(index+1) * 10 * time.Minute)
		h.sync()
	}
	status := h.status("m-1")
	if status.Phase != kelos.SessionMessagePhaseFailed || status.Reason != kelos.SessionMessageReasonTurnFailed ||
		!strings.Contains(status.Message, "after 3 retries") || !strings.Contains(status.Message, last) {
		t.Fatalf("exhausted status = %#v", status)
	}
	h.noTurnQueued()
	if count := provider.promptCount(); count != 4 {
		t.Fatalf("provider ran %d turns, want 4", count)
	}
}

func TestSessionMessageRelayKeepsTransientRetryAcrossRestart(t *testing.T) {
	path := t.TempDir() + "/events.jsonl"
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	h := newSessionMessageHarness(t, journal, &flakyProvider{failures: []string{testBifrostFailure}})
	h.create(testSessionMessage("m-1", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "hello"}))
	h.sync()
	h.runPendingTurn()
	h.sync()
	waiting, err := h.client.Get(t.Context(), "m-1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if waiting.Status.Reason != kelos.SessionMessageReasonTransientRetry {
		t.Fatalf("waiting status = %#v", waiting.Status)
	}
	journal.Close()

	journal, err = OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	recovery, err := recoverJournal(journal)
	if err != nil {
		t.Fatal(err)
	}
	waiting = waiting.DeepCopy()
	waiting.ResourceVersion = ""
	provider := &flakyProvider{}
	restarted := newSessionMessageHarness(t, journal, provider, waiting)
	if err := restarted.server.restoreTurn(recovery.pendingTurn); err != nil {
		t.Fatal(err)
	}
	restarted.server.nextTurnID.Store(recovery.nextTurnID)
	restarted.sync()
	restarted.noTurnQueued()
	if status := restarted.status("m-1"); status.Reason != kelos.SessionMessageReasonTransientRetry || status.RequestID != "sessionmessage-m-1-uid" {
		t.Fatalf("restart redelivered before the delay: %#v", status)
	}

	restarted.advance(31 * time.Second)
	restarted.sync()
	if status := restarted.status("m-1"); status.RequestID != "sessionmessage-m-1-uid-retry" || status.PodUID != testSessionMessagePodUID {
		t.Fatalf("retried status = %#v", status)
	}
	restarted.runPendingTurn()
	restarted.sync()
	restarted.sync()
	if status := restarted.status("m-1"); status.Phase != kelos.SessionMessagePhaseCompleted {
		t.Fatalf("final status = %#v", status)
	}
	restarted.noTurnQueued()
	if count := provider.promptCount(); count != 1 {
		t.Fatalf("restarted provider ran %d turns, want 1", count)
	}
}

func TestSessionMessageRelayAdoptsRetrySubmittedBeforeRestart(t *testing.T) {
	path := t.TempDir() + "/events.jsonl"
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []Event{
		{Type: EventUserMessage, RequestID: "sessionmessage-m-1-uid", TurnID: "turn-1", Text: "hello", Revision: 1},
		{Type: EventTurnStarted, TurnID: "turn-1", Status: "running"},
		{Type: EventError, TurnID: "turn-1", Text: testBifrostFailure, Status: "failed"},
		{Type: EventTurnCompleted, TurnID: "turn-1", Status: "failed"},
		{Type: EventUserMessage, RequestID: "sessionmessage-m-1-uid-retry", TurnID: "turn-2", Text: "hello", Revision: 1},
	} {
		if err := journal.Append(event); err != nil {
			t.Fatal(err)
		}
	}
	journal.Close()
	journal, err = OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	recovery, err := recoverJournal(journal)
	if err != nil {
		t.Fatal(err)
	}
	message := testSessionMessage("m-1", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "hello"})
	now := metav1.Now()
	message.Status = kelos.SessionMessageStatus{
		Phase: kelos.SessionMessagePhaseAccepted, PodUID: "pod-old", RequestID: "sessionmessage-m-1-uid", TurnID: "turn-1",
		Reason: kelos.SessionMessageReasonTransientRetry, AcceptedAt: &now,
	}
	provider := &flakyProvider{}
	h := newSessionMessageHarness(t, journal, provider, message)
	if err := h.server.restoreTurn(recovery.pendingTurn); err != nil {
		t.Fatal(err)
	}
	h.server.nextTurnID.Store(recovery.nextTurnID)
	h.sync()
	if status := h.status("m-1"); status.RequestID != "sessionmessage-m-1-uid-retry" || status.TurnID != "turn-2" || status.Reason != "" {
		t.Fatalf("submitted retry was not adopted: %#v", status)
	}
	h.runPendingTurn()
	h.advance(10 * time.Minute)
	h.sync()
	if status := h.status("m-1"); status.Phase != kelos.SessionMessagePhaseCompleted {
		t.Fatalf("final status = %#v", status)
	}
	h.noTurnQueued()
	if count := provider.promptCount(); count != 1 {
		t.Fatalf("provider ran %d turns, want 1", count)
	}
}
