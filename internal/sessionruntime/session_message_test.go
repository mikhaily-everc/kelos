package sessionruntime

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	kelos "github.com/kelos-dev/kelos/api/v1alpha2"
	"github.com/kelos-dev/kelos/internal/sessionupdate"
	kelosfake "github.com/kelos-dev/kelos/pkg/generated/clientset/versioned/fake"
	clientv1alpha2 "github.com/kelos-dev/kelos/pkg/generated/clientset/versioned/typed/api/v1alpha2"
)

const (
	testSessionMessageSessionUID = types.UID("session-uid")
	testSessionMessagePodUID     = types.UID("pod-current")
)

func testSessionMessage(name string, spec kelos.SessionMessageSpec) *kelos.SessionMessage {
	if spec.SessionName == "" {
		spec.SessionName = "chat"
	}
	return &kelos.SessionMessage{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID(name + "-uid"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: kelos.GroupVersion.String(),
				Kind:       "Session",
				Name:       spec.SessionName,
				UID:        testSessionMessageSessionUID,
			}},
		},
		Spec: spec,
	}
}

type sessionMessageHarness struct {
	t       *testing.T
	server  *Server
	journal *Journal
	relay   *sessionMessageRelay
	client  clientv1alpha2.SessionMessageInterface
	events  <-chan Event
}

func newSessionMessageHarness(t *testing.T, journal *Journal, provider Provider, objects ...runtime.Object) *sessionMessageHarness {
	t.Helper()
	if journal == nil {
		journal = NewJournal()
	}
	t.Cleanup(journal.Close)
	server := NewServer(Config{SessionName: "chat", PodUID: testSessionMessagePodUID}, journal, provider)
	client := kelosfake.NewSimpleClientset(objects...).ApiV1alpha2().SessionMessages("default")
	relay := newSessionMessageRelay(server, client, testSessionMessageSessionUID)
	retained, events, _, stop := journal.Subscribe(0)
	t.Cleanup(stop)
	list, err := client.List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	relay.replaceCache(list.Items)
	relay.recover(retained)
	relay.flush(t.Context())
	return &sessionMessageHarness{t: t, server: server, journal: journal, relay: relay, client: client, events: events}
}

func (h *sessionMessageHarness) create(message *kelos.SessionMessage) {
	h.t.Helper()
	created, err := h.client.Create(h.t.Context(), message, metav1.CreateOptions{})
	if err != nil {
		h.t.Fatal(err)
	}
	h.relay.observe(sessionMessageObservation{object: created})
}

func (h *sessionMessageHarness) sync() {
	for {
		select {
		case event := <-h.events:
			h.relay.apply(event)
			continue
		case result := <-h.relay.controls:
			h.relay.finishControl(result)
			continue
		default:
		}
		break
	}
	h.relay.flush(h.t.Context())
	h.relay.deliverPending(h.t.Context())
}

func (h *sessionMessageHarness) status(name string) kelos.SessionMessageStatus {
	h.t.Helper()
	message, err := h.client.Get(h.t.Context(), name, metav1.GetOptions{})
	if err != nil {
		h.t.Fatal(err)
	}
	return message.Status
}

func (h *sessionMessageHarness) waitPhase(name string, phase kelos.SessionMessagePhase) kelos.SessionMessageStatus {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		h.sync()
		status := h.status(name)
		if status.Phase == phase {
			return status
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("SessionMessage %q status = %#v, want phase %s", name, status, phase)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (h *sessionMessageHarness) runPendingTurn() {
	h.t.Helper()
	turn := <-h.server.turns
	<-turn.accepted
	pending, ok := h.server.takePendingTurn(turn)
	if !ok {
		h.t.Fatal("pending turn was not available")
	}
	h.server.runTurn(h.t.Context(), pending)
}

func TestSessionMessageRelayRunDeliversOwnedMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	journal := NewJournal()
	t.Cleanup(journal.Close)
	provider := &fakeProvider{}
	server := NewServer(Config{SessionName: "chat", PodUID: testSessionMessagePodUID}, journal, provider)
	foreign := testSessionMessage("b-foreign", kelos.SessionMessageSpec{SessionName: "other", Type: kelos.SessionMessageTypeMessage, Text: "not mine"})
	wrongOwner := testSessionMessage("c-wrong-owner", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "stale owner"})
	wrongOwner.OwnerReferences[0].UID = "old-session-uid"
	client := kelosfake.NewSimpleClientset(
		testSessionMessage("a-hello", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "hello"}),
		foreign,
		wrongOwner,
	).ApiV1alpha2().SessionMessages("default")
	go server.runTurns(ctx)
	go newSessionMessageRelay(server, client, testSessionMessageSessionUID).run(ctx)

	deadline := time.Now().Add(5 * time.Second)
	var message *kelos.SessionMessage
	for {
		var err error
		message, err = client.Get(ctx, "a-hello", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if message.Status.Phase == kelos.SessionMessagePhaseCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("status = %#v, want Completed", message.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	status := message.Status
	if status.PodUID != testSessionMessagePodUID || status.RequestID != "sessionmessage-a-hello-uid" || status.TurnID != "turn-1" {
		t.Fatalf("status identity = %#v", status)
	}
	if status.AcceptedAt == nil || status.CompletedAt == nil || status.Result == nil || status.Result.Text != "working done" || status.Result.Truncated {
		t.Fatalf("status result = %#v", status)
	}
	for _, name := range []string{"b-foreign", "c-wrong-owner"} {
		other, err := client.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if other.Status.Phase != "" {
			t.Fatalf("SessionMessage %q status = %#v, want untouched", name, other.Status)
		}
	}
	provider.mu.Lock()
	prompts := append([]string(nil), provider.prompts...)
	provider.mu.Unlock()
	if len(prompts) != 1 || prompts[0] != "hello" {
		t.Fatalf("provider prompts = %v", prompts)
	}
}

func TestSessionMessageRelayMergesAndRemovesPendingMessages(t *testing.T) {
	provider := &fakeProvider{}
	h := newSessionMessageHarness(t, nil, provider)
	h.create(testSessionMessage("m-1", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "first"}))
	h.create(testSessionMessage("m-2", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "second"}))
	h.create(testSessionMessage("m-3", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "third"}))
	h.sync()

	first := h.status("m-1")
	second := h.status("m-2")
	third := h.status("m-3")
	if first.Phase != kelos.SessionMessagePhaseAccepted || first.TurnID != "turn-1" || first.MergedInto != "" {
		t.Fatalf("first status = %#v", first)
	}
	if second.Phase != kelos.SessionMessagePhaseAccepted || second.TurnID != "turn-1" || second.MergedInto != "m-1" {
		t.Fatalf("second status = %#v", second)
	}
	if third.MergedInto != "m-1" {
		t.Fatalf("third status = %#v", third)
	}

	h.create(testSessionMessage("x-remove", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeRemove, TaskRef: "m-2"}))
	h.sync()
	if status := h.status("x-remove"); status.Phase != kelos.SessionMessagePhaseCompleted {
		t.Fatalf("remove status = %#v", status)
	}
	if status := h.status("m-2"); status.Phase != kelos.SessionMessagePhaseCanceled || status.Reason != kelos.SessionMessageReasonRemoved || status.CompletedAt == nil {
		t.Fatalf("removed message status = %#v", status)
	}
	if h.server.pendingTurn == nil || h.server.pendingTurn.text != "first\n\nthird" {
		t.Fatalf("pending turn = %#v", h.server.pendingTurn)
	}

	h.create(testSessionMessage("x-remove-first", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeRemove, TaskRef: "m-1"}))
	h.sync()
	if status := h.status("m-1"); status.Phase != kelos.SessionMessagePhaseCanceled {
		t.Fatalf("removed primary status = %#v", status)
	}
	if status := h.status("m-3"); status.Phase != kelos.SessionMessagePhaseAccepted || status.MergedInto != "" {
		t.Fatalf("remaining message status = %#v", status)
	}

	h.runPendingTurn()
	h.sync()
	if status := h.status("m-3"); status.Phase != kelos.SessionMessagePhaseCompleted || status.Result == nil || status.Result.Text != "working done" {
		t.Fatalf("completed status = %#v", status)
	}
	provider.mu.Lock()
	prompts := append([]string(nil), provider.prompts...)
	provider.mu.Unlock()
	if len(prompts) != 1 || prompts[0] != "third" {
		t.Fatalf("provider prompts = %v", prompts)
	}

	h.create(testSessionMessage("x-remove-late", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeRemove, TaskRef: "m-3"}))
	h.sync()
	if status := h.status("x-remove-late"); status.Phase != kelos.SessionMessagePhaseRejected || status.Reason != kelos.SessionMessageReasonNotPending {
		t.Fatalf("late remove status = %#v", status)
	}
}

func TestSessionMessageRelayRemovesWholePendingTurn(t *testing.T) {
	h := newSessionMessageHarness(t, nil, &fakeProvider{})
	h.create(testSessionMessage("m-1", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "only"}))
	h.sync()
	h.create(testSessionMessage("m-2-remove", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeRemove, TaskRef: "m-1"}))
	h.sync()
	if status := h.status("m-1"); status.Phase != kelos.SessionMessagePhaseCanceled || status.Reason != kelos.SessionMessageReasonRemoved {
		t.Fatalf("removed message status = %#v", status)
	}
	if h.server.pendingTurn != nil || h.server.outstanding != 0 {
		t.Fatalf("pending turn = %#v outstanding = %d", h.server.pendingTurn, h.server.outstanding)
	}
}

func TestSessionMessageRelayCancelsUndeliveredMessage(t *testing.T) {
	h := newSessionMessageHarness(t, nil, &fakeProvider{})
	h.server.submitMu.Lock()
	h.server.updateRequest = &sessionupdate.Request{ID: "update", PodUID: testSessionMessagePodUID}
	h.server.submitMu.Unlock()
	h.create(testSessionMessage("m-1", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "held"}))
	h.create(testSessionMessage("m-2-remove", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeRemove, TaskRef: "m-1"}))
	h.sync()
	if status := h.status("m-1"); status.Phase != kelos.SessionMessagePhaseCanceled || status.TurnID != "" {
		t.Fatalf("undelivered message status = %#v", status)
	}
	if status := h.status("m-2-remove"); status.Phase != kelos.SessionMessagePhaseCompleted {
		t.Fatalf("remove status = %#v", status)
	}
	if len(h.journal.Snapshot()) != 0 {
		t.Fatalf("journal = %#v, want no delivery", h.journal.Snapshot())
	}
}

func TestSessionMessageRelayLeavesMessagesPendingWhileDraining(t *testing.T) {
	h := newSessionMessageHarness(t, nil, &fakeProvider{})
	h.server.submitMu.Lock()
	h.server.idleDrainRequest = &sessionupdate.Request{ID: "drain", PodUID: testSessionMessagePodUID}
	h.server.submitMu.Unlock()
	h.create(testSessionMessage("m-1", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "first"}))
	h.create(testSessionMessage("m-2", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "second"}))
	h.sync()
	for _, name := range []string{"m-1", "m-2"} {
		if status := h.status(name); status.Phase != "" || status.PodUID != "" {
			t.Fatalf("SessionMessage %q status = %#v, want Pending", name, status)
		}
	}

	h.server.submitMu.Lock()
	h.server.idleDrainRequest = nil
	h.server.submitMu.Unlock()
	h.sync()
	if status := h.status("m-1"); status.Phase != kelos.SessionMessagePhaseAccepted || status.MergedInto != "" {
		t.Fatalf("first status = %#v", status)
	}
	if status := h.status("m-2"); status.Phase != kelos.SessionMessagePhaseAccepted || status.MergedInto != "m-1" {
		t.Fatalf("second status = %#v", status)
	}
}

func TestSessionMessageRelayWaitsBehindPendingCommand(t *testing.T) {
	h := newSessionMessageHarness(t, nil, &fakeProvider{})
	if err := h.server.submitClientMessage(t.Context(), "!true", "request-shell"); err != nil {
		t.Fatal(err)
	}
	h.create(testSessionMessage("m-1", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "after command"}))
	h.sync()
	if status := h.status("m-1"); status.Phase != "" {
		t.Fatalf("status behind pending command = %#v", status)
	}
	turn := <-h.server.turns
	<-turn.accepted
	pending, _ := h.server.takePendingTurn(turn)
	h.server.config.WorkingDir = t.TempDir()
	h.server.runTurn(t.Context(), pending)
	h.sync()
	if status := h.status("m-1"); status.Phase != kelos.SessionMessagePhaseAccepted || status.TurnID != "turn-2" {
		t.Fatalf("status after command = %#v", status)
	}
}

func TestSessionMessageRelayReportsAndAnswersInput(t *testing.T) {
	provider := &inputProvider{answers: make(chan map[string][]string, 1), done: make(chan struct{})}
	h := newSessionMessageHarness(t, nil, provider)
	h.create(testSessionMessage("m-1", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "ask me"}))
	h.sync()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.runPendingTurn()
	}()
	status := h.waitPhase("m-1", kelos.SessionMessagePhaseInputRequired)
	if status.Input == nil || status.Input.ID != "input-test" || len(status.Input.Questions) != 2 || !status.Input.Questions[1].MultiSelect {
		t.Fatalf("input status = %#v", status.Input)
	}

	h.create(testSessionMessage("m-2-input", kelos.SessionMessageSpec{
		Type:    kelos.SessionMessageTypeInput,
		TaskRef: "m-1",
		Answers: map[string]kelos.SessionInputAnswer{"first": {"a"}},
	}))
	h.sync()
	if status := h.status("m-2-input"); status.Phase != kelos.SessionMessagePhaseCompleted {
		t.Fatalf("partial input status = %#v", status)
	}
	h.create(testSessionMessage("m-3-input", kelos.SessionMessageSpec{
		Type:    kelos.SessionMessageTypeInput,
		InputID: "input-test",
		Answers: map[string]kelos.SessionInputAnswer{"second": {"b", "c"}},
	}))
	h.sync()
	<-done
	answers := <-provider.answers
	if len(answers["first"]) != 1 || len(answers["second"]) != 2 {
		t.Fatalf("provider answers = %#v", answers)
	}
	status = h.waitPhase("m-1", kelos.SessionMessagePhaseCompleted)
	if status.Input != nil || status.Result == nil || status.Result.Text != "answers received" {
		t.Fatalf("completed status = %#v", status)
	}

	h.create(testSessionMessage("m-4-input", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeInput, InputID: "input-test", Cancel: true}))
	h.sync()
	if status := h.status("m-4-input"); status.Phase != kelos.SessionMessagePhaseRejected || status.Reason != kelos.SessionMessageReasonNoPendingInput {
		t.Fatalf("stale input status = %#v", status)
	}
}

func TestSessionMessageRelayInterruptsTurn(t *testing.T) {
	provider := &interruptProvider{started: make(chan struct{}), interrupted: make(chan struct{}), done: make(chan struct{})}
	h := newSessionMessageHarness(t, nil, provider)
	h.create(testSessionMessage("m-0-interrupt", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeInterrupt}))
	h.create(testSessionMessage("m-1", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "long"}))
	h.sync()
	h.waitPhase("m-0-interrupt", kelos.SessionMessagePhaseRejected)
	if status := h.status("m-0-interrupt"); status.Reason != kelos.SessionMessageReasonNoActiveTurn {
		t.Fatalf("idle interrupt status = %#v", status)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.runPendingTurn()
	}()
	<-provider.started
	h.waitPhase("m-1", kelos.SessionMessagePhaseWorking)
	h.create(testSessionMessage("m-2-interrupt", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeInterrupt, TaskRef: "m-1"}))
	h.sync()
	<-done
	h.waitPhase("m-2-interrupt", kelos.SessionMessagePhaseCompleted)
	status := h.waitPhase("m-1", kelos.SessionMessagePhaseCanceled)
	if status.Reason != kelos.SessionMessageReasonInterrupted {
		t.Fatalf("interrupted status = %#v", status)
	}
}

func TestSessionMessageRelayRecoversAcrossRuntimeRestart(t *testing.T) {
	path := t.TempDir() + "/events.jsonl"
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []Event{
		{Type: EventUserMessage, RequestID: "sessionmessage-a-done-uid", TurnID: "turn-1", Text: "done", Revision: 1},
		{Type: EventTurnStarted, TurnID: "turn-1", Status: "running"},
		{Type: EventAssistantMessage, TurnID: "turn-1", Text: "finished before restart"},
		{Type: EventTurnCompleted, TurnID: "turn-1", Status: "completed"},
		{Type: EventUserMessage, RequestID: "sessionmessage-b-working-uid", TurnID: "turn-2", Text: "working", Revision: 1},
		{Type: EventTurnStarted, TurnID: "turn-2", Status: "running"},
		{Type: EventUserMessage, RequestID: "sessionmessage-c-unwritten-uid", TurnID: "turn-3", Text: "unwritten", Revision: 1},
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

	accepted := func(name, turnID string, phase kelos.SessionMessagePhase, podUID types.UID) *kelos.SessionMessage {
		message := testSessionMessage(name, kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: name})
		now := metav1.Now()
		message.Status = kelos.SessionMessageStatus{Phase: phase, PodUID: podUID, RequestID: "sessionmessage-" + name + "-uid", TurnID: turnID, AcceptedAt: &now}
		return message
	}
	completed := accepted("e-completed", "turn-0", kelos.SessionMessagePhaseCompleted, "pod-old")
	provider := &fakeProvider{}
	h := newSessionMessageHarness(t, journal, provider,
		accepted("a-done", "turn-1", kelos.SessionMessagePhaseWorking, "pod-old"),
		accepted("b-working", "turn-2", kelos.SessionMessagePhaseWorking, "pod-old"),
		testSessionMessage("c-unwritten", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "unwritten"}),
		accepted("d-lost", "turn-9", kelos.SessionMessagePhaseAccepted, "pod-old"),
		completed,
	)
	if err := h.server.restoreTurn(recovery.pendingTurn); err != nil {
		t.Fatal(err)
	}
	h.server.nextTurnID.Store(recovery.nextTurnID)
	h.sync()

	if status := h.status("a-done"); status.Phase != kelos.SessionMessagePhaseCompleted || status.PodUID != "pod-old" || status.Result == nil || status.Result.Text != "finished before restart" {
		t.Fatalf("finished message status = %#v", status)
	}
	if status := h.status("b-working"); status.Phase != kelos.SessionMessagePhaseFailed || status.Reason != kelos.SessionMessageReasonPodLost {
		t.Fatalf("interrupted message status = %#v", status)
	}
	if status := h.status("c-unwritten"); status.Phase != kelos.SessionMessagePhaseAccepted || status.TurnID != "turn-3" || status.PodUID != testSessionMessagePodUID {
		t.Fatalf("adopted message status = %#v", status)
	}
	if status := h.status("d-lost"); status.Phase != kelos.SessionMessagePhaseAccepted || status.TurnID != "turn-3" || status.MergedInto != "c-unwritten" || status.PodUID != testSessionMessagePodUID {
		t.Fatalf("redelivered message status = %#v", status)
	}
	if status := h.status("e-completed"); status.Phase != kelos.SessionMessagePhaseCompleted || status.PodUID != "pod-old" {
		t.Fatalf("terminal message status = %#v", status)
	}

	h.runPendingTurn()
	h.sync()
	for _, name := range []string{"c-unwritten", "d-lost"} {
		if status := h.status(name); status.Phase != kelos.SessionMessagePhaseCompleted {
			t.Fatalf("SessionMessage %q status = %#v", name, status)
		}
	}
	provider.mu.Lock()
	prompts := append([]string(nil), provider.prompts...)
	provider.mu.Unlock()
	if len(prompts) != 1 || prompts[0] != "unwritten\n\nd-lost" {
		t.Fatalf("provider prompts = %q", prompts)
	}
}

func TestSessionMessageRelayRejectsInvalidRequests(t *testing.T) {
	h := newSessionMessageHarness(t, nil, &fakeProvider{})
	h.create(testSessionMessage("m-1", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeMessage, Text: "   "}))
	h.create(testSessionMessage("m-2", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeInput, TaskRef: "missing", Cancel: true}))
	h.create(testSessionMessage("m-3", kelos.SessionMessageSpec{Type: kelos.SessionMessageTypeInterrupt, TaskRef: "m-1"}))
	h.sync()
	if status := h.status("m-1"); status.Phase != kelos.SessionMessagePhaseRejected || status.Reason != kelos.SessionMessageReasonInvalidRequest || !strings.Contains(status.Message, "must not be empty") {
		t.Fatalf("empty message status = %#v", status)
	}
	if status := h.status("m-2"); status.Phase != kelos.SessionMessagePhaseRejected || status.Reason != kelos.SessionMessageReasonTaskNotFound {
		t.Fatalf("missing target status = %#v", status)
	}
	if status := h.status("m-3"); status.Phase != kelos.SessionMessagePhaseRejected || status.Reason != kelos.SessionMessageReasonNoActiveTurn {
		t.Fatalf("inactive interrupt status = %#v", status)
	}
}

func TestSessionMessageTurnResultIsTruncated(t *testing.T) {
	turn := &sessionMessageTurn{}
	turn.segmentOpen = true
	turn.appendSegment(strings.Repeat("é", maxSessionMessageResultRunes+10))
	turn.closeSegment()
	result := turn.result()
	if !result.Truncated || utf8.RuneCountInString(result.Text) != maxSessionMessageResultRunes || !utf8.ValidString(result.Text) {
		t.Fatalf("result truncated=%v runes=%d", result.Truncated, utf8.RuneCountInString(result.Text))
	}

	short := &sessionMessageTurn{}
	short.segmentOpen = true
	short.appendSegment("partial ")
	short.appendSegment("text")
	short.closeSegment()
	short.segmentOpen = true
	short.segment = ""
	short.appendSegment("final")
	if result := short.result(); result.Truncated || result.Text != "final" {
		t.Fatalf("short result = %#v", result)
	}
}

func TestRemoveMergedMessageText(t *testing.T) {
	for _, tt := range []struct {
		current, part, want string
		ok                  bool
	}{
		{"only", "only", "", true},
		{"a\n\nb\n\nc", "a", "b\n\nc", true},
		{"a\n\nb\n\nc", "b", "a\n\nc", true},
		{"a\n\nb\n\nc", "c", "a\n\nb", true},
		{"a\n\nb", "x", "a\n\nb", false},
	} {
		got, ok := removeMergedMessageText(tt.current, tt.part)
		if got != tt.want || ok != tt.ok {
			t.Fatalf("removeMergedMessageText(%q, %q) = %q, %v", tt.current, tt.part, got, ok)
		}
	}
}
