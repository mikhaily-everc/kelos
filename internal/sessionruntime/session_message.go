package sessionruntime

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/retry"

	kelos "github.com/kelos-dev/kelos/api/v1alpha2"
	clientv1alpha2 "github.com/kelos-dev/kelos/pkg/generated/clientset/versioned/typed/api/v1alpha2"
)

const (
	sessionMessageRequestPrefix     = "sessionmessage-"
	sessionMessageResyncInterval    = 10 * time.Second
	maxSessionMessageResultRunes    = 16384
	maxSessionMessageResultBytes    = 4 * maxSessionMessageResultRunes
	maxSessionMessageStatusMsgRunes = 1024
)

// sessionMessageTurn is the SessionMessage view of one journal turn.
type sessionMessageTurn struct {
	known        bool
	started      bool
	done         bool
	status       string
	interrupting bool
	input        *kelos.SessionMessageInput
	errorText    string
	segment      string
	segmentOpen  bool
	lastText     string
	truncated    bool
	messages     []string
}

type sessionMessageRequest struct {
	turnID string
	merged bool
}

// trackedSessionMessage is a SessionMessage whose status this runtime owns.
type trackedSessionMessage struct {
	name    string
	uid     types.UID
	text    string
	turnID  string
	final   bool
	status  kelos.SessionMessageStatus
	written *kelos.SessionMessageStatus
}

type sessionMessageControlResult struct {
	message *kelos.SessionMessage
	err     error
}

type sessionMessageObservation struct {
	object  *kelos.SessionMessage
	deleted bool
	list    []kelos.SessionMessage
	resync  bool
}

// sessionMessageRelay delivers SessionMessage objects owned by this runtime's
// Session through the same code paths as socket client requests and reports
// their progress in SessionMessage status.
type sessionMessageRelay struct {
	server     *Server
	client     clientv1alpha2.SessionMessageInterface
	sessionUID types.UID
	now        func() time.Time

	cache       map[string]*kelos.SessionMessage
	tracked     map[string]*trackedSessionMessage
	handled     map[types.UID]struct{}
	redeliver   map[types.UID]struct{}
	turns       map[string]*sessionMessageTurn
	inputs      map[string]string
	requests    map[string]sessionMessageRequest
	merged      []string
	lastEventID int64
	replaying   bool
	controls    chan sessionMessageControlResult
}

func newSessionMessageRelay(server *Server, client clientv1alpha2.SessionMessageInterface, sessionUID types.UID) *sessionMessageRelay {
	return &sessionMessageRelay{
		server:     server,
		client:     client,
		sessionUID: sessionUID,
		now:        time.Now,
		cache:      map[string]*kelos.SessionMessage{},
		tracked:    map[string]*trackedSessionMessage{},
		handled:    map[types.UID]struct{}{},
		redeliver:  map[types.UID]struct{}{},
		turns:      map[string]*sessionMessageTurn{},
		inputs:     map[string]string{},
		requests:   map[string]sessionMessageRequest{},
		controls:   make(chan sessionMessageControlResult, 16),
	}
}

func (s *Server) runSessionMessages(ctx context.Context) {
	sessionUID := s.config.SessionUID
	for sessionUID == "" && s.config.SessionClient != nil {
		session, err := s.config.SessionClient.Get(ctx, s.config.SessionName, metav1.GetOptions{})
		if err == nil {
			sessionUID = session.UID
			break
		}
		log.Printf("Getting Session for SessionMessage delivery failed error=%v", err)
		if !waitForSessionUpdateRetry(ctx) {
			return
		}
	}
	if sessionUID == "" {
		log.Printf("SessionMessage delivery is disabled because the Session UID is unknown")
		return
	}
	newSessionMessageRelay(s, s.config.SessionMessageClient, sessionUID).run(ctx)
}

func (r *sessionMessageRelay) run(ctx context.Context) {
	retained, events, overflow, stop := r.server.journal.Subscribe(0)
	defer func() { stop() }()
	var list *kelos.SessionMessageList
	for {
		var err error
		list, err = r.client.List(ctx, metav1.ListOptions{})
		if err == nil {
			break
		}
		if ctx.Err() == nil {
			log.Printf("Listing SessionMessages failed error=%v", err)
		}
		if !waitForSessionUpdateRetry(ctx) {
			return
		}
	}
	r.replaceCache(list.Items)
	r.recover(retained)
	r.flush(ctx)
	r.deliverPending(ctx)

	observations := make(chan sessionMessageObservation, 64)
	go r.watch(ctx, list.ResourceVersion, observations)
	ticker := time.NewTicker(sessionMessageResyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				select {
				case <-overflow:
				default:
					return
				}
				stop()
				retained, events, overflow, stop = r.server.journal.Subscribe(r.lastEventID)
				for _, event := range retained {
					r.apply(event)
				}
				r.flush(ctx)
				r.deliverPending(ctx)
				continue
			}
			r.apply(event)
			r.flush(ctx)
			switch event.Type {
			case EventTurnStarted, EventTurnCompleted, EventUserMessageRemoved:
				r.deliverPending(ctx)
			}
		case observation := <-observations:
			r.observe(observation)
			r.deliverPending(ctx)
		case result := <-r.controls:
			r.finishControl(result)
			r.flush(ctx)
		case <-ticker.C:
			r.flush(ctx)
			r.deliverPending(ctx)
		}
	}
}

func (r *sessionMessageRelay) watch(ctx context.Context, resourceVersion string, out chan<- sessionMessageObservation) {
	send := func(observation sessionMessageObservation) bool {
		select {
		case out <- observation:
			return true
		case <-ctx.Done():
			return false
		}
	}
	for ctx.Err() == nil {
		if resourceVersion == "" {
			list, err := r.client.List(ctx, metav1.ListOptions{})
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("Listing SessionMessages failed error=%v", err)
				}
				if !waitForSessionUpdateRetry(ctx) {
					return
				}
				continue
			}
			if !send(sessionMessageObservation{list: list.Items, resync: true}) {
				return
			}
			resourceVersion = list.ResourceVersion
		}
		watcher, err := r.client.Watch(ctx, metav1.ListOptions{ResourceVersion: resourceVersion, AllowWatchBookmarks: true})
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("Watching SessionMessages failed error=%v", err)
			}
			resourceVersion = ""
			if !waitForSessionUpdateRetry(ctx) {
				return
			}
			continue
		}
		resourceVersion = r.consumeWatch(ctx, watcher, resourceVersion, send)
		watcher.Stop()
	}
}

func (r *sessionMessageRelay) consumeWatch(ctx context.Context, watcher watch.Interface, resourceVersion string, send func(sessionMessageObservation) bool) string {
	for {
		select {
		case <-ctx.Done():
			return resourceVersion
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return resourceVersion
			}
			if event.Type == watch.Error {
				return ""
			}
			message, ok := event.Object.(*kelos.SessionMessage)
			if !ok {
				continue
			}
			resourceVersion = message.ResourceVersion
			if event.Type == watch.Bookmark {
				continue
			}
			if !send(sessionMessageObservation{object: message, deleted: event.Type == watch.Deleted}) {
				return resourceVersion
			}
		}
	}
}

func (r *sessionMessageRelay) owns(message *kelos.SessionMessage) bool {
	if message.Spec.SessionName != r.server.config.SessionName {
		return false
	}
	for _, reference := range message.OwnerReferences {
		if reference.Kind == "Session" && reference.UID == r.sessionUID {
			return true
		}
	}
	return false
}

func (r *sessionMessageRelay) replaceCache(items []kelos.SessionMessage) {
	r.cache = map[string]*kelos.SessionMessage{}
	present := map[types.UID]struct{}{}
	for index := range items {
		message := &items[index]
		if !r.owns(message) {
			continue
		}
		r.cache[message.Name] = message
		present[message.UID] = struct{}{}
	}
	for uid := range r.handled {
		if _, ok := present[uid]; !ok {
			delete(r.handled, uid)
		}
	}
}

func (r *sessionMessageRelay) observe(observation sessionMessageObservation) {
	if observation.resync {
		r.replaceCache(observation.list)
		return
	}
	message := observation.object
	if message == nil || !r.owns(message) {
		return
	}
	if observation.deleted {
		if current := r.cache[message.Name]; current != nil && current.UID == message.UID {
			delete(r.cache, message.Name)
		}
		delete(r.handled, message.UID)
		delete(r.redeliver, message.UID)
		return
	}
	r.cache[message.Name] = message
}

func (r *sessionMessageRelay) sortedNames() []string {
	names := make([]string, 0, len(r.cache))
	for name := range r.cache {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sessionMessagePending(message *kelos.SessionMessage) bool {
	return message.Status.Phase == "" || message.Status.Phase == kelos.SessionMessagePhasePending
}

func sessionMessageTerminal(phase kelos.SessionMessagePhase) bool {
	switch phase {
	case kelos.SessionMessagePhaseCompleted, kelos.SessionMessagePhaseFailed, kelos.SessionMessagePhaseCanceled, kelos.SessionMessagePhaseRejected:
		return true
	}
	return false
}

func sessionMessageRequestID(message *kelos.SessionMessage) string {
	return sessionMessageRequestPrefix + string(message.UID)
}

// recover restores tracking for messages accepted before this process started
// and replays the retained journal to bring their status up to date.
func (r *sessionMessageRelay) recover(retained []Event) {
	recovered := make([]*trackedSessionMessage, 0)
	for _, name := range r.sortedNames() {
		message := r.cache[name]
		if sessionMessagePending(message) || sessionMessageTerminal(message.Status.Phase) {
			continue
		}
		if message.Status.PodUID == "" || message.Status.TurnID == "" {
			continue
		}
		tracked := &trackedSessionMessage{name: message.Name, uid: message.UID, text: message.Spec.Text, turnID: message.Status.TurnID}
		tracked.status = *message.Status.DeepCopy()
		written := *message.Status.DeepCopy()
		tracked.written = &written
		r.tracked[message.Name] = tracked
		r.handled[message.UID] = struct{}{}
		turn := r.turn(tracked.turnID)
		turn.messages = append(turn.messages, message.Name)
		recovered = append(recovered, tracked)
	}

	r.replaying = true
	for _, event := range retained {
		r.apply(event)
	}
	r.replaying = false

	for _, name := range r.sortedNames() {
		message := r.cache[name]
		if !sessionMessagePending(message) || message.Spec.Type != kelos.SessionMessageTypeMessage {
			continue
		}
		if request, ok := r.requests[sessionMessageRequestID(message)]; ok {
			r.track(message, request.turnID, request.merged)
		}
	}

	for _, tracked := range recovered {
		turn := r.turns[tracked.turnID]
		podLost := tracked.status.PodUID != r.server.config.PodUID
		if turn == nil || !turn.known {
			if podLost && tracked.status.Phase == kelos.SessionMessagePhaseAccepted {
				r.untrack(tracked)
				r.redeliver[tracked.uid] = struct{}{}
				delete(r.handled, tracked.uid)
				continue
			}
			reason, text := kelos.SessionMessageReasonTurnLost, "the Session turn carrying this message is no longer recorded"
			if podLost {
				reason, text = kelos.SessionMessageReasonPodLost, "the Session Pod processing this message was lost"
			}
			r.finish(tracked, kelos.SessionMessagePhaseFailed, reason, text)
			continue
		}
		if !turn.done {
			tracked.status.PodUID = r.server.config.PodUID
		}
		r.refresh(tracked)
	}
	for id, turn := range r.turns {
		if (turn.done || !turn.known) && len(turn.messages) == 0 {
			r.forgetTurn(id)
		}
	}
	r.requests = map[string]sessionMessageRequest{}
}

func (r *sessionMessageRelay) turn(id string) *sessionMessageTurn {
	turn := r.turns[id]
	if turn == nil {
		turn = &sessionMessageTurn{}
		r.turns[id] = turn
	}
	return turn
}

func (r *sessionMessageRelay) forgetTurn(id string) {
	turn := r.turns[id]
	if turn != nil && turn.input != nil {
		delete(r.inputs, turn.input.ID)
	}
	delete(r.turns, id)
}

// apply folds one journal event into the turn state and refreshes the status
// of every message carried by the affected turn.
func (r *sessionMessageRelay) apply(event Event) {
	if event.ID != 0 {
		if event.ID <= r.lastEventID {
			return
		}
		r.lastEventID = event.ID
	}
	turnID := event.TurnID
	if event.Type == EventInputResolved && turnID == "" {
		turnID = r.inputs[event.InputID]
	}
	if turnID == "" {
		return
	}
	if r.replaying && strings.HasPrefix(event.RequestID, sessionMessageRequestPrefix) && (event.Type == EventUserMessage || event.Type == EventUserMessageUpdated) {
		if _, exists := r.requests[event.RequestID]; !exists {
			r.requests[event.RequestID] = sessionMessageRequest{turnID: turnID, merged: event.Type == EventUserMessageUpdated}
		}
	}
	if _, exists := r.turns[turnID]; !exists && !r.replaying {
		// Live turns matter only once a SessionMessage is tracked on them.
		return
	}
	turn := r.turn(turnID)
	turn.known = true
	switch event.Type {
	case EventUserMessageUpdated:
		if event.RequestID == "" && len(r.merged) > 0 {
			for _, name := range r.merged {
				if tracked := r.tracked[name]; tracked != nil {
					tracked.turnID = turnID
					turn.messages = append(turn.messages, name)
				}
			}
			r.merged = nil
			r.repointMerged(turn)
		}
	case EventTurnStarted:
		turn.started = true
	case EventTurnInterrupting:
		turn.interrupting = true
	case EventInputRequested:
		turn.closeSegment()
		input := &kelos.SessionMessageInput{ID: event.InputID}
		for _, question := range event.Questions {
			converted := kelos.SessionInputQuestion{
				ID:          question.ID,
				Header:      question.Header,
				Question:    question.Question,
				MultiSelect: question.MultiSelect,
				Secret:      question.Secret,
			}
			for _, option := range question.Options {
				converted.Options = append(converted.Options, kelos.SessionInputOption{Label: option.Label, Description: option.Description})
			}
			input.Questions = append(input.Questions, converted)
		}
		turn.input = input
		r.inputs[event.InputID] = turnID
	case EventInputResolved:
		if turn.input != nil && turn.input.ID == event.InputID {
			turn.input = nil
		}
		delete(r.inputs, event.InputID)
	case EventAssistantDelta:
		if !turn.segmentOpen {
			turn.segment = ""
			turn.segmentOpen = true
		}
		turn.appendSegment(event.Text)
	case EventAssistantMessage:
		if event.Text != "" {
			turn.segment = ""
			turn.truncated = false
			turn.appendSegment(event.Text)
		}
		turn.segmentOpen = true
		turn.closeSegment()
	case EventError:
		turn.closeSegment()
		turn.errorText = event.Text
	case EventTurnCompleted:
		turn.closeSegment()
		turn.done = true
		turn.status = event.Status
		if event.Status == "merged" {
			r.merged = append(r.merged, turn.messages...)
			turn.messages = nil
		}
	case EventUserMessageRemoved:
		turn.done = true
		turn.status = "removed"
	case EventUserMessage:
	default:
		turn.closeSegment()
	}
	for _, name := range append([]string(nil), turn.messages...) {
		if tracked := r.tracked[name]; tracked != nil {
			r.refresh(tracked)
		}
	}
	if turn.done && len(turn.messages) == 0 && !r.replaying {
		r.forgetTurn(turnID)
	}
}

func (t *sessionMessageTurn) appendSegment(text string) {
	if len(t.segment)+len(text) > maxSessionMessageResultBytes {
		text = text[:max(0, maxSessionMessageResultBytes-len(t.segment))]
		t.truncated = true
	}
	t.segment += text
}

func (t *sessionMessageTurn) closeSegment() {
	if !t.segmentOpen {
		return
	}
	t.segmentOpen = false
	t.lastText = t.segment
}

func (t *sessionMessageTurn) result() *kelos.SessionMessageResult {
	text := t.lastText
	if t.segmentOpen {
		text = t.segment
	}
	text = strings.ToValidUTF8(text, "")
	text, truncated := truncateRunes(text, maxSessionMessageResultRunes)
	return &kelos.SessionMessageResult{Text: text, Truncated: truncated || t.truncated}
}

func truncateRunes(text string, limit int) (string, bool) {
	if utf8.RuneCountInString(text) <= limit {
		return text, false
	}
	count := 0
	for index := range text {
		if count == limit {
			return text[:index], true
		}
		count++
	}
	return text, false
}

// refresh derives a tracked message's status from the state of its turn.
func (r *sessionMessageRelay) refresh(tracked *trackedSessionMessage) {
	if tracked.final {
		return
	}
	turn := r.turns[tracked.turnID]
	if turn == nil {
		return
	}
	status := &tracked.status
	status.Input = nil
	status.Result = nil
	status.Reason = ""
	status.Message = ""
	switch {
	case turn.done:
		switch turn.status {
		case "completed":
			status.Result = turn.result()
			r.finish(tracked, kelos.SessionMessagePhaseCompleted, "", "")
		case "interrupted":
			if turn.interrupting {
				r.finish(tracked, kelos.SessionMessagePhaseCanceled, kelos.SessionMessageReasonInterrupted, "the turn was interrupted")
			} else {
				r.finish(tracked, kelos.SessionMessagePhaseFailed, kelos.SessionMessageReasonPodLost, "the Session runtime restarted while the turn was running")
			}
		case "removed":
			r.finish(tracked, kelos.SessionMessagePhaseCanceled, kelos.SessionMessageReasonRemoved, "the pending turn was removed")
		default:
			message := turn.errorText
			if message == "" {
				message = fmt.Sprintf("the turn finished with status %q", turn.status)
			}
			r.finish(tracked, kelos.SessionMessagePhaseFailed, kelos.SessionMessageReasonTurnFailed, message)
		}
	case turn.input != nil:
		status.Phase = kelos.SessionMessagePhaseInputRequired
		status.Input = turn.input.DeepCopy()
	case turn.started:
		status.Phase = kelos.SessionMessagePhaseWorking
	default:
		status.Phase = kelos.SessionMessagePhaseAccepted
	}
}

func (r *sessionMessageRelay) finish(tracked *trackedSessionMessage, phase kelos.SessionMessagePhase, reason, message string) {
	message, _ = truncateRunes(message, maxSessionMessageStatusMsgRunes)
	tracked.final = true
	tracked.status.Phase = phase
	tracked.status.Reason = reason
	tracked.status.Message = message
	tracked.status.Input = nil
	if phase != kelos.SessionMessagePhaseCompleted {
		tracked.status.Result = nil
	}
	if tracked.status.CompletedAt == nil {
		now := metav1.NewTime(r.now())
		tracked.status.CompletedAt = &now
	}
}

func (r *sessionMessageRelay) track(message *kelos.SessionMessage, turnID string, merged bool) *trackedSessionMessage {
	now := metav1.NewTime(r.now())
	tracked := &trackedSessionMessage{name: message.Name, uid: message.UID, text: message.Spec.Text, turnID: turnID}
	tracked.status = kelos.SessionMessageStatus{
		Phase:      kelos.SessionMessagePhaseAccepted,
		PodUID:     r.server.config.PodUID,
		RequestID:  sessionMessageRequestID(message),
		TurnID:     turnID,
		AcceptedAt: &now,
	}
	turn := r.turn(turnID)
	turn.known = true
	if merged {
		tracked.status.MergedInto = r.primary(turn)
	}
	turn.messages = append(turn.messages, message.Name)
	r.tracked[message.Name] = tracked
	r.handled[message.UID] = struct{}{}
	delete(r.redeliver, message.UID)
	r.refresh(tracked)
	return tracked
}

// trackControl records the terminal status of a control message.
func (r *sessionMessageRelay) trackControl(message *kelos.SessionMessage, phase kelos.SessionMessagePhase, reason, text string) {
	now := metav1.NewTime(r.now())
	tracked := &trackedSessionMessage{name: message.Name, uid: message.UID}
	tracked.status = kelos.SessionMessageStatus{
		PodUID:     r.server.config.PodUID,
		RequestID:  sessionMessageRequestID(message),
		AcceptedAt: &now,
	}
	r.tracked[message.Name] = tracked
	r.handled[message.UID] = struct{}{}
	delete(r.redeliver, message.UID)
	r.finish(tracked, phase, reason, text)
}

func (r *sessionMessageRelay) untrack(tracked *trackedSessionMessage) {
	delete(r.tracked, tracked.name)
	turn := r.turns[tracked.turnID]
	if turn == nil {
		return
	}
	for index, name := range turn.messages {
		if name == tracked.name {
			turn.messages = append(turn.messages[:index], turn.messages[index+1:]...)
			break
		}
	}
	if turn.done && len(turn.messages) == 0 && !r.replaying {
		r.forgetTurn(tracked.turnID)
	}
}

// primary returns the first non-final message on a turn, which later
// submissions report as their mergedInto target.
func (r *sessionMessageRelay) primary(turn *sessionMessageTurn) string {
	for _, name := range turn.messages {
		if tracked := r.tracked[name]; tracked != nil && !tracked.final {
			return name
		}
	}
	return ""
}

func (r *sessionMessageRelay) repointMerged(turn *sessionMessageTurn) {
	primary := r.primary(turn)
	for _, name := range turn.messages {
		tracked := r.tracked[name]
		if tracked == nil || tracked.final {
			continue
		}
		if name == primary {
			tracked.status.MergedInto = ""
		} else {
			tracked.status.MergedInto = primary
		}
	}
}

// deliverPending delivers undelivered messages in name order. A message that
// the runtime cannot accept yet stays Pending, and later messages wait for it.
func (r *sessionMessageRelay) deliverPending(ctx context.Context) {
	blocked := false
	for _, name := range r.sortedNames() {
		message := r.cache[name]
		if _, done := r.handled[message.UID]; done || message.DeletionTimestamp != nil {
			continue
		}
		if _, again := r.redeliver[message.UID]; !again && !sessionMessagePending(message) {
			continue
		}
		switch message.Spec.Type {
		case kelos.SessionMessageTypeMessage:
			if blocked {
				continue
			}
			blocked = !r.deliverMessage(message)
		case kelos.SessionMessageTypeInput:
			r.deliverInput(message)
		case kelos.SessionMessageTypeInterrupt:
			r.deliverInterrupt(ctx, message)
		case kelos.SessionMessageTypeRemove:
			r.deliverRemove(message)
		default:
			r.trackControl(message, kelos.SessionMessagePhaseRejected, kelos.SessionMessageReasonInvalidRequest, fmt.Sprintf("unsupported SessionMessage type %q", message.Spec.Type))
		}
	}
	r.flush(ctx)
}

func sessionMessageRetryable(err error) bool {
	return errors.Is(err, errSessionReclaiming) ||
		errors.Is(err, errSessionDraining) ||
		errors.Is(err, errSessionProviderStopped) ||
		errors.Is(err, errSessionPendingCommand) ||
		errors.Is(err, errSessionPendingMessage)
}

// deliverMessage reports whether the message left the Pending phase.
func (r *sessionMessageRelay) deliverMessage(message *kelos.SessionMessage) bool {
	requestID := sessionMessageRequestID(message)
	turnID, merged, err := r.server.submitParsedMessageTurn(message.Spec.Text, requestID, sessionCommand{kind: sessionCommandMessage, text: message.Spec.Text})
	if err != nil {
		if sessionMessageRetryable(err) || r.server.journal.Err() != nil {
			return false
		}
		r.trackControl(message, kelos.SessionMessagePhaseRejected, kelos.SessionMessageReasonInvalidRequest, err.Error())
		return true
	}
	r.track(message, turnID, merged)
	return true
}

// target resolves spec.taskRef to a message of type message for this Session.
func (r *sessionMessageRelay) target(message *kelos.SessionMessage) (*kelos.SessionMessage, *trackedSessionMessage, bool) {
	name := message.Spec.TaskRef
	if name == "" {
		return nil, nil, false
	}
	target := r.cache[name]
	tracked := r.tracked[name]
	if target == nil && tracked == nil {
		return nil, nil, false
	}
	if target != nil && target.Spec.Type != kelos.SessionMessageTypeMessage {
		return nil, nil, false
	}
	return target, tracked, true
}

func (r *sessionMessageRelay) deliverInput(message *kelos.SessionMessage) {
	inputID := message.Spec.InputID
	if inputID == "" {
		_, tracked, ok := r.target(message)
		if !ok {
			r.trackControl(message, kelos.SessionMessagePhaseRejected, kelos.SessionMessageReasonTaskNotFound, fmt.Sprintf("SessionMessage %q was not found for this Session", message.Spec.TaskRef))
			return
		}
		var turn *sessionMessageTurn
		if tracked != nil {
			turn = r.turns[tracked.turnID]
		}
		if turn == nil || turn.input == nil {
			r.trackControl(message, kelos.SessionMessagePhaseRejected, kelos.SessionMessageReasonNoPendingInput, fmt.Sprintf("SessionMessage %q has no pending input request", message.Spec.TaskRef))
			return
		}
		inputID = turn.input.ID
	}
	answers := make(map[string][]string, len(message.Spec.Answers))
	for questionID, values := range message.Spec.Answers {
		answers[questionID] = append([]string(nil), values...)
	}
	if err := r.server.resolveInput(inputID, answers, message.Spec.Cancel, sessionMessageRequestID(message)); err != nil {
		r.trackControl(message, kelos.SessionMessagePhaseRejected, kelos.SessionMessageReasonNoPendingInput, err.Error())
		return
	}
	r.trackControl(message, kelos.SessionMessagePhaseCompleted, "", "")
}

func (r *sessionMessageRelay) deliverInterrupt(ctx context.Context, message *kelos.SessionMessage) {
	if message.Spec.TaskRef != "" {
		_, tracked, ok := r.target(message)
		if !ok {
			r.trackControl(message, kelos.SessionMessagePhaseRejected, kelos.SessionMessageReasonTaskNotFound, fmt.Sprintf("SessionMessage %q was not found for this Session", message.Spec.TaskRef))
			return
		}
		r.server.activeMu.Lock()
		active := r.server.activeTurn
		r.server.activeMu.Unlock()
		if tracked == nil || tracked.final || tracked.turnID != active {
			r.trackControl(message, kelos.SessionMessagePhaseRejected, kelos.SessionMessageReasonNoActiveTurn, fmt.Sprintf("the turn carrying SessionMessage %q is not running", message.Spec.TaskRef))
			return
		}
	}
	r.handled[message.UID] = struct{}{}
	delete(r.redeliver, message.UID)
	requestID := sessionMessageRequestID(message)
	go func() {
		err := r.server.interruptTurn(ctx, requestID)
		select {
		case r.controls <- sessionMessageControlResult{message: message, err: err}:
		case <-ctx.Done():
		}
	}()
}

func (r *sessionMessageRelay) finishControl(result sessionMessageControlResult) {
	switch {
	case result.err == nil:
		r.trackControl(result.message, kelos.SessionMessagePhaseCompleted, "", "")
	case errors.Is(result.err, ErrNoActiveTurn):
		r.trackControl(result.message, kelos.SessionMessagePhaseRejected, kelos.SessionMessageReasonNoActiveTurn, result.err.Error())
	default:
		r.trackControl(result.message, kelos.SessionMessagePhaseRejected, kelos.SessionMessageReasonInvalidRequest, result.err.Error())
	}
}

func (r *sessionMessageRelay) deliverRemove(message *kelos.SessionMessage) {
	target, tracked, ok := r.target(message)
	if !ok {
		r.trackControl(message, kelos.SessionMessagePhaseRejected, kelos.SessionMessageReasonTaskNotFound, fmt.Sprintf("SessionMessage %q was not found for this Session", message.Spec.TaskRef))
		return
	}
	if tracked == nil {
		_, delivered := r.handled[target.UID]
		if delivered || !sessionMessagePending(target) {
			r.trackControl(message, kelos.SessionMessagePhaseRejected, kelos.SessionMessageReasonNotPending, fmt.Sprintf("SessionMessage %q is no longer pending", target.Name))
			return
		}
		r.trackControl(target, kelos.SessionMessagePhaseCanceled, kelos.SessionMessageReasonRemoved, fmt.Sprintf("removed by SessionMessage %q before delivery", message.Name))
		r.trackControl(message, kelos.SessionMessagePhaseCompleted, "", "")
		return
	}
	turn := r.turns[tracked.turnID]
	if tracked.final || turn == nil || turn.started || turn.done {
		r.trackControl(message, kelos.SessionMessagePhaseRejected, kelos.SessionMessageReasonNotPending, fmt.Sprintf("SessionMessage %q is no longer pending", tracked.name))
		return
	}
	if err := r.server.removePendingMessageText(tracked.turnID, tracked.text, sessionMessageRequestID(message)); err != nil {
		r.trackControl(message, kelos.SessionMessagePhaseRejected, kelos.SessionMessageReasonNotPending, err.Error())
		return
	}
	r.finish(tracked, kelos.SessionMessagePhaseCanceled, kelos.SessionMessageReasonRemoved, fmt.Sprintf("removed by SessionMessage %q", message.Name))
	r.repointMerged(turn)
	r.trackControl(message, kelos.SessionMessagePhaseCompleted, "", "")
}

// flush writes every changed status and stops tracking messages whose terminal
// status was written.
func (r *sessionMessageRelay) flush(ctx context.Context) {
	names := make([]string, 0, len(r.tracked))
	for name := range r.tracked {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		tracked := r.tracked[name]
		if tracked.written == nil || !reflect.DeepEqual(*tracked.written, tracked.status) {
			err := r.writeStatus(ctx, tracked)
			if apierrors.IsNotFound(err) {
				r.untrack(tracked)
				continue
			}
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("Updating SessionMessage %q status failed error=%v", name, err)
				}
				continue
			}
			written := *tracked.status.DeepCopy()
			tracked.written = &written
		}
		if tracked.final {
			r.untrack(tracked)
		}
	}
}

func (r *sessionMessageRelay) writeStatus(ctx context.Context, tracked *trackedSessionMessage) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := r.client.Get(ctx, tracked.name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if current.UID != tracked.uid {
			return apierrors.NewNotFound(kelos.GroupVersion.WithResource("sessionmessages").GroupResource(), tracked.name)
		}
		current.Status = *tracked.status.DeepCopy()
		updated, err := r.client.UpdateStatus(ctx, current, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		if cached := r.cache[updated.Name]; cached == nil || cached.UID == updated.UID {
			r.cache[updated.Name] = updated
		}
		return nil
	})
}
