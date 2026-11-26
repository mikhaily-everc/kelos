package sessionruntime

import (
	"context"
	"errors"
	"sync"
	"time"
)

// claudeInner is one Claude Code process on one credential tier.
type claudeInner interface {
	Provider
	shellCommandContextProvider
	runtimeStatusSnapshot() RuntimeStatus
	providerError() error
}

type claudeInnerFactory func(ctx context.Context, config ProviderConfig) (claudeInner, error)

type failoverTurn struct {
	ctx   context.Context
	input TurnInput
	tried map[string]bool
}

// FailoverClaudeProvider replays a turn that hit a usage limit or auth error on the next tier.
type FailoverClaudeProvider struct {
	ctx     context.Context
	config  ProviderConfig
	tiers   *claudeTierConfig
	state   *claudeTierState
	factory claudeInnerFactory
	now     func() time.Time

	// lifecycle serializes swaps, turn starts and Close.
	lifecycle sync.Mutex

	mu            sync.Mutex
	inner         claudeInner
	gen           int
	current       claudeTier
	sink          EventSink
	turn          *failoverTurn
	suppressStart bool
	closed        bool
	done          chan struct{}
	doneErr       error
}

var (
	_ Provider                    = (*FailoverClaudeProvider)(nil)
	_ shellCommandContextProvider = (*FailoverClaudeProvider)(nil)
	_ runtimeStatusProvider       = (*FailoverClaudeProvider)(nil)
)

// NewFailoverClaudeProvider is NewClaudeProvider unless the Pod configures a credential chain.
func NewFailoverClaudeProvider(ctx context.Context, config ProviderConfig) (Provider, error) {
	tiers, ok := claudeTierConfigFromEnvironment(config.Environment)
	if !ok {
		return NewClaudeProvider(ctx, config)
	}
	factory := func(ctx context.Context, config ProviderConfig) (claudeInner, error) {
		return NewClaudeProvider(ctx, config)
	}
	return newFailoverClaudeProvider(ctx, config, tiers, factory, time.Now)
}

func newFailoverClaudeProvider(ctx context.Context, config ProviderConfig, tiers *claudeTierConfig, factory claudeInnerFactory, now func() time.Time) (Provider, error) {
	w := &FailoverClaudeProvider{
		ctx: ctx, config: config, tiers: tiers, factory: factory, now: now,
		state: loadClaudeTierState(config.StateDir), sink: config.EventSink,
		done: make(chan struct{}),
	}
	tier, ok := w.state.pick(tiers.available(config.Environment), nil, now())
	if !ok {
		logClaudeTier("no_tier", "", "no tier holds a credential; using the Pod environment")
		return NewClaudeProvider(ctx, config)
	}
	inner, err := w.start(tier, 0)
	if err != nil {
		return nil, err
	}
	w.inner, w.current = inner, tier
	logClaudeTier("start", tier.name, "")
	go w.watch(inner, 0)
	return w, nil
}

func (w *FailoverClaudeProvider) start(tier claudeTier, gen int) (claudeInner, error) {
	config := w.config
	config.Environment = tier.env
	config.EventSink = &failoverSink{provider: w, gen: gen}
	return w.factory(w.ctx, config)
}

// watch ends the provider when its current process dies; a swapped-out process is ignored.
func (w *FailoverClaudeProvider) watch(inner claudeInner, gen int) {
	<-inner.Done()
	w.mu.Lock()
	defer w.mu.Unlock()
	if gen != w.gen || w.doneClosed() {
		return
	}
	w.doneErr = inner.providerError()
	close(w.done)
}

func (w *FailoverClaudeProvider) doneClosed() bool {
	select {
	case <-w.done:
		return true
	default:
		return false
	}
}

type failoverSink struct {
	provider *FailoverClaudeProvider
	gen      int
}

func (s *failoverSink) Emit(event Event) { s.provider.emit(s.gen, event) }

func (s *failoverSink) RequestInput(ctx context.Context, request InputRequest) (map[string][]string, error) {
	s.provider.mu.Lock()
	sink := s.provider.sink
	s.provider.mu.Unlock()
	if sink == nil {
		return nil, errors.New("Session has no event sink")
	}
	return sink.RequestInput(ctx, request)
}

func (w *FailoverClaudeProvider) emit(gen int, event Event) {
	w.mu.Lock()
	if gen != w.gen {
		w.mu.Unlock()
		return
	}
	sink := w.sink
	switch event.Type {
	case EventTurnStarted:
		if w.suppressStart {
			w.suppressStart = false
			w.mu.Unlock()
			return
		}
	case EventTurnCompleted:
		turn := w.turn
		if event.Status == "failed" && turn != nil && claudeTierFailure(event.Text) {
			now := w.now()
			w.state.coolDown(w.current, claudeResetTime(event.Text, now))
			logClaudeTier("exhausted", w.current.name, "turn failed; trying the next tier")
			next, ok := w.state.pick(w.tiers.available(w.config.Environment), turn.tried, now)
			if ok {
				w.mu.Unlock()
				go w.retryOn(turn, next, event)
				return
			}
			logClaudeTier("chain_exhausted", w.current.name, "no tier left for this turn")
		} else if event.Status == "completed" {
			logClaudeTier("served", w.current.name, "")
		}
		w.turn = nil
	}
	w.mu.Unlock()
	if sink != nil {
		sink.Emit(event)
	}
}

// retryOn replays the failed turn on next; the failure is forwarded only if that cannot start.
func (w *FailoverClaudeProvider) retryOn(turn *failoverTurn, next claudeTier, failure Event) {
	w.lifecycle.Lock()
	defer w.lifecycle.Unlock()
	if err := w.swap(next); err != nil {
		w.giveUp(failure)
		return
	}
	w.mu.Lock()
	turn.tried[next.name] = true
	w.suppressStart = true
	inner := w.inner
	w.mu.Unlock()
	logClaudeTier("retry", next.name, "replaying the failed turn")
	if err := inner.StartTurn(turn.ctx, turn.input); err != nil {
		w.giveUp(failure)
	}
}

func (w *FailoverClaudeProvider) giveUp(failure Event) {
	w.mu.Lock()
	w.turn = nil
	w.suppressStart = false
	sink := w.sink
	w.mu.Unlock()
	if sink != nil {
		sink.Emit(failure)
	}
}

// swap replaces the process with one on tier; the caller holds lifecycle.
func (w *FailoverClaudeProvider) swap(tier claudeTier) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return errors.New("provider closed")
	}
	old := w.inner
	w.gen++
	gen := w.gen
	w.mu.Unlock()
	_ = old.Close()
	inner, err := w.start(tier, gen)
	if err != nil {
		logClaudeTier("swap_failed", tier.name, err.Error())
		w.mu.Lock()
		w.doneErr = err
		if !w.doneClosed() {
			close(w.done)
		}
		w.mu.Unlock()
		return err
	}
	w.mu.Lock()
	w.inner, w.current = inner, tier
	w.mu.Unlock()
	logClaudeTier("switch", tier.name, "")
	go w.watch(inner, gen)
	return nil
}

// refresh re-reads the tier files and moves to the preferred tier when it is not the current one.
func (w *FailoverClaudeProvider) refresh() {
	tier, ok := w.state.pick(w.tiers.available(w.config.Environment), nil, w.now())
	w.mu.Lock()
	same := !ok || (tier.name == w.current.name && tier.fingerprint == w.current.fingerprint)
	w.mu.Unlock()
	if !same {
		_ = w.swap(tier)
	}
}

func (w *FailoverClaudeProvider) SetEventSink(sink EventSink) {
	w.mu.Lock()
	w.sink = sink
	w.mu.Unlock()
}

func (w *FailoverClaudeProvider) StartTurn(ctx context.Context, input TurnInput) error {
	w.lifecycle.Lock()
	defer w.lifecycle.Unlock()
	w.mu.Lock()
	busy := w.turn != nil
	w.mu.Unlock()
	if !busy {
		w.refresh()
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return errors.New("provider closed")
	}
	if w.turn == nil {
		w.turn = &failoverTurn{ctx: ctx, input: input, tried: map[string]bool{w.current.name: true}}
	}
	inner := w.inner
	w.mu.Unlock()
	if err := inner.StartTurn(ctx, input); err != nil {
		w.mu.Lock()
		w.turn = nil
		w.mu.Unlock()
		return err
	}
	return nil
}

func (w *FailoverClaudeProvider) currentInner() claudeInner {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.inner
}

func (w *FailoverClaudeProvider) Interrupt(ctx context.Context) error {
	return w.currentInner().Interrupt(ctx)
}

func (w *FailoverClaudeProvider) recordShellCommand(ctx context.Context, record shellCommandRecord) error {
	return w.currentInner().recordShellCommand(ctx, record)
}

func (w *FailoverClaudeProvider) runtimeStatusSnapshot() RuntimeStatus {
	return w.currentInner().runtimeStatusSnapshot()
}

func (w *FailoverClaudeProvider) Done() <-chan struct{} { return w.done }

func (w *FailoverClaudeProvider) providerError() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.doneErr
}

func (w *FailoverClaudeProvider) Close() error {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.lifecycle.Lock()
	defer w.lifecycle.Unlock()
	return w.currentInner().Close()
}
