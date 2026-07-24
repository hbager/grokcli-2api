package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hm2899/grokcli-2api/internal/models"
	"github.com/hm2899/grokcli-2api/internal/pool"
	"github.com/hm2899/grokcli-2api/internal/protocol/anthropic"
	"github.com/hm2899/grokcli-2api/internal/protocol/toolcall"
	"github.com/hm2899/grokcli-2api/internal/provider"
	"github.com/hm2899/grokcli-2api/internal/upstream/console"
	"github.com/hm2899/grokcli-2api/internal/upstream/web"
	"github.com/hm2899/grokcli-2api/internal/upstream/grok"
)

// AccountFailureReporter is notified for every upstream account attempt that
// failed (including intermediate failover losers). Server wiring uses this to
// classify free-usage / 额度用完 bodies and kick accounts into the cooldown pool
// even when a later account eventually succeeds the request.
type AccountFailureReporter interface {
	ReportAccountFailure(accountID, model string, err error)
}

type ChatService struct {
	Catalog       *models.Catalog
	Client        *grok.Client
	Console       *console.Client // Phase 2: SSO console upstream
	Web           *web.Client     // Phase 3: SSO web chat upstream
	Now           func() time.Time
	PickObserver  PickObserver
	AffinityStore AffinityStore
	// FailureReporter is optional; when set, every failed account attempt is
	// reported so quota-exhausted text can enter the cooldown pool immediately.
	FailureReporter       AccountFailureReporter
	StickyFirstOnly       bool // try sticky/first account before broader failover
	FirstByteProbeWorkers int  // parallel first-byte probes after sticky miss (default 3, max 8)
	MaxFailoverAttempts   int  // total accounts tried per request (default 12, max 64)
}

type PickObserver interface {
	LoadPenalty(context.Context, string) int64
	MarkPick(context.Context, string)
	ReleasePick(context.Context, string)
}

// optional batching extension for hot-path candidate windows
type batchPickObserver interface {
	LoadPenalties(context.Context, []string) map[string]int64
}

type AffinityStore interface {
	GetAffinity(context.Context, string) (string, error)
	BindAffinity(context.Context, string, string) error
	// ClearAffinity drops a multi-turn pin (dead/cooling sticky account).
	ClearAffinity(context.Context, string) error
}

type ChatRequest struct {
	Model     string         `json:"model"`
	Stream    bool           `json:"stream"`
	Raw       map[string]any `json:"-"`
	UserAgent string         `json:"-"` // optional; Codex auto-compact threshold
}

type StreamFrame struct {
	Data []byte
	Done bool
}

type ChatDelta struct {
	ID           string
	Model        string
	Created      int64
	Content      string
	Reasoning    string
	ToolCalls    []map[string]any
	FunctionCall map[string]any
	FinishReason any
	Usage        any
}

type ChatResult struct {
	Payload       map[string]any
	AccountID     string
	Model         string
	Usage         any
	PreferAccount string
	FirstAccount  string
	Failover      bool
	Fingerprint   string
	Accounts      int
	Prep          BodyPrepStats
	PickHeld      bool
}

type StreamOpen struct {
	Body          io.ReadCloser
	AccountID     string
	Model         string
	PreferAccount string
	FirstAccount  string
	Failover      bool
	Fingerprint   string
	Accounts      int
	Prep          BodyPrepStats
	PickHeld      bool
}

type StreamStats struct {
	Usage        any
	FirstTokenMS int // 0 if never observed
	// CompletionHint approximates output tokens from streamed content/tools when
	// upstream omits the final usage frame (admin would otherwise show completion=0).
	CompletionHint int
}

func DecodeChatRequest(reader io.Reader) (ChatRequest, error) {
	var raw map[string]any
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		return ChatRequest{}, err
	}
	model, _ := raw["model"].(string)
	stream, _ := raw["stream"].(bool)
	return ChatRequest{Model: model, Stream: stream, Raw: raw}, nil
}

func (s *ChatService) Complete(ctx context.Context, request ChatRequest, candidates []pool.Candidate, mode string) (map[string]any, error) {
	result, err := s.CompleteWithResult(ctx, request, candidates, mode)
	if result.PickHeld {
		s.releasePick(context.Background(), result.AccountID)
	}
	if err != nil {
		return nil, err
	}
	return result.Payload, nil
}

func (s *ChatService) CompleteWithResult(ctx context.Context, request ChatRequest, candidates []pool.Candidate, mode string) (ChatResult, error) {
	if request.Stream {
		return ChatResult{}, fmt.Errorf("Go chat streaming requires ChatService.Stream")
	}
	model, chain, client, err := s.prepareChain(ctx, request, candidates, mode)
	if err != nil {
		return ChatResult{}, err
	}
	route := s.resolveRoute(request)
	_ = client
	body, prep := PrepareUpstreamBodyDetailed(request.Raw, request.UserAgent)
	ensureUpstreamCacheKey(body, request)
	fingerprint := ChatFingerprint(request)
	// Prefer account already boosted to chain[0] by prepareChain/ensureStickyCandidate.
	// Avoid a second Redis GET on the TTFT hot path.
	prefer := ""
	if len(chain) > 0 {
		prefer = chain[0].ID
	}
	first := ""
	if len(chain) > 0 {
		first = chain[0].ID
	}
	var lastEmpty error
	lastFailAccountID := ""
	stickyFirst := s.StickyFirstOnly
	if !stickyFirst {
		stickyFirst = prefer != "" && first != "" && prefer == first && len(chain) > 1
	}
	// stickyMissID defers pin clear until a later account actually succeeds.
	stickyMissID := ""
	for i, candidate := range chain {
		s.markAttempt(ctx, candidate.ID)
		accountID, rc, err := s.openAccountAttempt(ctx, candidate, route, body)
		if err != nil {
			// Intermediate + final losers: report so free-usage / 额度用完 bodies
			// enter the cooldown pool even when a later account succeeds.
			s.reportAccountFailure(accountID, model, err)
			s.releasePick(ctx, accountID)
			lastFailAccountID = accountID
			if stickyFirst && i == 0 && shouldDropStickyPin(err) {
				stickyMissID = accountID
			}
			// Retryable/non-retryable both continue within short chain until exhausted.
			if i == len(chain)-1 {
				if lastEmpty != nil {
					return ChatResult{PreferAccount: prefer, FirstAccount: first, Fingerprint: fingerprint, Accounts: len(chain), Prep: prep, AccountID: lastFailAccountID}, lastEmpty
				}
				return ChatResult{PreferAccount: prefer, FirstAccount: first, Fingerprint: fingerprint, Accounts: len(chain), Prep: prep, AccountID: lastFailAccountID}, err
			}
			continue
		}
		collector := newChatCollector(model)
		collector.SetAllowedTools(extractAllowedToolNames(request.Raw))
		readErr := grok.ReadSSE(rc, collector.feed)
		_ = rc.Close()
		if readErr != nil {
			s.reportAccountFailure(accountID, model, readErr)
			s.releasePick(ctx, accountID)
			return ChatResult{PreferAccount: prefer, FirstAccount: first, Fingerprint: fingerprint, Accounts: len(chain), Prep: prep, AccountID: accountID}, readErr
		}
		if !collector.emptyModelOutput() {
			failover := first != "" && accountID != first
			// Only drop sticky pin once we know a different account produced live output.
			if failover && stickyMissID != "" {
				s.noteStickyOutcome(ctx, request, stickyMissID, false)
			}
			// Always pin the live account so multi-turn cache stays on the healthy row.
			s.bindAffinity(ctx, request, accountID)
			return ChatResult{
				Payload: collector.response(), AccountID: accountID, Model: collector.model, Usage: collector.usage, PickHeld: true,
				PreferAccount: prefer, FirstAccount: first, Failover: failover,
				Fingerprint: fingerprint, Accounts: len(chain), Prep: prep,
			}, nil
		}
		lastEmpty = &grok.UpstreamError{Status: 502, Body: "Upstream returned HTTP 200 with empty model output (no content/tool_calls)"}
		s.reportAccountFailure(accountID, model, lastEmpty)
		s.releasePick(ctx, accountID)
		lastFailAccountID = accountID
		if stickyFirst && i == 0 {
			stickyMissID = candidate.ID
		}
	}
	if lastEmpty == nil {
		lastEmpty = &grok.UpstreamError{Status: 502, Body: "Upstream returned HTTP 200 with empty model output (no content/tool_calls)"}
	}
	return ChatResult{PreferAccount: prefer, FirstAccount: first, Fingerprint: fingerprint, Accounts: len(chain), Prep: prep, Failover: true, AccountID: lastFailAccountID}, lastEmpty
}

func (s *ChatService) Stream(ctx context.Context, request ChatRequest, candidates []pool.Candidate, mode string, emit func(StreamFrame) error) error {
	body, err := s.OpenStream(ctx, request, candidates, mode)
	if err != nil {
		return err
	}
	defer body.Close()
	return ForwardChatStream(body, emit)
}

func (s *ChatService) OpenStream(ctx context.Context, request ChatRequest, candidates []pool.Candidate, mode string) (io.ReadCloser, error) {
	opened, err := s.OpenStreamWithResult(ctx, request, candidates, mode)
	if err != nil {
		return nil, err
	}
	if !opened.PickHeld {
		return opened.Body, nil
	}
	return &pickReleaseReadCloser{
		ReadCloser: opened.Body,
		release: func() {
			s.releasePick(context.Background(), opened.AccountID)
		},
	}, nil
}

type pickReleaseReadCloser struct {
	io.ReadCloser
	once    sync.Once
	release func()
}

func (r *pickReleaseReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.once.Do(r.release)
	return err
}

func (s *ChatService) OpenStreamWithResult(ctx context.Context, request ChatRequest, candidates []pool.Candidate, mode string) (StreamOpen, error) {
	model, chain, client, err := s.prepareChain(ctx, request, candidates, mode)
	if err != nil {
		return StreamOpen{}, err
	}
	route := s.resolveRoute(request)
	_ = client
	body, prep := PrepareUpstreamBodyDetailed(request.Raw, request.UserAgent)
	ensureUpstreamCacheKey(body, request)
	fingerprint := ChatFingerprint(request)
	// Prefer account already boosted to chain[0] by prepareChain/ensureStickyCandidate.
	// Avoid a second Redis GET on the TTFT hot path.
	prefer := ""
	if len(chain) > 0 {
		prefer = chain[0].ID
	}
	first := ""
	if len(chain) > 0 {
		first = chain[0].ID
	}
	// Sticky-first: try preferred/first account alone before spending TTFT on failover chain.
	stickyFirst := s.StickyFirstOnly
	if !stickyFirst {
		stickyFirst = prefer != "" && first != "" && prefer == first && len(chain) > 1
	}

	meta := StreamOpen{PreferAccount: prefer, FirstAccount: first, Fingerprint: fingerprint, Accounts: len(chain), Prep: prep}
	var lastEmpty error

	// openOne tries a single account: dial + empty-stream peek.
	// On success returns a live guarded body. On empty/error releases pick and returns ok=false.
	openOneCandidate := func(candidate pool.Candidate) (StreamOpen, bool, error) {
		s.markAttempt(ctx, candidate.ID)
		accountID, rc, err := s.openAccountAttempt(ctx, candidate, route, body)
		if err != nil {
			s.reportAccountFailure(accountID, model, err)
			s.releasePick(ctx, accountID)
			return StreamOpen{PreferAccount: prefer, FirstAccount: first, Fingerprint: fingerprint, Accounts: len(chain), Prep: prep, AccountID: accountID}, false, err
		}
		guarded, empty, err := guardStreamAgainstEmpty(rc)
		if err != nil {
			_ = rc.Close()
			s.reportAccountFailure(accountID, model, err)
			s.releasePick(ctx, accountID)
			return StreamOpen{PreferAccount: prefer, FirstAccount: first, Fingerprint: fingerprint, Accounts: len(chain), Prep: prep, AccountID: accountID}, false, err
		}
		if empty {
			_ = guarded.Close()
			s.releasePick(ctx, accountID)
			emptyErr := &grok.UpstreamError{Status: 502, Body: "Upstream returned HTTP 200 with empty model output (no content/tool_calls)"}
			s.reportAccountFailure(accountID, model, emptyErr)
			return StreamOpen{PreferAccount: prefer, FirstAccount: first, Fingerprint: fingerprint, Accounts: len(chain), Prep: prep, AccountID: accountID}, false, emptyErr
		}
		// Silence-pass may still hollow-end empty (Claude high-effort). Confirm briefly
		// before binding sticky / returning live to streamAnthropic.
		guarded, empty, err = confirmStreamNotEmpty(guarded)
		if err != nil {
			if guarded != nil {
				_ = guarded.Close()
			}
			s.reportAccountFailure(accountID, model, err)
			s.releasePick(ctx, accountID)
			return StreamOpen{PreferAccount: prefer, FirstAccount: first, Fingerprint: fingerprint, Accounts: len(chain), Prep: prep, AccountID: accountID}, false, err
		}
		if empty {
			if guarded != nil {
				_ = guarded.Close()
			}
			s.releasePick(ctx, accountID)
			emptyErr := &grok.UpstreamError{Status: 502, Body: "Upstream returned HTTP 200 with empty model output (no content/tool_calls)"}
			s.reportAccountFailure(accountID, model, emptyErr)
			return StreamOpen{PreferAccount: prefer, FirstAccount: first, Fingerprint: fingerprint, Accounts: len(chain), Prep: prep, AccountID: accountID}, false, emptyErr
		}
		failover := first != "" && accountID != first
		// Always pin the live stream account (rebind after sticky failover).
		s.bindAffinity(ctx, request, accountID)
		return StreamOpen{
			Body: guarded, AccountID: accountID, Model: model, PickHeld: true,
			PreferAccount: prefer, FirstAccount: first, Failover: failover,
			Fingerprint: fingerprint, Accounts: len(chain), Prep: prep,
		}, true, nil
	}

	// Phase 1: top account alone (preserves sticky prompt-cache warmth when pinned).
	// Always race the remainder after first miss — sequential open over the full
	// chain made empty storms take tens of seconds before client 502.
	primary := chain
	rest := []pool.Candidate(nil)
	if len(chain) > 1 {
		primary = chain[:1]
		rest = chain[1:]
	}
	// stickyMissID: sticky/primary that failed. We defer clearStickyPins until a
	// failover winner is confirmed. Clearing early then losing the race left the
	// conversation with NO pin → next turn cold-picks a random account (intermittent
	// cache miss under empty/timeout storms).
	stickyMissID := ""
	for _, candidate := range primary {
		opened, ok, err := openOneCandidate(candidate)
		if ok {
			return opened, nil
		}
		if stickyFirst && shouldDropStickyPin(err) {
			stickyMissID = candidate.ID
		}
		if err != nil {
			// Prefer the concrete failing account id so open-failure paths still
			// feed reportChatPool / cooldown classification.
			if strings.TrimSpace(opened.AccountID) != "" {
				meta.AccountID = opened.AccountID
			} else {
				meta.AccountID = candidate.ID
			}
			if ue, is := err.(*grok.UpstreamError); is && strings.Contains(ue.Body, "empty model output") {
				lastEmpty = err
				continue
			}
			// Always try remaining accounts (parallel/serial phase) before failing the request.
			if len(rest) == 0 {
				if lastEmpty != nil {
					return meta, lastEmpty
				}
				return meta, err
			}
			lastEmpty = err
		}
	}

	// Phase 2: remaining failover candidates.
	// Build keeps parallel first-byte race; console/web use sequential SSO opens.
	if len(rest) == 0 {
		if lastEmpty == nil {
			lastEmpty = &grok.UpstreamError{Status: 502, Body: "Upstream returned HTTP 200 with empty model output (no content/tool_calls)"}
		}
		return StreamOpen{PreferAccount: prefer, FirstAccount: first, Fingerprint: fingerprint, Accounts: len(chain), Prep: prep, Failover: true}, lastEmpty
	}
	if route.Provider != provider.Build {
		for _, candidate := range rest {
			opened, ok, err := openOneCandidate(candidate)
			if ok {
				if stickyMissID != "" {
					s.noteStickyOutcome(ctx, request, stickyMissID, false)
				}
				return opened, nil
			}
			if err != nil {
				if strings.TrimSpace(opened.AccountID) != "" {
					meta.AccountID = opened.AccountID
				} else {
					meta.AccountID = candidate.ID
				}
				lastEmpty = err
			}
		}
	} else {
		restAccounts := upstreamAccounts(rest)
		opened, err := s.parallelFirstByteOpen(ctx, restAccounts, client, model, body, chain, prefer, first, fingerprint, prep, request)
		if err == nil {
			// Confirmed failover winner — drop the dead sticky pin only now.
			if stickyMissID != "" {
				s.noteStickyOutcome(ctx, request, stickyMissID, false)
				// parallelFirstByteOpen already bindAffinity'd the winner.
			}
			return opened, nil
		}
		if lastEmpty == nil {
			lastEmpty = err
		}
		// All failover failed: KEEP original sticky pin so a transient storm does not
		// permanently orphan the conversation onto a random cold account next turn.
	}

	if lastEmpty == nil {
		lastEmpty = &grok.UpstreamError{Status: 502, Body: "Upstream returned HTTP 200 with empty model output (no content/tool_calls)"}
	}
	return StreamOpen{PreferAccount: prefer, FirstAccount: first, Fingerprint: fingerprint, Accounts: len(chain), Prep: prep, Failover: true}, lastEmpty
}

// parallelFirstByteOpen races remaining failover accounts for the first non-empty
// upstream SSE. Sticky/primary is NOT included — call this only after sticky miss.
// Losers are closed promptly so we do not burn quota on multiple full generations.
type parallelAttemptControl struct {
	cancel   context.CancelFunc
	body     io.ReadCloser
	stopping bool
}

type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
	once   sync.Once
}

func (c *cancelOnClose) Close() error {
	if c == nil {
		return nil
	}
	var err error
	c.once.Do(func() {
		if c.ReadCloser != nil {
			err = c.ReadCloser.Close()
		}
		if c.cancel != nil {
			c.cancel()
		}
	})
	return err
}

func (s *ChatService) parallelFirstByteOpen(
	ctx context.Context,
	accounts []grok.Account,
	client *grok.Client,
	model string,
	body map[string]any,
	chain []pool.Candidate,
	prefer, first, fingerprint string,
	prep BodyPrepStats,
	request ChatRequest,
) (StreamOpen, error) {
	type raced struct {
		opened    StreamOpen
		err       error
		idx       int
		accountID string
		cancel    context.CancelFunc
	}
	if len(accounts) == 0 {
		return StreamOpen{}, &grok.UpstreamError{Status: 502, Body: "Upstream returned HTTP 200 with empty model output (no content/tool_calls)"}
	}
	// Bound parallel probes (configurable; default 3, hard max 8).
	maxWorkers := 3
	if s != nil && s.FirstByteProbeWorkers > 0 {
		maxWorkers = s.FirstByteProbeWorkers
	}
	if maxWorkers > 8 {
		maxWorkers = 8
	}
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	if maxWorkers > len(accounts) {
		maxWorkers = len(accounts)
	}

	// Workers pull from the complete candidate list. Concurrency is bounded, but
	// a freed worker keeps probing later candidates until one wins or all fail.
	baseCtx := ctxOrBackground(ctx)
	results := make(chan raced, len(accounts))
	var stateMu sync.Mutex
	next := 0
	stopped := false
	active := make(map[int]*parallelAttemptControl, maxWorkers)

	takeAttempt := func() (int, grok.Account, context.Context, *parallelAttemptControl, bool) {
		stateMu.Lock()
		defer stateMu.Unlock()
		if stopped || baseCtx.Err() != nil || next >= len(accounts) {
			return 0, grok.Account{}, nil, nil, false
		}
		idx := next
		next++
		attemptCtx, attemptCancel := context.WithCancel(baseCtx)
		control := &parallelAttemptControl{cancel: attemptCancel}
		active[idx] = control
		return idx, accounts[idx], attemptCtx, control, true
	}

	setAttemptBody := func(idx int, body io.ReadCloser) bool {
		stateMu.Lock()
		control, ok := active[idx]
		accepted := ok && !control.stopping
		if accepted {
			control.body = body
		}
		stateMu.Unlock()
		if !accepted {
			_ = body.Close()
			return false
		}
		return true
	}

	finishAttempt := func(idx int) {
		stateMu.Lock()
		delete(active, idx)
		stateMu.Unlock()
	}

	stopAttemptsExcept := func(keep int) {
		stateMu.Lock()
		stopped = true
		controls := make([]*parallelAttemptControl, 0, len(active))
		for idx, control := range active {
			if idx == keep || control.stopping {
				continue
			}
			control.stopping = true
			controls = append(controls, control)
		}
		stateMu.Unlock()
		for _, control := range controls {
			control.cancel()
			stateMu.Lock()
			attemptBody := control.body
			stateMu.Unlock()
			if attemptBody != nil {
				_ = attemptBody.Close()
			}
		}
	}

	probe := func(idx int, account grok.Account, attemptCtx context.Context, control *parallelAttemptControl) raced {
		result := raced{idx: idx, accountID: account.ID, cancel: control.cancel}
		attempt, err := OpenWithFailover(attemptCtx, client, []grok.Account{account}, model, body, &CommitState{})
		if err != nil {
			if attemptCtx.Err() == nil {
				s.reportAccountFailure(account.ID, model, err)
			}
			result.err = err
			return result
		}
		if !setAttemptBody(idx, attempt.Body) {
			result.err = attemptCtx.Err()
			if result.err == nil {
				result.err = context.Canceled
			}
			return result
		}

		guarded, empty, err := guardStreamAgainstEmpty(attempt.Body)
		if err != nil {
			_ = attempt.Body.Close()
			if attemptCtx.Err() == nil {
				s.reportAccountFailure(account.ID, model, err)
			}
			result.err = err
			return result
		}
		if empty {
			_ = guarded.Close()
			emptyErr := &grok.UpstreamError{Status: 502, Body: "Upstream returned HTTP 200 with empty model output (no content/tool_calls)"}
			if attemptCtx.Err() == nil {
				s.reportAccountFailure(account.ID, model, emptyErr)
			}
			result.err = emptyErr
			return result
		}

		// Silence-pass may still hollow-end empty (Claude high-effort). Confirm
		// before racing this body as a winner, matching the sticky path.
		guarded, empty, err = confirmStreamNotEmpty(guarded)
		if err != nil {
			if guarded != nil {
				_ = guarded.Close()
			}
			if attemptCtx.Err() == nil {
				s.reportAccountFailure(account.ID, model, err)
			}
			result.err = err
			return result
		}
		if empty {
			if guarded != nil {
				_ = guarded.Close()
			}
			emptyErr := &grok.UpstreamError{Status: 502, Body: "Upstream returned HTTP 200 with empty model output (no content/tool_calls)"}
			if attemptCtx.Err() == nil {
				s.reportAccountFailure(account.ID, model, emptyErr)
			}
			result.err = emptyErr
			return result
		}

		failover := first != "" && account.ID != first
		result.opened = StreamOpen{
			Body: guarded, AccountID: account.ID, Model: model, PickHeld: true,
			PreferAccount: prefer, FirstAccount: first, Failover: failover,
			Fingerprint: fingerprint, Accounts: len(chain), Prep: prep,
		}
		return result
	}

	var wg sync.WaitGroup
	for range maxWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx, account, attemptCtx, control, ok := takeAttempt()
				if !ok {
					return
				}
				// Mark only when this worker is about to make the upstream attempt.
				if attemptCtx.Err() != nil {
					control.cancel()
					finishAttempt(idx)
					return
				}
				s.markAttempt(attemptCtx, account.ID)
				result := probe(idx, account, attemptCtx, control)
				if result.err != nil {
					control.cancel()
					finishAttempt(idx)
					s.releasePick(context.Background(), account.ID)
					results <- result
					continue
				}
				results <- result
				return
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(results)
		close(done)
	}()

	// Drain every started attempt before returning. The result buffer has one
	// slot per candidate, so cancellation cannot strand workers on a send.
	var lastErr error
	var winner *raced
	parentDone := baseCtx.Done()
	parentCanceled := false
	for {
		select {
		case <-parentDone:
			parentCanceled = true
			parentDone = nil
			stopAttemptsExcept(-1)
		case result, ok := <-results:
			if !ok {
				goto drained
			}
			if result.err != nil {
				if !errors.Is(result.err, context.Canceled) && !errors.Is(result.err, context.DeadlineExceeded) {
					lastErr = result.err
				}
				continue
			}
			if winner == nil && !parentCanceled && baseCtx.Err() == nil {
				winner = &result
				stopAttemptsExcept(result.idx)
				continue
			}
			// A successful attempt that lost the race still owns a live body.
			_ = result.opened.Body.Close()
			result.cancel()
			finishAttempt(result.idx)
			s.releasePick(context.Background(), result.accountID)
		}
	}

drained:
	<-done
	if baseCtx.Err() != nil {
		parentCanceled = true
	}
	if parentCanceled {
		if winner != nil {
			_ = winner.opened.Body.Close()
			winner.cancel()
			finishAttempt(winner.idx)
			s.releasePick(context.Background(), winner.accountID)
		}
		return StreamOpen{PreferAccount: prefer, FirstAccount: first, Fingerprint: fingerprint, Accounts: len(chain), Prep: prep, Failover: true}, baseCtx.Err()
	}
	if winner != nil {
		finishAttempt(winner.idx)
		winner.opened.Body = &cancelOnClose{ReadCloser: winner.opened.Body, cancel: winner.cancel}
		// Parallel path only after sticky miss — always rebind the winner.
		s.bindAffinity(context.Background(), request, winner.opened.AccountID)
		return winner.opened, nil
	}
	if lastErr == nil {
		lastErr = &grok.UpstreamError{Status: 502, Body: "Upstream returned HTTP 200 with empty model output (no content/tool_calls)"}
	}
	return StreamOpen{PreferAccount: prefer, FirstAccount: first, Fingerprint: fingerprint, Accounts: len(chain), Prep: prep, Failover: true}, lastErr
}

func ForwardChatStream(reader io.Reader, emit func(StreamFrame) error) error {
	_, err := ForwardChatStreamWithStats(reader, emit)
	return err
}

func ForwardChatStreamWithStats(reader io.Reader, emit func(StreamFrame) error) (StreamStats, error) {
	if emit == nil {
		return StreamStats{}, fmt.Errorf("stream emitter is required")
	}
	var stats StreamStats
	started := time.Now()
	err := grok.ReadSSE(reader, func(event grok.Event) error {
		if event.Done {
			return emit(StreamFrame{Done: true})
		}
		if stats.FirstTokenMS == 0 && len(event.Data) > 0 {
			stats.FirstTokenMS = int(time.Since(started).Milliseconds())
			if stats.FirstTokenMS <= 0 {
				stats.FirstTokenMS = 1
			}
		}
		delta, err := ParseChatDelta(event.Data)
		if err != nil {
			return nil
		}
		if delta.Usage != nil {
			stats.Usage = delta.Usage
		}
		return emit(StreamFrame{Data: append([]byte(nil), event.Data...)})
	})
	return stats, err
}

func (s *ChatService) prepare(ctx context.Context, request ChatRequest, candidates []pool.Candidate, mode string) (string, pool.Candidate, *grok.Client, error) {
	model := s.resolveModel(request)
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}
	candidates = append([]pool.Candidate(nil), candidates...)
	fingerprint := ChatFingerprint(request)
	if s.AffinityStore != nil && fingerprint != "" {
		preferAffinity(ctxOrBackground(ctx), candidates, s.AffinityStore, fingerprint)
	}
	if s.PickObserver != nil {
		adjustCandidatesForObserver(ctxOrBackground(ctx), candidates, s.PickObserver)
	}
	picked, err := pool.Pick(candidates, model, mode, now)
	if err != nil {
		return "", pool.Candidate{}, nil, err
	}
	s.bindAffinity(ctx, request, picked.ID)
	if s.PickObserver != nil {
		s.PickObserver.MarkPick(ctxOrBackground(ctx), picked.ID)
	}
	client := s.Client
	if client == nil {
		client = &grok.Client{}
	}
	return model, picked, client, nil
}

// defaultFailoverChain matches the max_failover_attempts setting default.
const defaultFailoverChain = 12

func (s *ChatService) prepareChain(ctx context.Context, request ChatRequest, candidates []pool.Candidate, mode string) (string, []pool.Candidate, *grok.Client, error) {
	route := s.resolveRoute(request)
	model := route.Upstream
	if model == "" {
		model = s.resolveModel(request)
	}
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}
	candidates = append([]pool.Candidate(nil), candidates...)
	candidates = pool.FilterByAuth(candidates, route.Auth)
	fingerprint := ChatFingerprint(request)
	// Skip second Redis affinity GET when server already pinned sticky to candidates[0]
	// (RequestCount heavily boosted by listCandidatesForRequest).
	alreadyPinned := len(candidates) > 0 && candidates[0].RequestCount <= -1_000_000_000
	if !alreadyPinned && s.AffinityStore != nil && fingerprint != "" {
		preferAffinity(ctxOrBackground(ctx), candidates, s.AffinityStore, fingerprint)
	}
	if s.PickObserver != nil {
		adjustCandidatesForObserver(ctxOrBackground(ctx), candidates, s.PickObserver)
	}
	// Never build a full-pool chain: cap failover attempts.
	maxAttempts := s.MaxFailoverAttempts
	if maxAttempts < 1 || maxAttempts > 64 {
		maxAttempts = defaultFailoverChain
	}
	blockKey := s.resolveModel(request)
	chain := pool.Chain(candidates, blockKey, mode, now, maxAttempts)
	if len(chain) == 0 {
		return "", nil, nil, pool.ErrNoEligibleAccounts
	}
	// Re-pin sticky account to front of failover chain without extra Redis GET.
	// preferAffinity / server sticky inject already moved sticky candidate to candidates[0] when known.
	if len(candidates) > 0 {
		prefer := candidates[0].ID
		for i := range chain {
			if chain[i].ID == prefer {
				if i > 0 {
					cand := chain[i]
					copy(chain[1:i+1], chain[0:i])
					chain[0] = cand
				}
				break
			}
		}
	}
	// Account picks are marked when an attempt actually starts.
	client := s.Client
	if client == nil {
		client = &grok.Client{}
	}
	return model, chain, client, nil
}

type chatCollector struct {
	id           string
	model        string
	content      string
	reasoning    string
	toolCalls    []map[string]any
	functionCall map[string]any
	finishReason any
	usage        any
	created      int64
	// allowedTools: client-registered names for Update/StrReplace → Edit remap.
	allowedTools []string
}

func newChatCollector(model string) *chatCollector {
	return &chatCollector{model: model, created: time.Now().Unix()}
}

// SetAllowedTools configures client-registered tool names for outbound remap.
func (c *chatCollector) SetAllowedTools(names []string) {
	if c == nil {
		return
	}
	c.allowedTools = append([]string(nil), names...)
}

func (c *chatCollector) feed(event grok.Event) error {
	if event.Done {
		return nil
	}
	delta, err := parseChatDelta(event.Data)
	if err != nil {
		return nil
	}
	if delta.ID != "" && c.id == "" {
		c.id = delta.ID
	}
	if delta.Model != "" {
		c.model = delta.Model
	}
	if delta.Created > 0 {
		c.created = delta.Created
	}
	if delta.Usage != nil {
		c.usage = delta.Usage
	}
	if delta.FinishReason != nil {
		c.finishReason = delta.FinishReason
	}
	if len(delta.ToolCalls) > 0 {
		c.mergeToolCalls(delta.ToolCalls)
	}
	if delta.FunctionCall != nil {
		c.mergeFunctionCall(delta.FunctionCall)
	}
	c.content += delta.Content
	c.reasoning += delta.Reasoning
	return nil
}

func (c *chatCollector) response() map[string]any {
	id := c.id
	if id == "" {
		id = fmt.Sprintf("chatcmpl-go-%d", c.created)
	}
	message := map[string]any{
		"role":    "assistant",
		"content": c.content,
	}
	if c.reasoning != "" {
		message["reasoning_content"] = c.reasoning
	}
	// Normalize + drop incomplete tool calls so OpenAI clients never receive
	// alias-only / half-JSON arguments that break tool loops.
	// Remap Update/StrReplace → Edit using client-registered tool names.
	toolCalls := normalizeOutboundToolCalls(c.toolCalls, nil, true, c.allowedTools)
	if len(toolCalls) > 0 {
		message["tool_calls"] = toolCalls
		// OpenAI chat: tool messages require content=null when tool_calls present.
		if strings.TrimSpace(c.content) == "" {
			message["content"] = nil
		}
	}
	if c.functionCall != nil {
		if fn := normalizeOutboundFunctionCall(c.functionCall, nil, c.allowedTools); fn != nil {
			message["function_call"] = fn
			if strings.TrimSpace(c.content) == "" && message["tool_calls"] == nil {
				message["content"] = nil
			}
		}
	}
	finish := c.finishReason
	if finish == nil {
		if len(toolCalls) > 0 {
			finish = "tool_calls"
		} else if message["function_call"] != nil {
			finish = "function_call"
		} else {
			finish = "stop"
		}
	} else if fr, ok := finish.(string); ok {
		// Don't advertise tool_calls if every tool was incomplete and dropped.
		if (fr == "tool_calls" || fr == "function_call") && len(toolCalls) == 0 && message["function_call"] == nil {
			finish = "stop"
		}
	}
	usage := c.usage
	if usage == nil {
		usage = map[string]any{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
	}
	return map[string]any{
		"id":      id,
		"object":  "chat.completion",
		"created": c.created,
		"model":   c.model,
		"choices": []map[string]any{{
			"index":         0,
			"message":       message,
			"finish_reason": finish,
		}},
		"usage": usage,
	}
}

// preferredShellArgKey resolves the client-facing shell arg name for a tool.
// Honors an explicit map from the client tool schema when present; otherwise
// defaults via toolcall.DefaultShellArgKey (Codex "cmd", Hermes terminal "command").
func preferredShellArgKey(name string, keys map[string]string) string {
	if keys != nil {
		if v := strings.TrimSpace(keys[name]); v != "" {
			return v
		}
		if v := strings.TrimSpace(keys[strings.ToLower(name)]); v != "" {
			return v
		}
		if nk := toolcall.NameKey(name); nk != "" {
			if v := strings.TrimSpace(keys[nk]); v != "" {
				return v
			}
		}
	}
	if toolcall.IsShellTool(name) {
		return toolcall.DefaultShellArgKey(name)
	}
	return ""
}

// extractAllowedToolNames collects client-registered tool names from OpenAI or
// Anthropic-shaped tools arrays. Used to remap Grok Update/StrReplace → Edit.
func extractAllowedToolNames(raw map[string]any) []string {
	if raw == nil {
		return nil
	}
	items, _ := raw["tools"].([]any)
	if items == nil {
		if typed, ok := raw["tools"].([]map[string]any); ok {
			items = make([]any, len(typed))
			for i, t := range typed {
				items[i] = t
			}
		}
	}
	out := make([]string, 0, len(items))
	seen := map[string]bool{}
	for _, item := range items {
		tool, _ := item.(map[string]any)
		if tool == nil {
			continue
		}
		fn, _ := tool["function"].(map[string]any)
		name := strings.TrimSpace(stringValueAny(fn["name"]))
		if name == "" {
			name = strings.TrimSpace(stringValueAny(tool["name"]))
		}
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	return out
}

// normalizeOutboundToolCalls rewrites Grok aliases (path/search/cmd) into the
// client schema and drops tools that still miss required fields.
// force=true uses CoerceCompleteJSON (stream end / non-stream); force=false uses
// EffectiveJSON so mid-stream Update path+old without replace stays incomplete.
//
// allowedNames remaps Grok-invented Update/StrReplace → client Edit (Claude Code
// via sub2api / OpenAI chat). Empty allowed still remaps edit aliases to "Edit".
func normalizeOutboundToolCalls(calls []map[string]any, shellArgKeys map[string]string, force bool, allowedNames ...[]string) []map[string]any {
	if len(calls) == 0 {
		return nil
	}
	keys := shellArgKeys
	var allowed []string
	if len(allowedNames) > 0 {
		allowed = allowedNames[0]
	}
	out := make([]map[string]any, 0, len(calls))
	for i, call := range calls {
		if call == nil {
			continue
		}
		item := map[string]any{}
		for k, v := range call {
			item[k] = v
		}
		fn, _ := item["function"].(map[string]any)
		if fn == nil {
			// Sometimes name/arguments sit at top level.
			if name := strings.TrimSpace(stringValueAny(item["name"])); name != "" {
				fn = map[string]any{"name": name, "arguments": stringValueAny(item["arguments"])}
			}
		}
		if fn == nil {
			continue
		}
		rawName := strings.TrimSpace(stringValueAny(fn["name"]))
		if rawName == "" {
			continue
		}
		// Remap Update/StrReplace → Edit before arg coerce so edit-only aliases apply
		// under the client-registered name (and Claude Code never sees "Update").
		name := toolcall.CanonicalName(rawName, allowed)
		if name == "" {
			name = rawName
		}
		args := stringValueAny(fn["arguments"])
		// Live path waits for required fields; force-finish invents missing new_string.
		// Try under remapped name first; fall back to raw Grok name for readiness races.
		if force {
			args = toolcall.CoerceCompleteJSON(args, name)
			if !toolcall.CompleteJSON(args, name) && name != rawName {
				if alt := toolcall.CoerceCompleteJSON(stringValueAny(fn["arguments"]), rawName); toolcall.CompleteJSON(alt, rawName) {
					// Keep remapped name; re-coerce under Edit so densify/fill run.
					args = toolcall.CoerceCompleteJSON(alt, name)
				}
			}
		} else {
			// Live path: normalize only + CompleteJSONStrict. EffectiveJSON
			// repairs truncation and would emit half tools mid-stream.
			args = toolcall.NormalizeJSON(args, name)
			if args == "" {
				args = stringValueAny(fn["arguments"])
			}
			if !toolcall.CompleteJSONStrict(args, name) && name != rawName {
				alt := toolcall.NormalizeJSON(stringValueAny(fn["arguments"]), rawName)
				if alt != "" && toolcall.CompleteJSONStrict(alt, rawName) {
					// Remap under client name after readiness under raw name.
					args = toolcall.NormalizeJSON(alt, name)
					if args == "" {
						args = alt
					}
				}
			}
		}
		// Live: strict; force: CompleteJSON (after coerce already applied).
		// Force-finish: if still incomplete, accept non-empty JSON object salvage so
		// clients do not see half-open tool_use ("Tool use interrupted").
		if force {
			if !toolcall.CompleteJSON(args, name) {
				args2 := strings.TrimSpace(args)
				okSalvage := false
				if args2 != "" && (args2[0] == '{' || args2[0] == '[') {
					var raw any
					if json.Unmarshal([]byte(args2), &raw) == nil {
						switch v := raw.(type) {
						case map[string]any:
							okSalvage = len(v) > 0
						case []any:
							okSalvage = len(v) > 0
						}
					}
				}
				if !okSalvage {
					continue
				}
			}
		} else if !toolcall.CompleteJSONStrict(args, name) {
			continue
		}
		// Codex shell schema wants "cmd". Internal form is "command".
		if pref := preferredShellArgKey(name, keys); pref != "" {
			args = toolcall.ProjectShellArgsForClient(args, name, pref)
		} else if pref := preferredShellArgKey(rawName, keys); pref != "" {
			args = toolcall.ProjectShellArgsForClient(args, rawName, pref)
		}
		id := strings.TrimSpace(stringValueAny(item["id"]))
		if id == "" {
			id = fmt.Sprintf("call_go_%d", i)
		}
		typ := strings.TrimSpace(stringValueAny(item["type"]))
		if typ == "" {
			typ = "function"
		}
		// Prefer original index when present so stream emit maps correctly.
		outIndex := i
		if raw, ok := numberToInt64(item["index"]); ok && raw >= 0 {
			outIndex = int(raw)
		}
		out = append(out, map[string]any{
			"index": outIndex,
			"id":    id,
			"type":  typ,
			"function": map[string]any{
				"name":      name,
				"arguments": args,
			},
		})
	}
	return out
}

func normalizeOutboundFunctionCall(call map[string]any, shellArgKeys map[string]string, allowedNames ...[]string) map[string]any {
	if call == nil {
		return nil
	}
	keys := shellArgKeys
	var allowed []string
	if len(allowedNames) > 0 {
		allowed = allowedNames[0]
	}
	rawName := strings.TrimSpace(stringValueAny(call["name"]))
	if rawName == "" {
		return nil
	}
	name := toolcall.CanonicalName(rawName, allowed)
	if name == "" {
		name = rawName
	}
	args := toolcall.CoerceCompleteJSON(stringValueAny(call["arguments"]), name)
	if !toolcall.CompleteJSON(args, name) {
		// Legacy function_call with no required schema: keep if any valid JSON object.
		if text := strings.TrimSpace(args); text != "" && (text[0] == '{' || text[0] == '[') {
			var raw any
			if json.Unmarshal([]byte(text), &raw) == nil {
				if pref := preferredShellArgKey(name, keys); pref != "" {
					text = toolcall.ProjectShellArgsForClient(text, name, pref)
				}
				return map[string]any{"name": name, "arguments": text}
			}
		}
		return nil
	}
	if pref := preferredShellArgKey(name, keys); pref != "" {
		args = toolcall.ProjectShellArgsForClient(args, name, pref)
	}
	return map[string]any{"name": name, "arguments": args}
}

// ChatToolStreamAssembler buffers OpenAI chat.completion.chunk tool_calls across
// SSE frames and rewrites them into normalized, complete tool_calls once ready.
// Non-tool frames passthrough unchanged.
type ChatToolStreamAssembler struct {
	id           string
	model        string
	created      int64
	toolCalls    []map[string]any
	functionCall map[string]any
	emitted      map[int]bool
	// clientAcked: true only after the tool frame Write succeeded. Soft write
	// failures leave emitted=true but unacked so RequeueUnacked can re-emit
	// complete tool_calls (Claude Code "Tool use interrupted").
	clientAcked map[int]bool
	// pendingAcks: indexes framed in the last emitReadyToolFrames call.
	pendingAcks  []int
	finishReason any
	usage        any
	// finished is set after a finish_reason frame is produced so soft-disconnect
	// force-finish does not emit a second terminal chunk.
	finished bool
	// finishedAcked: true only after finish_reason Write succeeded.
	finishedAcked bool
	// shellArgKeys maps tool name → preferred client shell arg key ("cmd" or "command").
	// Empty/nil defaults shell-family tools to "cmd" (Codex schema).
	shellArgKeys map[string]string
	// allowedTools: client-registered names for Update/StrReplace → Edit remap.
	allowedTools []string
}

func NewChatToolStreamAssembler() *ChatToolStreamAssembler {
	return &ChatToolStreamAssembler{emitted: map[int]bool{}, clientAcked: map[int]bool{}, shellArgKeys: map[string]string{}}
}

// SetShellArgKeys configures client-facing shell parameter names (Codex: "cmd").
func (a *ChatToolStreamAssembler) SetShellArgKeys(keys map[string]string) {
	if a == nil {
		return
	}
	if keys == nil {
		a.shellArgKeys = map[string]string{}
		return
	}
	a.shellArgKeys = keys
}

// SetAllowedTools configures client-registered tool names for outbound remap
// (Grok Update/StrReplace → Claude Code Edit).
func (a *ChatToolStreamAssembler) SetAllowedTools(names []string) {
	if a == nil {
		return
	}
	a.allowedTools = append([]string(nil), names...)
}

// Feed merges tool deltas. Returns (frames, passthrough).
// passthrough=true means the original event.Data should be written as-is
// (no tool activity / no finish that needs rewrite).
func (a *ChatToolStreamAssembler) Feed(raw []byte, delta ChatDelta) (frames []map[string]any, passthrough bool) {
	if a == nil {
		return nil, true
	}
	if delta.ID != "" {
		a.id = delta.ID
	}
	if delta.Model != "" {
		a.model = delta.Model
	}
	if delta.Created > 0 {
		a.created = delta.Created
	}
	if delta.Usage != nil {
		a.usage = delta.Usage
	}
	hasTools := len(delta.ToolCalls) > 0 || delta.FunctionCall != nil
	heldTools := len(a.toolCalls) > 0 || a.functionCall != nil
	hasContent := strings.TrimSpace(delta.Content) != "" || strings.TrimSpace(delta.Reasoning) != ""

	// No tools this turn and nothing buffered → always passthrough (including
	// content+finish_reason chunks). Rewriting those would drop content.
	if !hasTools && !heldTools {
		return nil, true
	}

	if hasTools {
		if len(delta.ToolCalls) > 0 {
			a.mergeToolCalls(delta.ToolCalls)
		}
		if delta.FunctionCall != nil {
			a.mergeFunctionCall(delta.FunctionCall)
		}
		// Emit any tools that just became complete (dense, normalized args).
		if ready := a.emitReadyToolFrames(false); len(ready) > 0 {
			frames = append(frames, ready...)
		}
	}

	// Progressive text/reasoning while tools are buffering.
	if hasContent {
		payload := a.baseChunk()
		choiceDelta := map[string]any{}
		if strings.TrimSpace(delta.Content) != "" {
			choiceDelta["content"] = delta.Content
		}
		if strings.TrimSpace(delta.Reasoning) != "" {
			choiceDelta["reasoning_content"] = delta.Reasoning
		}
		payload["choices"] = []any{map[string]any{"index": 0, "delta": choiceDelta, "finish_reason": nil}}
		frames = append(frames, payload)
	}

	if delta.FinishReason != nil {
		a.finishReason = delta.FinishReason
		// Flush remaining complete tools + a finish frame (no content drop — emitted above).
		frames = append(frames, a.emitReadyToolFrames(true)...)
		if term := a.finishFrame(); term != nil {
			frames = append(frames, term)
		}
		return frames, false
	}

	// Tool-only partial chunk: hold (do not passthrough raw incomplete args).
	if hasTools && !hasContent {
		return frames, false
	}
	return frames, len(frames) == 0
}

// Holding reports whether any tool / function_call state is buffered.
func (a *ChatToolStreamAssembler) Holding() bool {
	if a == nil {
		return false
	}
	return len(a.toolCalls) > 0 || a.functionCall != nil
}

// Finish flushes any remaining complete tools (end of stream without finish_reason).
func (a *ChatToolStreamAssembler) Finish() []map[string]any {
	if a == nil {
		return nil
	}
	// Soft write may have left unacked tools; requeue before force-finish.
	a.RequeueUnacked()
	return a.emitReadyToolFrames(true)
}

// EmittedAny reports whether any tool/function_call frame was already written.
func (a *ChatToolStreamAssembler) EmittedAny() bool {
	if a == nil {
		return false
	}
	for _, v := range a.emitted {
		if v {
			return true
		}
	}
	return false
}

// FinishReasonFrame returns a terminal finish_reason chunk once. Soft-disconnect
// and [DONE] both call this; a nil return means already finished (or no tools).
func (a *ChatToolStreamAssembler) FinishReasonFrame() map[string]any {
	// Idempotent unless RequeueUnacked cleared finished after soft write.
	if a == nil || a.finished {
		return nil
	}
	// Only emit a terminal when we buffered/emitted tools this turn.
	if !a.Holding() && !a.EmittedAny() && a.finishReason == nil {
		return nil
	}
	return a.finishFrame()
}

// AckPayload marks tools whose index/id appear in a successfully written JSON payload,
// and finish_reason when present.
func (a *ChatToolStreamAssembler) AckPayload(payload string) {
	if a == nil || payload == "" {
		return
	}
	if a.clientAcked == nil {
		a.clientAcked = map[int]bool{}
	}
	if a.emitted == nil {
		a.emitted = map[int]bool{}
	}
	if strings.Contains(payload, "finish_reason") && a.finished {
		a.finishedAcked = true
	}
	if !strings.Contains(payload, "tool_calls") && !strings.Contains(payload, "function_call") {
		return
	}
	for _, idx := range a.pendingAcks {
		// Best-effort: if payload has tool_calls/function_call, ack pending in order.
		// Chat frames usually carry one batch of ready tools.
		a.clientAcked[idx] = true
	}
	// Also ack any emitted tool whose id appears in payload.
	for i, call := range a.toolCalls {
		if !a.emitted[i] || a.clientAcked[i] {
			continue
		}
		if id, _ := call["id"].(string); id != "" && strings.Contains(payload, id) {
			a.clientAcked[i] = true
		}
	}
	if a.emitted[-1] && !a.clientAcked[-1] && strings.Contains(payload, "function_call") {
		a.clientAcked[-1] = true
	}
	// Drop acked from pending
	kept := a.pendingAcks[:0]
	for _, idx := range a.pendingAcks {
		if !a.clientAcked[idx] {
			kept = append(kept, idx)
		}
	}
	a.pendingAcks = kept
}

// RequeueUnacked rolls back framed-but-unacked tools so Finish can re-emit them.
func (a *ChatToolStreamAssembler) RequeueUnacked() {
	if a == nil {
		return
	}
	if a.clientAcked == nil {
		a.clientAcked = map[int]bool{}
	}
	if a.emitted == nil {
		a.emitted = map[int]bool{}
	}
	if a.finished && !a.finishedAcked {
		a.finished = false
	}
	for idx, em := range a.emitted {
		if !em || a.clientAcked[idx] {
			continue
		}
		a.emitted[idx] = false
	}
	a.pendingAcks = a.pendingAcks[:0]
}

// NeedsFinishRetry reports soft-fail recovery still has work.
func (a *ChatToolStreamAssembler) NeedsFinishRetry() bool {
	if a == nil {
		return false
	}
	if a.finished && !a.finishedAcked {
		return true
	}
	for idx, em := range a.emitted {
		if em && !a.clientAcked[idx] {
			return true
		}
	}
	// Pending complete tools never framed
	if a.Holding() {
		for i := range a.toolCalls {
			if !a.emitted[i] {
				return true
			}
		}
		if a.functionCall != nil && !a.emitted[-1] {
			return true
		}
	}
	return false
}

// HasUnacked reports framed-but-unacked tools or terminal.
func (a *ChatToolStreamAssembler) HasUnacked() bool {
	if a == nil {
		return false
	}
	if a.finished && !a.finishedAcked {
		return true
	}
	for idx, em := range a.emitted {
		if em && !a.clientAcked[idx] {
			return true
		}
	}
	return len(a.pendingAcks) > 0
}

func (a *ChatToolStreamAssembler) mergeToolCalls(calls []map[string]any) {
	for _, incoming := range calls {
		idx := len(a.toolCalls)
		if rawIndex, ok := numberToInt64(incoming["index"]); ok && rawIndex >= 0 {
			idx = int(rawIndex)
		}
		for len(a.toolCalls) <= idx {
			a.toolCalls = append(a.toolCalls, map[string]any{"index": len(a.toolCalls)})
		}
		mergeToolCall(a.toolCalls[idx], incoming)
	}
}

func (a *ChatToolStreamAssembler) mergeFunctionCall(call map[string]any) {
	if a.functionCall == nil {
		a.functionCall = map[string]any{}
	}
	mergeStringFields(a.functionCall, call)
}

func (a *ChatToolStreamAssembler) emitReadyToolFrames(force bool) []map[string]any {
	// Fresh pending batch for this emit.
	a.pendingAcks = a.pendingAcks[:0]
	// Live path (force=false): EffectiveJSON only — do not invent missing new_string.
	// Force-finish (force=true / non-stream collector): CoerceCompleteJSON fills
	// delete-match defaults so incomplete path+old still emit at stream end.
	// Remap Update/StrReplace → Edit using client-registered tool names.
	normalized := normalizeOutboundToolCalls(a.toolCalls, a.shellArgKeys, force, a.allowedTools)
	if len(normalized) == 0 && a.functionCall == nil {
		return nil
	}
	// Map normalized calls back by original index; emit ones not yet emitted.
	var ready []map[string]any
	for _, call := range normalized {
		idx := 0
		if raw, ok := numberToInt64(call["index"]); ok {
			idx = int(raw)
		}
		if a.emitted[idx] && a.clientAcked[idx] {
			continue
		}
		// Only emit when force or arguments complete (already filtered by normalize).
		// Mark framed but not acked — soft write can RequeueUnacked and re-emit.
		a.emitted[idx] = true
		a.clientAcked[idx] = false
		a.pendingAcks = append(a.pendingAcks, idx)
		ready = append(ready, call)
	}
	var frames []map[string]any
	if len(ready) > 0 {
		payload := a.baseChunk()
		// Stream-compatible shape: one chunk with full tool_calls (args complete).
		// Clients that accumulate by index still work; ones that require single-shot also work.
		deltaCalls := make([]any, 0, len(ready))
		for _, call := range ready {
			deltaCalls = append(deltaCalls, call)
		}
		payload["choices"] = []any{map[string]any{
			"index":         0,
			"delta":         map[string]any{"tool_calls": deltaCalls},
			"finish_reason": nil,
		}}
		frames = append(frames, payload)
	}
	if force {
		if fn := normalizeOutboundFunctionCall(a.functionCall, a.shellArgKeys, a.allowedTools); fn != nil && !(a.emitted[-1] && a.clientAcked[-1]) {
			a.emitted[-1] = true
			a.clientAcked[-1] = false
			a.pendingAcks = append(a.pendingAcks, -1)
			payload := a.baseChunk()
			payload["choices"] = []any{map[string]any{
				"index":         0,
				"delta":         map[string]any{"function_call": fn},
				"finish_reason": nil,
			}}
			frames = append(frames, payload)
		}
	}
	return frames
}

func (a *ChatToolStreamAssembler) finishFrame() map[string]any {
	// Idempotent unless RequeueUnacked cleared finished after soft write.
	if a.finished {
		return nil
	}
	a.finished = true
	payload := a.baseChunk()
	finish := a.finishReason
	// If all tools were incomplete/dropped, don't claim tool_calls.
	anyEmitted := false
	for _, v := range a.emitted {
		if v {
			anyEmitted = true
			break
		}
	}
	if fr, ok := finish.(string); ok {
		if (fr == "tool_calls" || fr == "function_call") && !anyEmitted {
			finish = "stop"
		}
	}
	if finish == nil {
		if anyEmitted {
			finish = "tool_calls"
		} else {
			finish = "stop"
		}
	}
	choice := map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": finish}
	payload["choices"] = []any{choice}
	if a.usage != nil {
		payload["usage"] = a.usage
	}
	return payload
}

func (a *ChatToolStreamAssembler) baseChunk() map[string]any {
	id := a.id
	if id == "" {
		id = "chatcmpl-go-stream"
	}
	model := a.model
	if model == "" {
		model = "grok-4.5"
	}
	created := a.created
	if created == 0 {
		created = time.Now().Unix()
	}
	return map[string]any{
		"id":      id,
		"object":  "chat.completion.chunk",
		"created": created,
		"model":   model,
	}
}

func (c *chatCollector) mergeToolCalls(calls []map[string]any) {
	for _, incoming := range calls {
		idx := len(c.toolCalls)
		if rawIndex, ok := numberToInt64(incoming["index"]); ok && rawIndex >= 0 {
			idx = int(rawIndex)
		}
		for len(c.toolCalls) <= idx {
			c.toolCalls = append(c.toolCalls, map[string]any{"index": len(c.toolCalls)})
		}
		mergeToolCall(c.toolCalls[idx], incoming)
	}
}

func mergeToolCall(dst, src map[string]any) {
	for key, value := range src {
		if key == "function" {
			incoming, _ := value.(map[string]any)
			if incoming == nil {
				continue
			}
			existing, _ := dst["function"].(map[string]any)
			if existing == nil {
				existing = map[string]any{}
				dst["function"] = existing
			}
			// Merge name first so argument Merge knows Edit/Update aliases.
			if name := strings.TrimSpace(stringValueAny(incoming["name"])); name != "" {
				existing["name"] = name
			}
			if args, ok := incoming["arguments"]; ok {
				piece := stringValueAny(args)
				if piece != "" {
					cur := stringValueAny(existing["arguments"])
					name := stringValueAny(existing["name"])
					existing["arguments"] = toolcall.Merge(cur, piece, name)
				}
			}
			// Other function fields (if any) still append/overwrite safely.
			for k, v := range incoming {
				if k == "name" || k == "arguments" {
					continue
				}
				existing[k] = v
			}
			continue
		}
		if text, ok := value.(string); ok {
			if key == "id" || key == "type" {
				if text != "" {
					dst[key] = text
				}
			} else {
				dst[key] = stringValueAny(dst[key]) + text
			}
			continue
		}
		dst[key] = value
	}
}

func (c *chatCollector) mergeFunctionCall(call map[string]any) {
	if c.functionCall == nil {
		c.functionCall = map[string]any{}
	}
	mergeStringFields(c.functionCall, call)
}

func mergeStringFields(dst, src map[string]any) {
	for key, value := range src {
		if text, ok := value.(string); ok {
			if key == "name" {
				if text != "" {
					dst[key] = text
				}
			} else {
				dst[key] = stringValueAny(dst[key]) + text
			}
			continue
		}
		dst[key] = value
	}
}

func ParseChatDelta(data []byte) (ChatDelta, error) {
	return parseChatDelta(data)
}

func (d ChatDelta) AnthropicToolDeltas() []anthropic.ToolDelta {
	out := make([]anthropic.ToolDelta, 0, len(d.ToolCalls))
	for _, call := range d.ToolCalls {
		index := 0
		if rawIndex, ok := numberToInt64(call["index"]); ok && rawIndex >= 0 {
			index = int(rawIndex)
		}
		fn, _ := call["function"].(map[string]any)
		out = append(out, anthropic.ToolDelta{
			Index:     index,
			ID:        stringValueAny(call["id"]),
			Name:      stringValueAny(fn["name"]),
			Arguments: stringValueAny(fn["arguments"]),
		})
	}
	if d.FunctionCall != nil {
		out = append(out, anthropic.ToolDelta{Index: len(out), Name: stringValueAny(d.FunctionCall["name"]), Arguments: stringValueAny(d.FunctionCall["arguments"])})
	}
	return out
}

func parseChatDelta(data []byte) (ChatDelta, error) {
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return ChatDelta{}, err
	}
	delta := ChatDelta{}
	delta.ID, _ = payload["id"].(string)
	delta.Model, _ = payload["model"].(string)
	if created, ok := numberToInt64(payload["created"]); ok {
		delta.Created = created
	}
	// Prefer top-level usage; some relays nest under response.usage on hybrid frames.
	if u := payload["usage"]; u != nil {
		delta.Usage = u
	} else if resp, _ := payload["response"].(map[string]any); resp != nil {
		if u := resp["usage"]; u != nil {
			delta.Usage = u
		}
	}
	choices, _ := payload["choices"].([]any)
	for _, item := range choices {
		choice, _ := item.(map[string]any)
		if reason := choice["finish_reason"]; reason != nil {
			delta.FinishReason = reason
		}
		if itemDelta, _ := choice["delta"].(map[string]any); itemDelta != nil {
			delta.Content += contentTextFromAny(itemDelta["content"])
			if t := rawString(itemDelta["text"]); t != "" {
				delta.Content += t
			}
			if text := rawString(itemDelta["reasoning_content"]); text != "" {
				delta.Reasoning += text
			} else if text := rawString(itemDelta["reasoning"]); text != "" {
				delta.Reasoning += text
			} else if text := contentTextFromAny(itemDelta["reasoning_content"]); text != "" {
				delta.Reasoning += text
			}
			if calls := toolCallsFromAny(itemDelta["tool_calls"]); len(calls) > 0 {
				delta.ToolCalls = append(delta.ToolCalls, calls...)
			}
			if call, _ := itemDelta["function_call"].(map[string]any); call != nil {
				delta.FunctionCall = call
			}
		}
		if message, _ := choice["message"].(map[string]any); message != nil {
			delta.Content += contentTextFromAny(message["content"])
			if text := rawString(message["reasoning_content"]); text != "" {
				delta.Reasoning += text
			} else if text := rawString(message["reasoning"]); text != "" {
				delta.Reasoning += text
			} else if text := contentTextFromAny(message["reasoning_content"]); text != "" {
				delta.Reasoning += text
			}
			if calls := toolCallsFromAny(message["tool_calls"]); len(calls) > 0 {
				delta.ToolCalls = append(delta.ToolCalls, calls...)
			}
			if call, _ := message["function_call"].(map[string]any); call != nil {
				delta.FunctionCall = call
			}
		}
	}
	return delta, nil
}

// contentTextFromAny extracts visible text from OpenAI content that may be a
// plain string or an array of {type,text} / {type:output_text,text} parts.
// Without this, multi-part content frames never reach usage estimation and the
// admin shows completion_tokens=0/1 for real multi-part streams.
func contentTextFromAny(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []any:
		var b strings.Builder
		for _, part := range v {
			switch p := part.(type) {
			case string:
				b.WriteString(p)
			case map[string]any:
				if t := rawString(p["text"]); t != "" {
					b.WriteString(t)
					continue
				}
				if t := rawString(p["content"]); t != "" {
					b.WriteString(t)
					continue
				}
				// Nested content arrays (rare).
				if nested := contentTextFromAny(p["content"]); nested != "" {
					b.WriteString(nested)
				}
			}
		}
		return b.String()
	case map[string]any:
		if t := rawString(v["text"]); t != "" {
			return t
		}
		return contentTextFromAny(v["content"])
	default:
		return ""
	}
}

func toolCallsFromAny(value any) []map[string]any {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		call, ok := item.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, call)
	}
	return out
}

func stringValueAny(value any) string {
	text, _ := value.(string)
	return text
}

func rawString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	default:
		return ""
	}
}

func (s *ChatService) resolveModel(request ChatRequest) string {
	model := request.Model
	if s.Catalog != nil {
		model = s.Catalog.Resolve(model)
	}
	if model == "" {
		model = "grok-4.5"
	}
	return model
}

func (s *ChatService) resolveRoute(request ChatRequest) provider.Route {
	model := s.resolveModel(request)
	if s.Catalog != nil {
		return s.Catalog.ResolveRoute(model)
	}
	return provider.ResolveRoute(model, "grok-4.5")
}

// openAccountAttempt routes to build (token) or console (SSO) upstream.
func (s *ChatService) openAccountAttempt(ctx context.Context, candidate pool.Candidate, route provider.Route, body map[string]any) (accountID string, bodyRC io.ReadCloser, err error) {
	model := route.Upstream
	if model == "" {
		model = route.PublicID
	}
	if route.Provider == provider.Console {
		client := s.Console
		if client == nil {
			client = &console.Client{}
		}
		resp, err := client.Open(ctx, candidate.ConsoleAccount(), model, body, route.ReasoningEffort)
		if err != nil {
			return candidate.ID, nil, err
		}
		return candidate.ID, resp.Body, nil
	}
	if route.Provider == provider.Web {
		client := s.Web
		if client == nil {
			client = &web.Client{}
		}
		// Image/video web models are catalog-only for now.
		if _, ok := web.ModeForModel(model); !ok {
			return candidate.ID, nil, &grok.UpstreamError{Status: 501, Body: "web non-chat capability not implemented yet: " + model}
		}
		resp, err := client.Open(ctx, candidate.ConsoleAccount(), model, body)
		if err != nil {
			return candidate.ID, nil, err
		}
		return candidate.ID, resp.Body, nil
	}
	client := s.Client
	if client == nil {
		client = &grok.Client{}
	}
	resp, err := client.Open(ctx, candidate.UpstreamAccount(), model, body)
	if err != nil {
		return candidate.ID, nil, err
	}
	return candidate.ID, resp.Body, nil
}

func (s *ChatService) releasePick(ctx context.Context, accountID string) {
	if s.PickObserver == nil || accountID == "" {
		return
	}
	s.PickObserver.ReleasePick(ctxOrBackground(ctx), accountID)
}

func (s *ChatService) releaseChain(ctx context.Context, chain []pool.Candidate) {
	if s.PickObserver == nil {
		return
	}
	for _, candidate := range chain {
		s.releasePick(ctx, candidate.ID)
	}
}

func (s *ChatService) releaseChainExcept(ctx context.Context, chain []pool.Candidate, keepID string) {
	if s.PickObserver == nil {
		return
	}
	for _, candidate := range chain {
		if candidate.ID == keepID {
			continue
		}
		s.releasePick(ctx, candidate.ID)
	}
}

// clearStickyPins drops multi-turn pins for this request so a dead/cooling account
// is not preferred on the next turn (which destroys prompt-cache hit rates).
func (s *ChatService) clearStickyPins(ctx context.Context, request ChatRequest) {
	if s == nil || s.AffinityStore == nil {
		return
	}
	ctx = ctxOrBackground(ctx)
	keys := make([]string, 0, 8)
	if request.Raw != nil {
		if pck, _ := request.Raw["prompt_cache_key"].(string); strings.TrimSpace(pck) != "" {
			pck = strings.TrimSpace(pck)
			model := strings.TrimSpace(request.Model)
			if model != "" {
				keys = append(keys, "chat:"+model+":prompt_cache_key:"+pck)
			}
			keys = append(keys, "chat:prompt_cache_key:"+pck)
		}
		for _, key := range []string{"conversation_id", "conversation", "thread_id", "session_id"} {
			if value, _ := request.Raw[key].(string); strings.TrimSpace(value) != "" {
				keys = append(keys, "chat:"+strings.TrimSpace(request.Model)+":"+key+":"+strings.TrimSpace(value))
			}
		}
	}
	if fp := ChatFingerprint(request); fp != "" {
		keys = append(keys, fp)
	}
	seen := map[string]bool{}
	for _, k := range keys {
		k = strings.TrimSpace(k)
		if k == "" || seen[k] {
			continue
		}
		seen[k] = true
		_ = s.AffinityStore.ClearAffinity(ctx, k)
	}
}

// shouldDropStickyPin reports whether a sticky-primary open failure is durable
// enough to abandon the multi-turn pin. Transient network/timeouts keep the pin
// so the next turn can retry the cache-warm account (avoids intermittent cache
// miss after brief upstream blips). Empty/auth/quota failures drop the pin so
// the failover winner can own the conversation.
func shouldDropStickyPin(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	// Empty HTTP 200 / hollow stream: account+model is soft-blocked; hop.
	if strings.Contains(msg, "empty model output") ||
		strings.Contains(msg, "no content/tool_calls") ||
		strings.Contains(msg, "empty_upstream") {
		return true
	}
	// Transient transport — keep sticky.
	for _, needle := range []string{
		"timeout", "deadline exceeded", "i/o timeout",
		"connection reset", "connection refused", "broken pipe",
		"temporary", "temporar", "eof", "unexpected eof",
		"tls handshake", "no such host", "network is unreachable",
	} {
		if strings.Contains(msg, needle) {
			return false
		}
	}
	var ue *grok.UpstreamError
	if errors.As(err, &ue) && ue != nil {
		// Auth / forbidden / free-usage style → drop pin.
		if ue.Status == 401 || ue.Status == 403 {
			return true
		}
		body := strings.ToLower(ue.Body)
		if strings.Contains(body, "empty model output") ||
			strings.Contains(body, "no content/tool_calls") ||
			strings.Contains(body, "quota") ||
			strings.Contains(body, "额度") ||
			strings.Contains(body, "rate limit") ||
			strings.Contains(body, "too many requests") {
			return true
		}
		// 5xx / 0 often transient on sticky primary under load.
		if ue.Status == 0 || ue.Status >= 500 {
			return false
		}
	}
	// Default: drop so a hard failure cannot sticky-loop forever.
	return true
}

// noteStickyOutcome clears sticky pins when the sticky primary fails so the
// failover winner can be rebound and subsequent turns keep cache warmth.
func (s *ChatService) noteStickyOutcome(ctx context.Context, request ChatRequest, triedID string, success bool) {
	if success {
		s.bindAffinity(ctx, request, triedID)
		return
	}
	prefer := s.stickyPreferAccount(ctx, request)
	if prefer != "" && prefer == strings.TrimSpace(triedID) {
		s.clearStickyPins(ctx, request)
	}
}

// stickyPreferAccount returns the account currently pinned for this request's
// sticky fingerprint (if any). Empty means no prior multi-turn pin.
func (s *ChatService) stickyPreferAccount(ctx context.Context, request ChatRequest) string {
	if s == nil || s.AffinityStore == nil {
		return ""
	}
	// Resolve sticky in priority order:
	//  1) model-scoped prompt_cache_key (tightest multi-turn pin)
	//  2) model-less prompt_cache_key (alias-tolerant recovery)
	//  3) ChatFingerprint (conversation/session/prev-response fallbacks)
	ctx = ctxOrBackground(ctx)
	if request.Raw != nil {
		if pck, _ := request.Raw["prompt_cache_key"].(string); strings.TrimSpace(pck) != "" {
			pck = strings.TrimSpace(pck)
			model := strings.TrimSpace(request.Model)
			if model != "" {
				if id, err := s.AffinityStore.GetAffinity(ctx, "chat:"+model+":prompt_cache_key:"+pck); err == nil {
					if v := strings.TrimSpace(id); v != "" {
						return v
					}
				}
			}
			if id, err := s.AffinityStore.GetAffinity(ctx, "chat:prompt_cache_key:"+pck); err == nil {
				if v := strings.TrimSpace(id); v != "" {
					return v
				}
			}
		}
	}
	fp := ChatFingerprint(request)
	if fp == "" {
		return ""
	}
	id, err := s.AffinityStore.GetAffinity(ctx, fp)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(id)
}

func (s *ChatService) bindAffinity(ctx context.Context, request ChatRequest, accountID string) {
	if s.AffinityStore == nil || accountID == "" {
		return
	}
	// Bind stable keys only. previous_response_id changes every turn and is handled
	// by bindResponseAffinity on the server responses path.
	// Refresh TTL on every successful sticky hit so long multi-turn sessions
	// (Claude Code / Codex) do not lose the pin mid-conversation.
	if request.Raw != nil {
		if pck, _ := request.Raw["prompt_cache_key"].(string); strings.TrimSpace(pck) != "" {
			model := strings.TrimSpace(request.Model)
			pck = strings.TrimSpace(pck)
			// Model-scoped key is the tight pin (preferred on lookup).
			if model != "" {
				_ = s.AffinityStore.BindAffinity(ctxOrBackground(ctx), "chat:"+model+":prompt_cache_key:"+pck, accountID)
			}
			// Model-less key for recovery when model alias differs slightly.
			_ = s.AffinityStore.BindAffinity(ctxOrBackground(ctx), "chat:prompt_cache_key:"+pck, accountID)
			return
		}
		for _, key := range []string{"conversation_id", "conversation", "thread_id", "session_id"} {
			if value, _ := request.Raw[key].(string); strings.TrimSpace(value) != "" {
				fp := "chat:" + strings.TrimSpace(request.Model) + ":" + key + ":" + strings.TrimSpace(value)
				_ = s.AffinityStore.BindAffinity(ctxOrBackground(ctx), fp, accountID)
				return
			}
		}
	}
	fingerprint := ChatFingerprint(request)
	if fingerprint == "" {
		return
	}
	// Avoid binding ephemeral previous_response_id as primary sticky (changes every turn).
	// Seed fingerprints ARE stable and must bind — OpenAI chat without pck relies on them.
	if strings.Contains(fingerprint, ":previous_response_id:") {
		return
	}
	_ = s.AffinityStore.BindAffinity(ctxOrBackground(ctx), fingerprint, accountID)
}

func upstreamAccounts(chain []pool.Candidate) []grok.Account {
	accounts := make([]grok.Account, 0, len(chain))
	for _, candidate := range chain {
		accounts = append(accounts, candidate.UpstreamAccount())
	}
	return accounts
}

// ensureUpstreamCacheKey guarantees body.prompt_cache_key survives Stabilize/Sanitize
// so cli-chat-proxy /responses receives the same key that affinity used. Without this,
// intermittent strip/empty pck makes x-grok-conv-id empty and cache hits go cold.
func ensureUpstreamCacheKey(body map[string]any, request ChatRequest) {
	if body == nil {
		return
	}
	if raw, _ := body["prompt_cache_key"].(string); strings.TrimSpace(raw) != "" {
		return
	}
	// Prefer request-level pck (Claude session / OpenAI mint).
	if request.Raw != nil {
		if pck, _ := request.Raw["prompt_cache_key"].(string); strings.TrimSpace(pck) != "" {
			body["prompt_cache_key"] = strings.TrimSpace(pck)
			return
		}
	}
	// Last resort: derive from Claude Code user field if present on body.
	if user, _ := body["user"].(string); strings.TrimSpace(user) != "" {
		if sid := anthropic.ExtractClaudeCodeSessionID(user); sid != "" {
			body["prompt_cache_key"] = sid
		}
	}
}

func ChatFingerprint(request ChatRequest) string {
	if request.Raw == nil {
		return ""
	}
	// Explicit sticky keys first (Codex / OpenAI Responses multi-turn).
	// Prefer prompt_cache_key over previous_response_id: pck is stable across turns,
	// while previous_response_id changes every turn and would fragment affinity maps.
	for _, key := range []string{"prompt_cache_key", "conversation_id", "conversation", "thread_id", "session_id", "previous_response_id"} {
		if value, _ := request.Raw[key].(string); strings.TrimSpace(value) != "" {
			return "chat:" + strings.TrimSpace(request.Model) + ":" + key + ":" + strings.TrimSpace(value)
		}
	}
	// Nested metadata (Anthropic / some relays). Skip bare user_id when it is only a
	// global user id (no session_ marker) — shared across unrelated chats and would
	// pin unrelated conversations onto one account.
	if meta, _ := request.Raw["metadata"].(map[string]any); meta != nil {
		for _, key := range []string{"prompt_cache_key", "session_id", "sessionId", "thread_id", "conversation_id"} {
			if value, _ := meta[key].(string); strings.TrimSpace(value) != "" {
				return "chat:" + strings.TrimSpace(request.Model) + ":meta:" + key + ":" + strings.TrimSpace(value)
			}
		}
		if uid, _ := meta["user_id"].(string); strings.TrimSpace(uid) != "" {
			if sid := anthropic.ExtractClaudeCodeSessionID(uid); sid != "" {
				return "chat:" + strings.TrimSpace(request.Model) + ":meta:session_id:" + sid
			}
		}
	}
	// Stable conversation seed (system prefix + first user message). Full messages
	// hash changes every turn and was the main OpenAI multi-turn cache-miss path:
	// new fingerprint → no sticky pin → random account → upstream prefix cold.
	if seed := stableConversationSeed(request.Raw); seed != "" {
		return "chat:" + strings.TrimSpace(request.Model) + ":seed:" + seed
	}
	return ""
}

// stableConversationSeed mirrors anthropic.ExtractPromptCacheKey's seed path so
// OpenAI chat without an explicit prompt_cache_key still gets a multi-turn-stable
// affinity key. Tools are intentionally excluded (clients mutate tool lists).
func stableConversationSeed(raw map[string]any) string {
	if raw == nil {
		return ""
	}
	// Prefer the shared extractor (session metadata + first-user seed).
	if pck := strings.TrimSpace(anthropic.ExtractPromptCacheKey(raw)); pck != "" {
		// Avoid double-prefixing when extractor already returned sess:/session_ ids.
		if strings.HasPrefix(pck, "sess:") {
			return strings.TrimPrefix(pck, "sess:")
		}
		sum := sha256.Sum256([]byte(pck))
		return hex.EncodeToString(sum[:16])
	}
	return ""
}

// ChatFingerprintFromHeaders picks sticky keys from common client/proxy headers
// (Codex X-Grok-Conv-Id, session/thread headers).
func ChatFingerprintFromHeaders(headers http.Header, model string) string {
	if headers == nil {
		return ""
	}
	get := func(names ...string) string {
		for _, name := range names {
			if v := strings.TrimSpace(headers.Get(name)); v != "" {
				return v
			}
		}
		return ""
	}
	if v := get("X-Grok-Conv-Id", "x-grok-conv-id", "X-Grok2API-Conv-Id"); v != "" {
		return "chat:" + strings.TrimSpace(model) + ":conv:" + v
	}
	if v := get("X-Session-Id", "x-session-id", "Session-Id"); v != "" {
		return "chat:" + strings.TrimSpace(model) + ":session:" + v
	}
	if v := get("X-Thread-Id", "x-thread-id", "Thread-Id"); v != "" {
		return "chat:" + strings.TrimSpace(model) + ":thread:" + v
	}
	if v := get("X-Prompt-Cache-Key", "x-prompt-cache-key"); v != "" {
		return "chat:" + strings.TrimSpace(model) + ":prompt_cache_key:" + v
	}
	return ""
}

func preferAffinity(ctx context.Context, candidates []pool.Candidate, store AffinityStore, fingerprint string) {
	accountID, err := store.GetAffinity(ctx, fingerprint)
	if err != nil || accountID == "" {
		return
	}
	idx := -1
	for i := range candidates {
		if candidates[i].ID == accountID {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	// Strong sticky pin: move to front and massively prefer in least_used ordering.
	candidates[idx].RequestCount -= 1_000_000_000
	if idx > 0 {
		cand := candidates[idx]
		copy(candidates[1:idx+1], candidates[0:idx])
		candidates[0] = cand
	}
}

func adjustCandidatesForObserver(ctx context.Context, candidates []pool.Candidate, observer PickObserver) {
	if observer == nil || len(candidates) == 0 {
		return
	}
	if batch, ok := observer.(batchPickObserver); ok {
		ids := make([]string, 0, len(candidates))
		for _, c := range candidates {
			if id := strings.TrimSpace(c.ID); id != "" {
				ids = append(ids, id)
			}
		}
		penalties := batch.LoadPenalties(ctx, ids)
		for i := range candidates {
			if p := penalties[candidates[i].ID]; p > 0 {
				candidates[i].RequestCount += p
			}
		}
		return
	}
	for i := range candidates {
		penalty := observer.LoadPenalty(ctx, candidates[i].ID)
		if penalty <= 0 {
			continue
		}
		candidates[i].RequestCount += penalty
	}
}

func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func numberToInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case float64:
		return int64(v), true
	case json.Number:
		n, err := v.Int64()
		return n, err == nil
	case int64:
		return v, true
	case int:
		return int64(v), true
	default:
		return 0, false
	}
}

// emptyStreamNoDataBudget is kept for callers/tests that pace delayed frames.
const emptyStreamNoDataBudget = 120 * time.Millisecond

// The guard uses the combined absolute and confirmation budgets as one peek
// window. Keeping the named parts preserves the configured 8s + 4s split while
// ensuring there is only one reader and one replay buffer.
const emptyStreamAbsBudget = 8 * time.Second
const emptyStreamConfirmBudget = 4 * time.Second

// emptyStreamPeekBudget is an alias kept for tests that time delays against the
// short silence window (historical name).
const emptyStreamPeekBudget = emptyStreamNoDataBudget

// guardStreamAgainstEmpty peeks upstream SSE until the first model payload or
// stream end. Empty HTTP 200 bodies can then failover before the client envelope
// is opened. On success, returns a reader that replays peeked frames + remainder.
//
// Single-reader design: one pump goroutine owns body.Read into a bounded buffer.
// The peeker consumes that buffer into one bounded replay. Hollow frames are
// observed for the combined absolute and confirmation budget; pure silence is
// passed live at the deadline for slow high-effort first tokens.
func guardStreamAgainstEmpty(body io.ReadCloser) (io.ReadCloser, bool, error) {
	if body == nil {
		return nil, true, &grok.UpstreamError{Status: 502, Body: "Upstream returned HTTP 200 with empty model output (no content/tool_calls)"}
	}

	pumped := newPumpedStream(body)

	type peekResult struct {
		sawModel   bool
		sawDone    bool
		sawHollow  bool
		replayFull bool
		cleanEOF   bool
		buffered   string
		err        error
	}

	stopPeek := make(chan struct{})
	var stopOnce sync.Once
	requestStop := func() { stopOnce.Do(func() { close(stopPeek) }) }

	resultCh := make(chan peekResult, 1)
	go func() {
		buffered := cappedReplayBuffer{limit: pumpedStreamMaxBuffered}
		sawModel := false
		sawDone := false
		sawHollow := false
		src := &peekerReader{p: pumped, stop: stopPeek}
		err := grok.ReadSSE(io.TeeReader(src, &buffered), func(event grok.Event) error {
			if event.Done {
				sawDone = true
				return errStopPeek
			}
			delta, parseErr := parseChatDelta(event.Data)
			if parseErr != nil {
				// Malformed / keepalive data still means the stream was not silent.
				sawHollow = true
				return nil
			}
			if strings.TrimSpace(delta.Content) != "" ||
				strings.TrimSpace(delta.Reasoning) != "" ||
				len(delta.ToolCalls) > 0 ||
				delta.FunctionCall != nil {
				sawModel = true
				return errStopPeek
			}
			sawHollow = true
			return nil
		})
		if errors.Is(err, errReplayLimit) {
			resultCh <- peekResult{sawHollow: sawHollow, replayFull: true, buffered: buffered.String()}
			return
		}
		if err != nil && !errors.Is(err, errStopPeek) {
			resultCh <- peekResult{err: err}
			return
		}
		resultCh <- peekResult{
			sawModel:  sawModel,
			sawDone:   sawDone,
			sawHollow: sawHollow || buffered.Len() > 0,
			cleanEOF:  err == nil,
			buffered:  buffered.String(),
		}
	}()

	finishLive := func(buffered string) io.ReadCloser {
		return &multiClose{
			Reader: io.MultiReader(strings.NewReader(buffered), pumped),
			closer: pumped,
		}
	}
	finish := func(result peekResult, timedOut bool) (io.ReadCloser, bool, error) {
		if result.err != nil {
			_ = pumped.Close()
			return nil, false, result.err
		}
		if result.sawModel {
			return finishLive(result.buffered), false, nil
		}
		if result.sawDone || result.cleanEOF || result.replayFull || (timedOut && result.sawHollow) {
			_ = pumped.Close()
			return io.NopCloser(strings.NewReader(result.buffered)), true, nil
		}
		// Only a completely silent stream is passed live at the deadline.
		return finishLive(result.buffered), false, nil
	}

	timer := time.NewTimer(emptyStreamAbsBudget + emptyStreamConfirmBudget)
	defer timer.Stop()
	select {
	case result := <-resultCh:
		return finish(result, false)
	case <-timer.C:
		requestStop()
		return finish(<-resultCh, true)
	}
}

// confirmStreamNotEmpty is retained for existing OpenStream call sites. The full
// confirmation budget is now consumed by guardStreamAgainstEmpty in one pass.
func confirmStreamNotEmpty(body io.ReadCloser) (io.ReadCloser, bool, error) {
	if body == nil {
		return nil, true, &grok.UpstreamError{Status: 502, Body: "Upstream returned HTTP 200 with empty model output (no content/tool_calls)"}
	}
	return body, false, nil
}

var errReplayLimit = errors.New("stream replay limit exceeded")

type cappedReplayBuffer struct {
	bytes.Buffer
	limit int
}

func (b *cappedReplayBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		return 0, errReplayLimit
	}
	return b.Buffer.Write(p)
}

var errStopPeek = errors.New("stop peek")

// peekerReader reads from pumpedStream but can be cancelled via stop while
// waiting for the first/next bytes (returns errStopPeek without consuming).
type peekerReader struct {
	p    *pumpedStream
	stop <-chan struct{}
}

func (r *peekerReader) Read(b []byte) (int, error) {
	if r == nil || r.p == nil {
		return 0, io.EOF
	}
	for {
		// Stop wins over already-buffered data so a continuously active upstream
		// cannot starve the deadline and keep the peeker goroutine running.
		select {
		case <-r.stop:
			return 0, errStopPeek
		default:
		}
		if n, err, ok := r.p.tryRead(b); ok {
			return n, err
		}
		select {
		case <-r.stop:
			return 0, errStopPeek
		case <-time.After(5 * time.Millisecond):
		}
	}
}

const pumpedStreamMaxBuffered = 256 << 10

// pumpedStream is a single-owner pump of an upstream body into a bounded
// buffer. Concurrent Read calls are serialized; only the pump goroutine calls
// src.Read. Peek and client both consume this buffer — no dual body.Read.
type pumpedStream struct {
	src    io.ReadCloser
	mu     sync.Mutex
	cond   *sync.Cond
	buf    bytes.Buffer
	err    error // set when pump ends (io.EOF or read error)
	closed bool
}

func newPumpedStream(src io.ReadCloser) *pumpedStream {
	p := &pumpedStream{src: src}
	p.cond = sync.NewCond(&p.mu)
	go p.pump()
	return p
}

func (p *pumpedStream) pump() {
	tmp := make([]byte, 32*1024)
	for {
		p.mu.Lock()
		for p.buf.Len() >= pumpedStreamMaxBuffered && !p.closed {
			p.cond.Wait()
		}
		if p.closed {
			p.mu.Unlock()
			return
		}
		available := pumpedStreamMaxBuffered - p.buf.Len()
		if available > len(tmp) {
			available = len(tmp)
		}
		p.mu.Unlock()

		n, err := p.src.Read(tmp[:available])
		p.mu.Lock()
		if n > 0 {
			_, _ = p.buf.Write(tmp[:n])
			p.cond.Broadcast()
		}
		if err != nil {
			if p.err == nil {
				p.err = err
			}
			p.cond.Broadcast()
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()
	}
}

// tryRead returns (n, err, true) when data/EOF/closed is available without blocking.
func (p *pumpedStream) tryRead(b []byte) (int, error, bool) {
	if p == nil {
		return 0, io.EOF, true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.buf.Len() > 0 {
		n, err := p.buf.Read(b)
		p.cond.Broadcast()
		return n, err, true
	}
	if p.closed {
		return 0, io.ErrClosedPipe, true
	}
	if p.err != nil {
		return 0, p.err, true
	}
	return 0, nil, false
}

func (p *pumpedStream) Read(b []byte) (int, error) {
	if p == nil {
		return 0, io.EOF
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.buf.Len() == 0 && p.err == nil && !p.closed {
		p.cond.Wait()
	}
	if p.buf.Len() > 0 {
		n, err := p.buf.Read(b)
		p.cond.Broadcast()
		return n, err
	}
	if p.closed {
		return 0, io.ErrClosedPipe
	}
	if p.err != nil {
		return 0, p.err
	}
	return 0, io.EOF
}

func (p *pumpedStream) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.cond.Broadcast()
	src := p.src
	p.mu.Unlock()
	if src == nil {
		return nil
	}
	return src.Close()
}

type multiClose struct {
	io.Reader
	closer io.Closer
}

func (m *multiClose) Close() error {
	if m.closer == nil {
		return nil
	}
	return m.closer.Close()
}

// reportAccountFailure notifies the optional FailureReporter about a failed
// attempt. Used for intermediate failover losers so free-usage / 额度用完 bodies
// still kick the account into the cooldown pool even when another account wins.
func (s *ChatService) reportAccountFailure(accountID, model string, err error) {
	if s == nil || s.FailureReporter == nil || err == nil {
		return
	}
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return
	}
	s.FailureReporter.ReportAccountFailure(accountID, model, err)
}

func (s *ChatService) markAttempt(ctx context.Context, accountID string) {
	if s == nil || s.PickObserver == nil || strings.TrimSpace(accountID) == "" {
		return
	}
	s.PickObserver.MarkPick(ctxOrBackground(ctx), accountID)
}

func remainingAccounts(accounts []grok.Account, usedID string) []grok.Account {
	out := make([]grok.Account, 0, len(accounts))
	for _, account := range accounts {
		if account.ID == usedID {
			continue
		}
		out = append(out, account)
	}
	return out
}

func (c *chatCollector) emptyModelOutput() bool {
	if len(c.toolCalls) > 0 || c.functionCall != nil {
		return false
	}
	if strings.TrimSpace(c.content) != "" {
		return false
	}
	if strings.TrimSpace(c.reasoning) != "" {
		return false
	}
	return true
}
