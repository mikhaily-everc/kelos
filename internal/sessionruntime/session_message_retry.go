package sessionruntime

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

// sessionMessageRetryDelays: Claude Code has already retried the API call, so retry 1 waits too.
var sessionMessageRetryDelays = []time.Duration{30 * time.Second, 2 * time.Minute, 5 * time.Minute}

var (
	// 2026-11-26 dev: the gateway OOM-restarted and the token proxy answered 502 bad_gateway.
	sessionTransientPattern = regexp.MustCompile(`(?i)(\b50[234]\b|\b529\b|overloaded|` +
		`could not obtain the bifrost key|bifrost unreachable|gateway unreachable|` +
		`could not obtain a gateway access token|upstream connect error|` +
		`disconnect/reset before headers|econnreset|connection reset)`)
	sessionPermanentPattern = regexp.MustCompile(`(?i)(\b40[03]\b|invalid_request|invalid request|` +
		`permission_error|forbidden)`)
)

// sessionTransientFailure reports a turn error that a later run of the same message may not hit.
func sessionTransientFailure(text string) bool {
	if claudeTierFailure(text) || sessionPermanentPattern.MatchString(text) {
		return false
	}
	return sessionTransientPattern.MatchString(text)
}

func sessionMessageMaxRetries() int { return len(sessionMessageRetryDelays) }

// sessionMessageAttemptSuffix: attempt 1 keeps the legacy "-retry" so older journals still recover.
func sessionMessageAttemptSuffix(attempt int) string {
	switch {
	case attempt <= 0:
		return ""
	case attempt == 1:
		return sessionMessageRetrySuffix
	default:
		return sessionMessageRetrySuffix + strconv.Itoa(attempt)
	}
}

// sessionMessageAttempt is the attempt a request ID was delivered as; 0 is the first delivery.
func sessionMessageAttempt(requestID string) int {
	index := strings.LastIndex(requestID, sessionMessageRetrySuffix)
	if index < 0 {
		return 0
	}
	rest := requestID[index+len(sessionMessageRetrySuffix):]
	if rest == "" {
		return 1
	}
	attempt, err := strconv.Atoi(rest)
	if err != nil || attempt < 2 {
		return 0
	}
	return attempt
}
