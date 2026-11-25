package sessionruntime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Pod configuration for the Claude credential chain; absent means no failover.
const (
	claudeTiersDirEnv   = "KELOS_CLAUDE_TIERS_DIR"
	claudeTierOrderEnv  = "KELOS_CLAUDE_TIER_ORDER"
	claudeBifrostEnvEnv = "KELOS_CLAUDE_BIFROST_ENV"

	claudeTierBifrost    = "bifrost"
	claudeTierStateFile  = "claude-tier-state.json"
	defaultTierCooldown  = time.Hour
	defaultTierChainText = "primary,secondary,bifrost"
)

// Every variable that selects how Claude Code authenticates; a tier owns all of them.
var claudeCredentialEnv = []string{
	"CLAUDE_CODE_OAUTH_TOKEN",
	"ANTHROPIC_API_KEY",
	"ANTHROPIC_AUTH_TOKEN",
	"ANTHROPIC_BASE_URL",
	"CLAUDE_CODE_USE_BEDROCK",
}

type claudeTier struct {
	name string
	// A token hash prefix, so a swapped token is noticed without logging the token.
	fingerprint string
	env         []string
}

type claudeTierConfig struct {
	dir     string
	order   []string
	bifrost map[string]string
}

func claudeTierConfigFromEnvironment(environment []string) (claudeTierConfig, bool) {
	dir := environmentValue(environment, claudeTiersDirEnv)
	var bifrost map[string]string
	if raw := environmentValue(environment, claudeBifrostEnvEnv); raw != "" {
		if err := json.Unmarshal([]byte(raw), &bifrost); err != nil {
			logClaudeTier("config_invalid", "", claudeBifrostEnvEnv+" is not a JSON object of strings")
			bifrost = nil
		}
	}
	if dir == "" && len(bifrost) == 0 {
		return claudeTierConfig{}, false
	}
	order := environmentValue(environment, claudeTierOrderEnv)
	if order == "" {
		order = defaultTierChainText
	}
	config := claudeTierConfig{dir: dir, bifrost: bifrost}
	for _, name := range strings.Split(order, ",") {
		if name = strings.TrimSpace(name); name != "" {
			config.order = append(config.order, name)
		}
	}
	return config, true
}

// available reads the tiers that hold a credential right now, in chain order.
func (c claudeTierConfig) available(base []string) []claudeTier {
	var tiers []claudeTier
	for _, name := range c.order {
		if name == claudeTierBifrost {
			if len(c.bifrost) == 0 {
				continue
			}
			keys := make([]string, 0, len(c.bifrost))
			for key := range c.bifrost {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			env := stripEnvironment(base, claudeCredentialEnv)
			for _, key := range keys {
				env = append(env, key+"="+c.bifrost[key])
			}
			tiers = append(tiers, claudeTier{name: name, fingerprint: name, env: env})
			continue
		}
		if c.dir == "" || strings.ContainsAny(name, "/\\") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(c.dir, name))
		token := strings.TrimSpace(string(data))
		if err != nil || token == "" {
			continue
		}
		sum := sha256.Sum256([]byte(token))
		env := append(stripEnvironment(base, claudeCredentialEnv), "CLAUDE_CODE_OAUTH_TOKEN="+token)
		tiers = append(tiers, claudeTier{name: name, fingerprint: hex.EncodeToString(sum[:4]), env: env})
	}
	return tiers
}

func stripEnvironment(environment []string, names []string) []string {
	result := make([]string, 0, len(environment)+2)
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		drop := false
		for _, candidate := range names {
			drop = drop || name == candidate
		}
		if !drop {
			result = append(result, entry)
		}
	}
	return result
}

var (
	claudeLimitPattern = regexp.MustCompile(`(?i)(hit your [a-z0-9 -]{0,20}limit|usage limit|session limit|weekly limit|rate.?limit|too many requests|credit balance is too low|\b429\b)`)
	claudeAuthPattern  = regexp.MustCompile(`(?i)(authentication_error|invalid (x-api-key|api key|bearer token|authentication credentials)|oauth token (has expired|has been revoked)|failed to authenticate|please run /login|\b401\b)`)
	claudeResetPattern = regexp.MustCompile(`(?i)resets?\s+(?:at\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)(?:\s*\(([^)]+)\))?`)
)

// claudeTierFailure reports an error another credential could cure: a limit or a rejected token.
func claudeTierFailure(text string) bool {
	return claudeLimitPattern.MatchString(text) || claudeAuthPattern.MatchString(text)
}

// claudeResetTime reads "resets 1:40pm (UTC)" as the next such wall-clock time after now.
func claudeResetTime(text string, now time.Time) time.Time {
	match := claudeResetPattern.FindStringSubmatch(text)
	if match == nil {
		return now.Add(defaultTierCooldown)
	}
	hour, _ := strconv.Atoi(match[1])
	minute := 0
	if match[2] != "" {
		minute, _ = strconv.Atoi(match[2])
	}
	if hour < 1 || hour > 12 || minute > 59 {
		return now.Add(defaultTierCooldown)
	}
	hour %= 12
	if strings.EqualFold(match[3], "pm") {
		hour += 12
	}
	location := time.UTC
	if zone := strings.TrimSpace(match[4]); zone != "" && !strings.EqualFold(zone, "utc") {
		loaded, err := time.LoadLocation(zone)
		if err != nil {
			return now.Add(defaultTierCooldown)
		}
		location = loaded
	}
	local := now.In(location)
	reset := time.Date(local.Year(), local.Month(), local.Day(), hour, minute, 0, 0, location)
	if !reset.After(now) {
		reset = reset.AddDate(0, 0, 1)
	}
	return reset
}

// claudeTierState remembers which tiers are cooling down, across Pod restarts.
type claudeTierState struct {
	mu      sync.Mutex
	path    string
	cooling map[string]claudeCooldown
}

type claudeCooldown struct {
	Until       time.Time `json:"until"`
	Fingerprint string    `json:"fingerprint"`
}

func loadClaudeTierState(stateDir string) *claudeTierState {
	state := &claudeTierState{cooling: map[string]claudeCooldown{}}
	if stateDir == "" {
		return state
	}
	state.path = filepath.Join(stateDir, claudeTierStateFile)
	if data, err := os.ReadFile(state.path); err == nil {
		_ = json.Unmarshal(data, &state.cooling)
		if state.cooling == nil {
			state.cooling = map[string]claudeCooldown{}
		}
	}
	return state
}

func (s *claudeTierState) coolDown(tier claudeTier, until time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cooling[tier.name] = claudeCooldown{Until: until, Fingerprint: tier.fingerprint}
	if s.path == "" {
		return
	}
	if data, err := json.Marshal(s.cooling); err == nil {
		_ = os.WriteFile(s.path, data, 0600)
	}
}

// usable is true for a replaced credential whatever the old one's reset time said.
func (s *claudeTierState) usable(tier claudeTier, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cooldown, ok := s.cooling[tier.name]
	return !ok || cooldown.Fingerprint != tier.fingerprint || !now.Before(cooldown.Until)
}

// pick returns the first usable tier not yet tried, else the soonest to recover if none tried.
func (s *claudeTierState) pick(tiers []claudeTier, tried map[string]bool, now time.Time) (claudeTier, bool) {
	for _, tier := range tiers {
		if !tried[tier.name] && s.usable(tier, now) {
			return tier, true
		}
	}
	if len(tried) > 0 || len(tiers) == 0 {
		return claudeTier{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	best := tiers[0]
	for _, tier := range tiers[1:] {
		if s.cooling[tier.name].Until.Before(s.cooling[best.name].Until) {
			best = tier
		}
	}
	return best, true
}

func logClaudeTier(event, tier, detail string) {
	line, _ := json.Marshal(map[string]string{"evt": "claude_tier", "event": event, "tier": tier, "detail": detail})
	fmt.Fprintln(os.Stderr, string(line))
}
