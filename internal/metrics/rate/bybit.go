package rate

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"cryptoflow/logger"
)

// ReportBybitSnapshotWeight parses Bybit rate-limit headers and emits a
// `used_weight` metric for the given IP. It falls back between legacy
// X-Bapi-* headers and the newer X-RateLimit-* variants.
func ReportBybitSnapshotWeight(log *logger.Log, header http.Header, ip string) {
	// Bybit has changed header names over time; try both the old X-Bapi-*
	// headers and the generic X-RateLimit-* variants. Missing headers are
	// treated as zero which previously resulted in metrics always reading
	// zero. Falling back ensures metrics are populated regardless of which
	// set is returned by the API.
	limitStr := header.Get("X-Bapi-Limit")
	if limitStr == "" {
		limitStr = header.Get("X-RateLimit-Limit")
	}

	statusStr := header.Get("X-Bapi-Limit-Status")
	if statusStr == "" {
		statusStr = header.Get("X-RateLimit-Remaining")
	}

	usedStr := header.Get("X-Bapi-Used")
	if usedStr == "" {
		usedStr = header.Get("X-RateLimit-Used")
	}

	used := computeBybitUsed(limitStr, statusStr, usedStr, ip)
	if used < 0 {
		used = 0
	}

	l := log.WithComponent("bybit_reader")
	fields := logger.Fields{"ip": ip}
	l.LogMetric("bybit_reader", "used_weight", used, "gauge", fields)
}

type bybitUsage struct {
	Limit     int64
	Remaining int64
	Used      int64
}

func computeBybitUsed(limitStr, statusStr, usedStr, ip string) int64 {
	statusUsage := parseBybitUsage(statusStr, ip)
	generalUsage := statusUsage
	if ip != "" {
		generalUsage = parseBybitUsage(statusStr, "")
	}

	limit, _ := parseBybitLimit(limitStr, ip, statusUsage, generalUsage)
	used, hasUsed := parseBybitUsedHeader(usedStr, ip, statusUsage, generalUsage)
	remaining := statusUsage.Remaining
	if remaining == 0 {
		remaining = generalUsage.Remaining
	}

	if limit == 0 {
		if statusUsage.Limit > 0 {
			limit = statusUsage.Limit
		} else if generalUsage.Limit > 0 {
			limit = generalUsage.Limit
		}
	}

	if !hasUsed {
		if statusUsage.Used > 0 {
			used = statusUsage.Used
			hasUsed = true
		} else if generalUsage.Used > 0 {
			used = generalUsage.Used
			hasUsed = true
		}
	}

	if !hasUsed && limit > 0 && remaining > 0 {
		used = limit - remaining
		hasUsed = true
	}

	if !hasUsed && limit > 0 && generalUsage.Remaining > 0 {
		used = limit - generalUsage.Remaining
		hasUsed = true
	}

	if !hasUsed && statusUsage.Limit > 0 && statusUsage.Remaining > 0 {
		used = statusUsage.Limit - statusUsage.Remaining
		hasUsed = true
	}

	if !hasUsed && generalUsage.Limit > 0 && generalUsage.Remaining > 0 {
		used = generalUsage.Limit - generalUsage.Remaining
		hasUsed = true
	}

	if limit > 0 && used > limit {
		return limit
	}

	return used
}

func parseBybitLimit(limitStr, ip string, statusUsage, generalUsage bybitUsage) (int64, bool) {
	if limitStr != "" {
		if ip != "" {
			if usage, ok := parseBybitUsageForIP(limitStr, ip); ok {
				switch {
				case usage.Limit > 0:
					return usage.Limit, true
				case usage.Remaining > 0 && usage.Limit == 0:
					return usage.Remaining, true
				}
			}
		}
		if usage, ok := parseBybitUsageFromString(limitStr); ok {
			switch {
			case usage.Limit > 0:
				return usage.Limit, true
			case usage.Remaining > 0 && usage.Limit == 0:
				return usage.Remaining, true
			}
		}
	}
	if statusUsage.Limit > 0 {
		return statusUsage.Limit, true
	}
	if generalUsage.Limit > 0 {
		return generalUsage.Limit, true
	}
	return 0, false
}

func parseBybitUsedHeader(usedStr, ip string, statusUsage, generalUsage bybitUsage) (int64, bool) {
	if usedStr != "" {
		if ip != "" {
			if usage, ok := parseBybitUsageForIP(usedStr, ip); ok {
				switch {
				case usage.Used > 0:
					return usage.Used, true
				case usage.Remaining > 0 && usage.Limit == 0:
					return usage.Remaining, true
				case usage.Limit > 0 && usage.Remaining > 0:
					return usage.Limit - usage.Remaining, true
				}
			}
		}
		if usage, ok := parseBybitUsageFromString(usedStr); ok {
			switch {
			case usage.Used > 0:
				return usage.Used, true
			case usage.Remaining > 0 && usage.Limit == 0:
				return usage.Remaining, true
			case usage.Limit > 0 && usage.Remaining > 0:
				return usage.Limit - usage.Remaining, true
			}
		}
	}
	if statusUsage.Used > 0 {
		return statusUsage.Used, true
	}
	if generalUsage.Used > 0 {
		return generalUsage.Used, true
	}
	return 0, false
}

func parseBybitUsage(source string, ip string) bybitUsage {
	if source == "" {
		return bybitUsage{}
	}
	if ip != "" {
		if usage, ok := parseBybitUsageForIP(source, ip); ok {
			return usage
		}
	}
	if usage, ok := parseBybitUsageFromString(source); ok {
		return usage
	}
	return bybitUsage{}
}

func parseBybitUsageForIP(source, ip string) (bybitUsage, bool) {
	if usage, ok := parseBybitUsageForIPJSON(source, ip); ok {
		return usage, true
	}
	if usage, ok := parseBybitUsageForIPString(source, ip); ok {
		return usage, true
	}
	return bybitUsage{}, false
}

func parseBybitUsageForIPJSON(source, ip string) (bybitUsage, bool) {
	trimmed := strings.TrimSpace(source)
	if trimmed == "" || ip == "" {
		return bybitUsage{}, false
	}
	if !strings.HasPrefix(trimmed, "{") && !strings.HasPrefix(trimmed, "[") {
		return bybitUsage{}, false
	}
	dec := json.NewDecoder(strings.NewReader(trimmed))
	dec.UseNumber()
	var raw interface{}
	if err := dec.Decode(&raw); err != nil {
		return bybitUsage{}, false
	}
	return findUsageForIP(raw, ip)
}

func parseBybitUsageForIPString(source, ip string) (bybitUsage, bool) {
	fragment := substringAfterIP(source, ip)
	if fragment != "" {
		if usage, ok := parseBybitUsageFromString(fragment); ok {
			return usage, true
		}
	}
	tokens := splitTokens(source)
	ipLower := strings.ToLower(ip)
	sanitized := strings.ReplaceAll(ipLower, ".", "_")
	sanitizedHyphen := strings.ReplaceAll(ipLower, ".", "-")
	for _, token := range tokens {
		lower := strings.ToLower(token)
		if strings.Contains(lower, ipLower) || strings.Contains(lower, sanitized) || strings.Contains(lower, sanitizedHyphen) {
			fragment = substringAfterIP(token, ip)
			if fragment == "" {
				fragment = token
			}
			if usage, ok := parseBybitUsageFromString(fragment); ok {
				return usage, true
			}
		}
	}
	return bybitUsage{}, false
}

func parseBybitUsageFromString(source string) (bybitUsage, bool) {
	trimmed := strings.TrimSpace(source)
	if trimmed == "" {
		return bybitUsage{}, false
	}
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		if usage, ok := parseBybitUsageJSON(trimmed); ok {
			return usage, true
		}
	}
	return parseBybitUsageText(trimmed)
}

func parseBybitUsageJSON(source string) (bybitUsage, bool) {
	dec := json.NewDecoder(strings.NewReader(source))
	dec.UseNumber()
	var raw interface{}
	if err := dec.Decode(&raw); err != nil {
		return bybitUsage{}, false
	}
	return parseUsageValue(raw)
}

func parseBybitUsageText(source string) (bybitUsage, bool) {
	usage := bybitUsage{}
	if u, l, ok := parseSlashPair(source); ok {
		usage.Used = u
		usage.Limit = l
	}
	if usage.Remaining == 0 {
		if n, ok := extractValueByKeys(source, []string{"remaining", "remain", "left", "available"}); ok {
			usage.Remaining = n
		}
	}
	if usage.Used == 0 {
		if n, ok := extractValueByKeys(source, []string{"used", "consume", "usage", "cost"}); ok {
			usage.Used = n
		}
	}
	if usage.Limit == 0 {
		if n, ok := extractValueByKeys(source, []string{"limit", "quota", "cap"}); ok {
			usage.Limit = n
		}
	}
	if usage.Limit == 0 && usage.Used == 0 && usage.Remaining == 0 {
		nums := extractNumbers(source)
		if len(nums) == 1 {
			usage.Remaining = nums[0]
		} else if len(nums) >= 2 {
			usage.Used = nums[0]
			usage.Limit = nums[1]
		}
	}
	if usage.Limit > 0 && usage.Remaining > 0 && usage.Used == 0 {
		usage.Used = usage.Limit - usage.Remaining
	} else if usage.Limit > 0 && usage.Used > 0 && usage.Remaining == 0 {
		usage.Remaining = usage.Limit - usage.Used
	}
	if usage.Limit == 0 && usage.Used == 0 && usage.Remaining == 0 {
		return usage, false
	}
	return usage, true
}

func findUsageForIP(val interface{}, ip string) (bybitUsage, bool) {
	switch v := val.(type) {
	case map[string]interface{}:
		for key, child := range v {
			if keyMatchesIP(key, ip) {
				return parseUsageValue(child)
			}
		}
		for key, child := range v {
			lower := strings.ToLower(key)
			if strings.Contains(lower, "ip") || strings.Contains(lower, "addr") || strings.Contains(lower, "address") {
				if usage, ok := findUsageForIP(child, ip); ok {
					return usage, true
				}
			}
		}
		for _, child := range v {
			if usage, ok := findUsageForIP(child, ip); ok {
				return usage, true
			}
		}
	case []interface{}:
		for _, child := range v {
			if usage, ok := findUsageForIP(child, ip); ok {
				return usage, true
			}
		}
	case string:
		if strings.Contains(v, ip) || strings.Contains(strings.ToLower(v), strings.ReplaceAll(strings.ToLower(ip), ".", "_")) {
			fragment := substringAfterIP(v, ip)
			if usage, ok := parseBybitUsageFromString(fragment); ok {
				return usage, true
			}
		}
	}
	return bybitUsage{}, false
}

func parseUsageValue(val interface{}) (bybitUsage, bool) {
	switch v := val.(type) {
	case map[string]interface{}:
		usage := bybitUsage{}
		found := false
		for key, child := range v {
			lower := strings.ToLower(key)
			switch {
			case strings.Contains(lower, "limit") || strings.Contains(lower, "quota") || strings.Contains(lower, "cap"):
				if n, ok := toInt(child); ok {
					usage.Limit = n
					found = true
					continue
				}
			case strings.Contains(lower, "remain") || strings.Contains(lower, "left") || strings.Contains(lower, "available"):
				if n, ok := toInt(child); ok {
					usage.Remaining = n
					found = true
					continue
				}
			case strings.Contains(lower, "used") || strings.Contains(lower, "consume") || strings.Contains(lower, "usage") || strings.Contains(lower, "cost"):
				if n, ok := toInt(child); ok {
					usage.Used = n
					found = true
					continue
				}
			}
			if subUsage, ok := parseUsageValue(child); ok {
				mergeUsage(&usage, subUsage)
				found = true
			}
		}
		if usage.Limit > 0 && usage.Remaining > 0 && usage.Used == 0 {
			usage.Used = usage.Limit - usage.Remaining
		} else if usage.Limit > 0 && usage.Used > 0 && usage.Remaining == 0 {
			usage.Remaining = usage.Limit - usage.Used
		}
		return usage, found
	case []interface{}:
		usage := bybitUsage{}
		found := false
		for _, child := range v {
			if subUsage, ok := parseUsageValue(child); ok {
				mergeUsage(&usage, subUsage)
				found = true
			}
		}
		return usage, found
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return bybitUsage{Remaining: n}, true
		}
		if f, err := v.Float64(); err == nil {
			return bybitUsage{Remaining: int64(f)}, true
		}
	case float64:
		return bybitUsage{Remaining: int64(v)}, true
	case string:
		return parseBybitUsageText(v)
	case int64:
		return bybitUsage{Remaining: v}, true
	case int32:
		return bybitUsage{Remaining: int64(v)}, true
	case int:
		return bybitUsage{Remaining: int64(v)}, true
	}
	return bybitUsage{}, false
}

func mergeUsage(dst *bybitUsage, src bybitUsage) {
	if dst.Limit == 0 {
		dst.Limit = src.Limit
	}
	if dst.Remaining == 0 {
		dst.Remaining = src.Remaining
	}
	if dst.Used == 0 {
		dst.Used = src.Used
	}
}

func keyMatchesIP(key, ip string) bool {
	if ip == "" {
		return false
	}
	keyLower := strings.ToLower(strings.TrimSpace(key))
	ipLower := strings.ToLower(strings.TrimSpace(ip))
	if keyLower == ipLower {
		return true
	}
	variants := []string{
		strings.ReplaceAll(ipLower, ".", "_"),
		strings.ReplaceAll(ipLower, ".", "-"),
		strings.ReplaceAll(ipLower, ".", ""),
	}
	for _, variant := range variants {
		if variant != "" && keyLower == variant {
			return true
		}
	}
	if strings.Contains(keyLower, ipLower) {
		return true
	}
	for _, variant := range variants {
		if variant != "" && strings.Contains(keyLower, variant) {
			return true
		}
	}
	return false
}

func substringAfterIP(s, ip string) string {
	if ip == "" {
		return ""
	}
	idx, length := indexOfIPToken(s, ip)
	if idx == -1 {
		return ""
	}
	start := idx + length
	for start < len(s) {
		switch s[start] {
		case ' ', '\t', '\n', '\r', '\f', '\v', '"', '\'', ':', '=':
			start++
		default:
			goto foundStart
		}
	}
foundStart:
	if start >= len(s) {
		return ""
	}
	switch s[start] {
	case '{':
		depth := 1
		end := start + 1
		for end < len(s) && depth > 0 {
			switch s[end] {
			case '{':
				depth++
			case '}':
				depth--
			}
			end++
		}
		return s[start:end]
	case '[':
		depth := 1
		end := start + 1
		for end < len(s) && depth > 0 {
			switch s[end] {
			case '[':
				depth++
			case ']':
				depth--
			}
			end++
		}
		return s[start:end]
	}
	end := start
	for end < len(s) {
		switch s[end] {
		case ',', ';':
			return s[start:end]
		case '}', ']':
			return s[start:end]
		}
		end++
	}
	return s[start:]
}

func indexOfIPToken(s, ip string) (int, int) {
	ipVariants := []string{
		ip,
		strings.ToLower(ip),
		strings.ToUpper(ip),
		strings.ReplaceAll(ip, ".", "_"),
		strings.ReplaceAll(strings.ToLower(ip), ".", "_"),
		strings.ReplaceAll(strings.ToUpper(ip), ".", "_"),
		strings.ReplaceAll(ip, ".", "-"),
		strings.ReplaceAll(strings.ToLower(ip), ".", "-"),
		strings.ReplaceAll(strings.ToUpper(ip), ".", "-"),
	}
	lower := strings.ToLower(s)
	for _, candidate := range ipVariants {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if idx := strings.Index(lower, strings.ToLower(candidate)); idx != -1 {
			return idx, len(candidate)
		}
	}
	compact := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(ip)), ".", "")
	if compact != "" {
		if idx := strings.Index(lower, compact); idx != -1 {
			return idx, len(compact)
		}
	}
	return -1, 0
}

func splitTokens(s string) []string {
	tokens := []string{}
	var current strings.Builder
	depth := 0
	for _, r := range s {
		switch r {
		case '{', '[':
			depth++
			current.WriteRune(r)
		case '}', ']':
			depth--
			if depth < 0 {
				depth = 0
			}
			current.WriteRune(r)
		case ',', ';':
			if depth == 0 {
				if current.Len() > 0 {
					tokens = append(tokens, strings.TrimSpace(current.String()))
					current.Reset()
				}
			} else {
				current.WriteRune(r)
			}
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, strings.TrimSpace(current.String()))
	}
	return tokens
}

func extractValueByKeys(source string, keys []string) (int64, bool) {
	lower := strings.ToLower(source)
	for _, key := range keys {
		search := lower
		offset := 0
		for {
			idx := strings.Index(search, key)
			if idx == -1 {
				break
			}
			realIdx := offset + idx
			before := rune(0)
			if realIdx > 0 {
				before = rune(lower[realIdx-1])
			}
			afterIdx := realIdx + len(key)
			after := rune(0)
			if afterIdx < len(lower) {
				after = rune(lower[afterIdx])
			}
			if !isAlphaNum(before) && !isAlphaNum(after) {
				if n, ok := extractFirstInt(source[afterIdx:]); ok {
					return n, true
				}
			}
			nextStart := realIdx + len(key)
			if nextStart >= len(lower) {
				break
			}
			search = lower[nextStart:]
			offset = nextStart
		}
	}
	return 0, false
}

func extractFirstInt(s string) (int64, bool) {
	start := -1
	for i, r := range s {
		if r >= '0' && r <= '9' {
			start = i
			break
		}
	}
	if start == -1 {
		return 0, false
	}
	end := start
	for end < len(s) {
		if s[end] < '0' || s[end] > '9' {
			break
		}
		end++
	}
	n, err := strconv.ParseInt(s[start:end], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func extractNumbers(s string) []int64 {
	res := make([]int64, 0)
	var current int64
	has := false
	for _, r := range s {
		if r >= '0' && r <= '9' {
			current = current*10 + int64(r-'0')
			has = true
		} else {
			if has {
				res = append(res, current)
				current = 0
				has = false
			}
		}
	}
	if has {
		res = append(res, current)
	}
	return res
}

func parseSlashPair(s string) (int64, int64, bool) {
	if !strings.Contains(s, "/") {
		return 0, 0, false
	}
	parts := strings.Split(s, "/")
	if len(parts) < 2 {
		return 0, 0, false
	}
	leftNums := extractNumbers(parts[0])
	if len(leftNums) == 0 {
		return 0, 0, false
	}
	rightNums := extractNumbers(strings.Join(parts[1:], "/"))
	if len(rightNums) == 0 {
		return 0, 0, false
	}
	return leftNums[len(leftNums)-1], rightNums[0], true
}

func toInt(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return n, true
		}
		if f, err := v.Float64(); err == nil {
			return int64(f), true
		}
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int:
		return int64(v), true
	case string:
		return extractFirstInt(v)
	}
	return 0, false
}

func isAlphaNum(r rune) bool {
	if r >= 'a' && r <= 'z' {
		return true
	}
	if r >= '0' && r <= '9' {
		return true
	}
	return r == '_' || r == '-'
}

// BybitWSWeightTracker tracks outgoing websocket messages and connection
// attempts for Bybit futures order book delta streams. While Bybit does not
// charge websocket market data against REST limits, tracking our own message
// and reconnect cadence helps avoid self-induced rate issues.
type BybitWSWeightTracker struct {
	mu       sync.Mutex
	window   time.Time
	msgs     int
	attempts int
}

// NewBybitWSWeightTracker creates a new tracker.
func NewBybitWSWeightTracker() *BybitWSWeightTracker {
	return &BybitWSWeightTracker{window: time.Now()}
}

// RegisterOutgoing records n outgoing client messages (subs/pings).
func (t *BybitWSWeightTracker) RegisterOutgoing(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if now.Sub(t.window) >= time.Second {
		t.msgs = 0
		t.window = now
	}
	t.msgs += n
}

// RegisterConnectionAttempt records a websocket handshake attempt.
func (t *BybitWSWeightTracker) RegisterConnectionAttempt() {
	t.mu.Lock()
	t.attempts++
	t.mu.Unlock()
}

// Stats returns the current message count within the one second window and the
// total connection attempts.
func (t *BybitWSWeightTracker) Stats() (msgs int, attempts int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	msgs = t.msgs
	attempts = t.attempts
	return
}

// ReportBybitWSWeight emits websocket related weight metrics.
func ReportBybitWSWeight(log *logger.Log, t *BybitWSWeightTracker, ip string) {
	msgs, attempts := t.Stats()
	l := log.WithComponent("bybit_delta_reader")
	fields := logger.Fields{"ip": ip}
	l.LogMetric("bybit_delta_reader", "outgoing_messages", int64(msgs), "gauge", fields)
	l.LogMetric("bybit_delta_reader", "connection_attempts", int64(attempts), "counter", fields)
}
