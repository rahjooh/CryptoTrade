package rate

import (
	"strconv"
	"strings"
)

// extractInts returns all integer substrings contained in s. Any non-digit
// characters are treated as separators. Missing or unparsable values result in
// an empty slice.
func extractInts(s string) []int64 {
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r < '0' || r > '9'
	})
	nums := make([]int64, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			continue
		}
		if n, err := strconv.ParseInt(p, 10, 64); err == nil {
			nums = append(nums, n)
		}
	}
	return nums
}
