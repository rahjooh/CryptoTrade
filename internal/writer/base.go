package writer

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"cryptoflow/config"
)

const defaultPartitionTimeFormat = "date={year}-{month}-{day}"

// BufferKey normalizes exchange/market/symbol for writer buffers.
func BufferKey(exchange, market, symbol string) string {
	return strings.Join([]string{
		strings.ToLower(strings.TrimSpace(exchange)),
		strings.ToLower(strings.TrimSpace(market)),
		strings.ToUpper(strings.TrimSpace(symbol)),
	}, "|")
}

// BuildPartitionParts constructs partition path segments using configured keys.
func BuildPartitionParts(partition config.PartitioningConfig, timestamp time.Time, values map[string]string) []string {
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}

	parts := make([]string, 0, len(partition.AdditionalKeys)+1)
	for _, key := range partition.AdditionalKeys {
		if val, ok := values[key]; ok {
			val = strings.TrimSpace(val)
			if val == "" {
				continue
			}
			parts = append(parts, fmt.Sprintf("%s=%s", key, val))
		}
	}

	timeFormat := partition.TimeFormat
	if timeFormat == "" {
		timeFormat = defaultPartitionTimeFormat
	}

	parts = append(parts, formatTimePartition(timeFormat, timestamp))
	return parts
}

func formatTimePartition(format string, timestamp time.Time) string {
	replacer := strings.NewReplacer(
		"{year}", fmt.Sprintf("%04d", timestamp.Year()),
		"{month}", fmt.Sprintf("%02d", timestamp.Month()),
		"{day}", fmt.Sprintf("%02d", timestamp.Day()),
		"{hour}", fmt.Sprintf("%02d", timestamp.Hour()),
	)
	return replacer.Replace(format)
}

// BuildS3Key joins partition parts and filename into a slash-separated key.
func BuildS3Key(parts []string, filename string) string {
	if len(parts) == 0 && filename == "" {
		return ""
	}
	path := append([]string{}, parts...)
	if filename != "" {
		path = append(path, filename)
	}
	return filepath.ToSlash(filepath.Join(path...))
}
