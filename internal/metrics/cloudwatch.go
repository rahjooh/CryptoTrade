package metrics

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	"cryptoflow/logger"
)

//go:embed CWdash.json
var dashboardTemplate string

type cloudWatchState struct {
	client        *cloudwatch.Client
	namespace     string
	dashboardName string
	region        string
}

const cloudWatchMaxMetricBatch = 20

var (
	cwState                   atomic.Pointer[cloudWatchState]
	publishMetricsFunc        = publishMetrics
	cloudWatchPublishInterval = time.Minute
	metricPublishTimesMu      sync.Mutex
	metricPublishTimes        = make(map[string]time.Time)
	timeNow                   = time.Now
)

func init() {
	cwState.Store(&cloudWatchState{
		namespace:     "CryptoFlow",
		dashboardName: "CryptoFlow",
	})
}

// InitCloudWatch initialises the CloudWatch client using the provided region and namespace.
// The dashboard is created using the embedded CWdash.json definition. When the client cannot
// be created the function logs a warning and leaves publishing disabled.
func InitCloudWatch(region, namespace, dashboard string) {
	log := logger.GetLogger().WithComponent("cloudwatch")

	if region == "" {
		region = os.Getenv("AWS_REGION")
	}

	ctx := context.Background()
	opts := []func(*config.LoadOptions) error{}
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		log.WithError(err).Warn("failed to load AWS configuration; CloudWatch metrics disabled")
		return
	}

	current := cwState.Load()
	state := cloudWatchState{}
	if current != nil {
		state = *current
	}

	state.client = cloudwatch.NewFromConfig(cfg)
	if namespace != "" {
		state.namespace = namespace
	}
	if dashboard != "" {
		state.dashboardName = dashboard
	}
	if cfg.Region != "" {
		state.region = cfg.Region
	} else {
		state.region = region
	}

	cwState.Store(&state)
	resetMetricPublishTimes()

	log.WithFields(logger.Fields{
		"region":    state.region,
		"namespace": state.namespace,
	}).Info("initialized CloudWatch client")

	if err := CreateDashboardFromTemplate(ctx); err != nil {
		log.WithError(err).Warn("failed to create CloudWatch dashboard")
	}
}

// EmitMetric logs the metric locally and publishes it to CloudWatch when configured.
func EmitMetric(log *logger.Log, component string, metric string, value interface{}, metricType string, fields logger.Fields) {
	metricEvent, ok := recordMetric(log, component, metric, value, metricType, fields)
	if !ok {
		return
	}

	numericValue, ok := toFloat64(metricEvent.Value)
	if !ok {
		logger.GetLogger().WithComponent("cloudwatch").WithFields(logger.Fields{"metric": metricEvent.Name}).Debug("non-numeric metric value; skipping publish")
		return
	}

	publishMetricDatum(metricEvent, numericValue)
}

// CreateDashboardFromTemplate applies the embedded dashboard definition and updates the
// configured CloudWatch dashboard. Invalid JSON or API failures are surfaced to the caller.
func CreateDashboardFromTemplate(ctx context.Context) error {
	state := cwState.Load()
	if state == nil || state.client == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	body := dashboardTemplate
	if state.namespace != "" {
		body = strings.ReplaceAll(body, "\"CryptoFlow\"", fmt.Sprintf("%q", state.namespace))
	}
	if state.region != "" {
		body = strings.ReplaceAll(body, "\"ap-south-1\"", fmt.Sprintf("%q", state.region))
	}

	if !json.Valid([]byte(body)) {
		return fmt.Errorf("dashboard template is not valid JSON after substitution")
	}

	_, err := state.client.PutDashboard(ctx, &cloudwatch.PutDashboardInput{
		DashboardName: aws.String(state.dashboardName),
		DashboardBody: aws.String(body),
	})
	if err != nil {
		return err
	}

	logger.GetLogger().WithComponent("cloudwatch").Debug("updated CloudWatch dashboard from template")
	return nil
}

func publishMetricDatum(metric Metric, value float64) {
	state := cwState.Load()
	if state == nil || state.client == nil {
		return
	}

	datum := buildMetricDatum(metric, value)
	if !shouldPublishMetric(metric, datum) {
		return
	}

	publishMetricsFunc(context.Background(), state, []cwtypes.MetricDatum{datum})
}

func publishMetrics(ctx context.Context, state *cloudWatchState, data []cwtypes.MetricDatum) {
	if state == nil || state.client == nil {
		return
	}
	if len(data) == 0 {
		logger.GetLogger().WithComponent("cloudwatch").Debug("no metric data to publish")
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	names := make([]string, 0, len(data))
	for start := 0; start < len(data); start += cloudWatchMaxMetricBatch {
		end := start + cloudWatchMaxMetricBatch
		if end > len(data) {
			end = len(data)
		}
		batch := data[start:end]

		if _, err := state.client.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
			Namespace:  aws.String(state.namespace),
			MetricData: batch,
		}); err != nil {
			logger.GetLogger().WithComponent("cloudwatch").WithError(err).Warn("failed to publish CloudWatch metrics")
			return
		}

		for _, datum := range batch {
			if datum.MetricName != nil {
				names = append(names, *datum.MetricName)
			}
		}
	}

	logger.GetLogger().WithComponent("cloudwatch").WithField("metrics", strings.Join(names, ",")).Debug("published metrics to CloudWatch")
}

func buildMetricDatum(metric Metric, value float64) cwtypes.MetricDatum {
	fields := metric.Fields
	unit := cwtypes.StandardUnitCount
	if fields != nil {
		if rawUnit, ok := fields["unit"]; ok {
			if unitStr, ok := rawUnit.(string); ok {
				if parsedUnit, found := metricUnitFromString(unitStr); found {
					unit = parsedUnit
				} else {
					logger.GetLogger().WithComponent("cloudwatch").WithFields(logger.Fields{"metric": metric.Name, "unit": unitStr}).Debug("unsupported metric unit; defaulting to Count")
				}
			}
		}
	}

	dims := []cwtypes.Dimension{{Name: aws.String("component"), Value: aws.String(metric.Component)}}
	for k, v := range fields {
		if k == "metric" || k == "metric_type" || k == "value" || k == "unit" {
			continue
		}
		if s, ok := v.(string); ok && s != "" {
			dims = append(dims, cwtypes.Dimension{Name: aws.String(k), Value: aws.String(s)})
		}
	}

	datum := cwtypes.MetricDatum{
		MetricName: aws.String(metric.Name),
		Dimensions: dims,
		Unit:       unit,
		Value:      aws.Float64(value),
	}
	if !metric.Timestamp.IsZero() {
		ts := metric.Timestamp
		datum.Timestamp = aws.Time(ts)
	}
	return datum
}

func shouldPublishMetric(metric Metric, datum cwtypes.MetricDatum) bool {
	key := metricThrottleKey(metric, datum)
	if key == "" {
		return true
	}

	interval := cloudWatchPublishInterval
	if interval <= 0 {
		interval = time.Minute
	}

	metricPublishTimesMu.Lock()
	defer metricPublishTimesMu.Unlock()

	now := timeNow()
	if last, ok := metricPublishTimes[key]; ok {
		if now.Sub(last) < interval {
			return false
		}
	}

	metricPublishTimes[key] = now
	return true
}

func metricThrottleKey(metric Metric, datum cwtypes.MetricDatum) string {
	if datum.MetricName == nil {
		return ""
	}

	var builder strings.Builder
	builder.Grow(64)
	builder.WriteString(metric.Component)
	builder.WriteString("|")
	builder.WriteString(*datum.MetricName)

	if len(datum.Dimensions) > 0 {
		dims := make([]string, 0, len(datum.Dimensions))
		for _, dim := range datum.Dimensions {
			var name, value string
			if dim.Name != nil {
				name = *dim.Name
			}
			if dim.Value != nil {
				value = *dim.Value
			}
			dims = append(dims, name+"="+value)
		}
		sort.Strings(dims)
		for _, dim := range dims {
			builder.WriteString("|")
			builder.WriteString(dim)
		}
	}

	if datum.Unit != "" {
		builder.WriteString("|unit=")
		builder.WriteString(string(datum.Unit))
	}

	return builder.String()
}

func resetMetricPublishTimes() {
	metricPublishTimesMu.Lock()
	metricPublishTimes = make(map[string]time.Time)
	metricPublishTimesMu.Unlock()
}

func toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

func metricUnitFromString(unit string) (cwtypes.StandardUnit, bool) {
	switch strings.ToLower(unit) {
	case "count":
		return cwtypes.StandardUnitCount, true
	case "percent":
		return cwtypes.StandardUnitPercent, true
	default:
		return cwtypes.StandardUnitCount, false
	}
}
