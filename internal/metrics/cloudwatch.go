package metrics

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

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

var cwState atomic.Pointer[cloudWatchState]

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

	ctx := context.Background()
	publishMetricDatum(ctx, metricEvent.Component, metricEvent.Name, numericValue, metricEvent.Fields)
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

func publishMetricDatum(ctx context.Context, component, metric string, value float64, fields logger.Fields) {
	state := cwState.Load()
	if state == nil || state.client == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	unit := cwtypes.StandardUnitCount
	if rawUnit, ok := fields["unit"]; ok {
		if unitStr, ok := rawUnit.(string); ok {
			if parsedUnit, found := metricUnitFromString(unitStr); found {
				unit = parsedUnit
			} else {
				logger.GetLogger().WithComponent("cloudwatch").WithFields(logger.Fields{"metric": metric, "unit": unitStr}).Debug("unsupported metric unit; defaulting to Count")
			}
		}
	}

	dims := []cwtypes.Dimension{{Name: aws.String("component"), Value: aws.String(component)}}
	for k, v := range fields {
		if k == "metric" || k == "metric_type" || k == "value" || k == "unit" {
			continue
		}
		if s, ok := v.(string); ok && s != "" {
			dims = append(dims, cwtypes.Dimension{Name: aws.String(k), Value: aws.String(s)})
		}
	}

	data := []cwtypes.MetricDatum{{
		MetricName: aws.String(metric),
		Dimensions: dims,
		Unit:       unit,
		Value:      aws.Float64(value),
	}}
	publishMetrics(ctx, state, data)
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

	if _, err := state.client.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace:  aws.String(state.namespace),
		MetricData: data,
	}); err != nil {
		logger.GetLogger().WithComponent("cloudwatch").WithError(err).Warn("failed to publish CloudWatch metrics")
		return
	}

	names := make([]string, 0, len(data))
	for _, datum := range data {
		if datum.MetricName != nil {
			names = append(names, *datum.MetricName)
		}
	}

	logger.GetLogger().WithComponent("cloudwatch").WithField("metrics", strings.Join(names, ",")).Debug("published metrics to CloudWatch")
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
