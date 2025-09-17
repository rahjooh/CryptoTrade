package logger

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

var cwClient *cloudwatch.Client
var cwNamespace = "CryptoFlow"
var cwDashboard = "CryptoFlow"

// InitCloudWatch initialises the CloudWatch client using the provided region and
// namespace. If region is empty it falls back to the AWS_REGION environment
// variable. When the client cannot be created the function logs a warning and
// metrics publishing remains disabled.
func InitCloudWatch(region, namespace, dashboard string) {
	log := GetLogger().WithComponent("cloudwatch")

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

	cwClient = cloudwatch.NewFromConfig(cfg)

	if namespace != "" {
		cwNamespace = namespace
	}

	if dashboard != "" {
		cwDashboard = dashboard
	}

	log.WithFields(Fields{"region": region, "namespace": cwNamespace}).Info("initialized CloudWatch client")

	CreateDefaultDashboard(ctx)
}

// publishMetrics sends the provided metric data to CloudWatch when the client
// has been initialised. Unsupported values simply log at debug level.
func publishMetrics(ctx context.Context, data []cwtypes.MetricDatum) {
	log := GetLogger().WithComponent("cloudwatch")
	if cwClient == nil {
		log.Debug("CloudWatch client not initialized; skipping metric publish")
		return
	}

	if len(data) == 0 {
		log.Debug("no metric data to publish")
		return
	}

	if _, err := cwClient.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace:  aws.String(cwNamespace),
		MetricData: data,
	}); err != nil {
		log.WithError(err).Warn("failed to publish CloudWatch metrics")
		return
	}

	names := make([]string, 0, len(data))
	for _, datum := range data {
		if datum.MetricName != nil {
			names = append(names, *datum.MetricName)
		}
	}

	log.WithField("metrics", strings.Join(names, ",")).Debug("published metrics to CloudWatch")
}

// CreateDefaultDashboard ensures a basic dashboard exists when the CloudWatch
// client has been configured. Failures are logged but do not stop execution.
func CreateDefaultDashboard(ctx context.Context) {
	if cwClient == nil {
		return
	}

	body := fmt.Sprintf(`{
"widgets": [{
"type": "metric",
"width": 24,
"height": 6,
"properties": {
"metrics": [
    ["%[1]s","Hadi-CPUPercent"],
    ["%[1]s","Hadi-MemoryMB"],
    ["%[1]s","Hadi-DiskMB"]
],
"period": 60,
"stat": "Average",
"title": "CryptoFlow System Metrics"
}
}]
}`, cwNamespace)

	if _, err := cwClient.PutDashboard(ctx, &cloudwatch.PutDashboardInput{
		DashboardName: aws.String(cwDashboard),
		DashboardBody: aws.String(body),
	}); err != nil {
		GetLogger().WithComponent("cloudwatch").WithError(err).Warn("failed to create CloudWatch dashboard")
	}
}
