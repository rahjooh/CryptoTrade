package logger

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

var cwClient *cloudwatch.Client
var cwNamespace string = "CryptoFlow"
var cwDashboard string = "Data"

// If dashboard is provided, metrics are sent to that dashboard; otherwise a
// default name derived from the namespace is used.
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

// publishMetrics sends the provided metric data to CloudWatch if a client is configured.
func publishMetrics(ctx context.Context, data []cwtypes.MetricDatum) {
	if cwClient == nil || len(data) == 0 {
		return
	}
	_, err := cwClient.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace:  aws.String(cwNamespace),
		MetricData: data,
	})
	if err != nil {
		GetLogger().WithComponent("cloudwatch").WithError(err).Warn("failed to publish CloudWatch metrics")
	}
}

// CreateDefaultDashboard creates or updates a basic CloudWatch dashboard with
// common system metrics. If the CloudWatch client is not configured, the
// function exits without performing any action.
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

	_, err := cwClient.PutDashboard(ctx, &cloudwatch.PutDashboardInput{
		DashboardName: aws.String(cwDashboard),
		DashboardBody: aws.String(body),
	})
	if err != nil {
		GetLogger().WithComponent("cloudwatch").WithError(err).Warn("failed to create CloudWatch dashboard")
	}
}
