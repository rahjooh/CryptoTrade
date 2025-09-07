package logger

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

var cwClient *cloudwatch.Client
var cwNamespace string = "CryptoFlow"

// InitCloudWatch configures the CloudWatch client used for publishing metrics.
// If region is empty, the default region resolution is used. Namespace defaults to "CryptoFlow".
func InitCloudWatch(region, namespace string) {
	ctx := context.Background()
	opts := []func(*config.LoadOptions) error{}
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return
	}
	cwClient = cloudwatch.NewFromConfig(cfg)
	if namespace != "" {
		cwNamespace = namespace
	}
}

// publishMetrics sends the provided metric data to CloudWatch if a client is configured.
func publishMetrics(ctx context.Context, data []cwtypes.MetricDatum) {
	if cwClient == nil || len(data) == 0 {
		return
	}
	_, _ = cwClient.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace:  aws.String(cwNamespace),
		MetricData: data,
	})
}
