#!/usr/bin/env bash
set -euo pipefail

# create_cloudwatch_dashboard.sh
#
# This script provisions basic CloudWatch resources for CryptoFlow:
#   * log group for application logs
#   * dashboard visualising channel size, error count and host metrics
#   * sample alarm on error count metric
#
# Prerequisites:
#   * AWS CLI configured with credentials and region
#   * envsubst available (usually provided by gettext package)
#
# Usage:
#   AWS_REGION=us-east-1 INSTANCE_ID=i-1234 ALARM_TOPIC_ARN=arn:aws:sns:... ./scripts/create_cloudwatch_dashboard.sh

LOG_GROUP_NAME="${LOG_GROUP_NAME:-/cryptoflow/app}"
DASHBOARD_NAME="${DASHBOARD_NAME:-CryptoFlow}"
AWS_REGION="${AWS_REGION:-us-east-1}"
INSTANCE_ID="${INSTANCE_ID:-i-xxxxxxxx}"
ALARM_TOPIC_ARN="${ALARM_TOPIC_ARN:-}"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TEMPLATE_DIR="$SCRIPT_DIR/../cloudwatch"

create_log_group() {
  echo "Creating log group $LOG_GROUP_NAME"
  aws logs create-log-group --log-group-name "$LOG_GROUP_NAME" 2>/dev/null || true
}

create_dashboard() {
  echo "Creating dashboard $DASHBOARD_NAME in $AWS_REGION"
  export AWS_REGION INSTANCE_ID
  envsubst < "$TEMPLATE_DIR/dashboard.json" > /tmp/dashboard-rendered.json
  aws cloudwatch put-dashboard \
    --dashboard-name "$DASHBOARD_NAME" \
    --dashboard-body file:///tmp/dashboard-rendered.json
}

create_alarm() {
  echo "Creating error count alarm"
  local alarm_args=(
    --alarm-name "${DASHBOARD_NAME}-HighErrors"
    --metric-name Count
    --namespace CryptoFlow/Errors
    --statistic Sum
    --period 60
    --threshold 1
    --comparison-operator GreaterThanOrEqualToThreshold
    --evaluation-periods 1
    --region "$AWS_REGION"
  )
  if [[ -n "$ALARM_TOPIC_ARN" ]]; then
    alarm_args+=(--alarm-actions "$ALARM_TOPIC_ARN")
  fi
  aws cloudwatch put-metric-alarm "${alarm_args[@]}"
}

main() {
  create_log_group
  create_dashboard
  create_alarm
  echo "CloudWatch resources created"
}

main "$@"
