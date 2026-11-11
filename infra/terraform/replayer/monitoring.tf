# Platform alarms (#36), to the same email as the ops alerts. The replayer writes the slot's metrics every tick
# (services/replayer/handler.py, embedded metric format), so they exist only while a window runs.

# WAL the slot holds back: Neon Free has ~1 GB, so a slot nobody reads fills it. 256 MB is a quarter of it; a demo
# window writes a few MB per simulated hour (sizing.md), so crossing it means Debezium has stopped reading.
resource "aws_cloudwatch_metric_alarm" "slot_wal" {
  alarm_name          = "ridepulse-slot-retained-wal"
  alarm_description   = "Debezium's replication slot holds over 256 MB of WAL: is Debezium reading? docs/index.html#rb-debezium"
  namespace           = "RidePulse"
  metric_name         = "SlotRetainedWalBytes"
  dimensions          = { Slot = "ridepulse_debezium" }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 268435456
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching" # no window, no ticks, no metric
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

# The slot inactive for 5 minutes while the replayer ticks: the ticks wait (CdcNotReady), the window does nothing.
resource "aws_cloudwatch_metric_alarm" "slot_inactive" {
  alarm_name          = "ridepulse-slot-inactive"
  alarm_description   = "Debezium's slot inactive for 5 min during a window: ticks are waiting. docs/index.html#rb-debezium"
  namespace           = "RidePulse"
  metric_name         = "SlotActive"
  dimensions          = { Slot = "ridepulse_debezium" }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}
