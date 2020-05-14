package org.apache.flink.metrics.influxdb;

import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

import java.util.Properties;

public class InfluxdbReporterFactory implements MetricReporterFactory {

	@Override
	public MetricReporter createMetricReporter(Properties properties) {
		return new InfluxdbReporter();
	}
}
