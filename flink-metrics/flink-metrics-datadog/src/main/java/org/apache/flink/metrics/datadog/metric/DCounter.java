package org.apache.flink.metrics.datadog.metric;

import java.util.List;

/**
 * Mapping of counter between Flink and Datadog
 * */
public class DCounter extends DMetric {
	public DCounter(String metric, Number number, List tags) {
		super(metric, number, MetricType.counter, tags);
	}
}
