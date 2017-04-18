package org.apache.flink.metrics.datadog.metric;

import java.util.List;

/**
 * Mapping of gauge between Flink and Datadog
 * */
public class DGauge<T extends Number> extends DMetric {
	public DGauge(String metric, T number,  List<String> tags) {
		super(metric, number, MetricType.gauge, tags);
	}
}
