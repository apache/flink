package org.apache.flink.metrics.datadog;


import org.apache.flink.metrics.Meter;

import java.util.List;

/**
 * Mapping of meter between Flink and Datadog
 *
 * Only consider rate of the meter, due to Datadog HTTP API's limited support of meter
 * */
public class DMeter extends DMetric {
	private final Meter meter;

	public DMeter(Meter m, String metricName, List<String> tags) {
		super(MetricType.gauge, metricName, tags);
		meter = m;
	}

	@Override
	Number getMetricValue() {
		return meter.getRate();
	}
}
