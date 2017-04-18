package org.apache.flink.metrics.datadog.metric;

import org.apache.flink.metrics.datadog.utils.TimestampUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract metric of Datadog
 * */
public abstract class DMetric<T extends Number> {
	private final String metric;
	private final List<List<Number>> points;
	private final MetricType type;
	private final List<String> tags;

	public DMetric(String metric, T number, MetricType metricType, List<String> tags) {
		this.metric = metric;
		this.points = new ArrayList<>();
		List<Number> point = new ArrayList<>();
		point.add(TimestampUtils.getUnixEpochTimestamp());
		point.add(number);
		this.points.add(point);
		this.type = metricType;
		this.tags = tags;
	}

	public MetricType getType() {
		return type;
	}

	public String getMetric() {
		return metric;
	}

	public List<String> getTags() {
		return tags;
	}

	public List<List<Number>> getPoints() {
		return points;
	}
}
