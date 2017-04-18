package org.apache.flink.metrics.datadog.parser;

public abstract class AbstractMetricParser implements IMetricParser {
	public String getName(String fullName, String keyword) {
		return fullName.substring(fullName.indexOf(keyword));
	}
}
