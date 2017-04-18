package org.apache.flink.metrics.datadog.parser;

public interface IMetricParser {
	NameAndTags getNameAndTags(String fullName, String keyword);

	String getName(String fullName, String keyword);
}
