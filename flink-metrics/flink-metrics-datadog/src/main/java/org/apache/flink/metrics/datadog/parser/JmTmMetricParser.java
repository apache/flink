package org.apache.flink.metrics.datadog.parser;

import com.google.common.collect.Lists;

public class JmTmMetricParser extends AbstractMetricParser implements IMetricParser {
	@Override
	public NameAndTags getNameAndTags(String fullName, String keyword) {
		String tagStr = fullName.substring(0, fullName.indexOf(keyword) - 1);

		return new NameAndTags(getName(fullName, keyword), Lists.newArrayList(tagStr.split("\\.")));
	}
}
