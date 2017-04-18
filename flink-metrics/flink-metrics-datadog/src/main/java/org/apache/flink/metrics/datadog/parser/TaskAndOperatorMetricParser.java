package org.apache.flink.metrics.datadog.parser;


import com.google.common.collect.Lists;

import java.util.List;

/**
 * metrics.scope.task:     <host>.<tm_id>.<job_name>.<subtask_index>.<task_name>.task
 * metrics.scope.operator: <host>.<tm_id>.<job_name>.<subtask_index>.<operator_name>.operator
 * */
public class TaskAndOperatorMetricParser extends AbstractMetricParser implements IMetricParser {
	private static final int NUMBER_OF_TAGS = 5;

	@Override
	public NameAndTags getNameAndTags(String fullName, String keyword) {
		String tagStr = fullName.substring(0, fullName.indexOf(keyword) - 1);

		List<String> tags = Lists.newArrayList();
		int start = 0;
		int numberOfDots = 0;
		for(int i = 0; i < tagStr.length(); i ++) {
			if(tagStr.charAt(i) == '.') {
				numberOfDots ++;
				tags.add(tagStr.substring(start, i));
				start = i + 1;

				if(numberOfDots == NUMBER_OF_TAGS - 1) {
					tags.add(tagStr.substring(i + 1));
					break;
				}
			}
		}

		if(tags.size() != NUMBER_OF_TAGS) {
			throw new IllegalStateException(
				String.format("Failed getting %d tags from %s. Only get %d tags.",
					NUMBER_OF_TAGS, fullName, tags.size()));
		}

		return new NameAndTags(getName(fullName, keyword), tags);
	}
}
