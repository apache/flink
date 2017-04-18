package org.apache.flink.metrics.datadog.parser;

import com.google.common.collect.Lists;
import org.apache.flink.metrics.MetricConfig;

import java.util.List;

/**
 * Parse Flink's full metric names in necessary
 * */
public class MetricParser {
	static final String JOB_MANAGER = "jobmanager";
	static final String TASK_MANAGER = "taskmanager";
	static final String TASK = "task";
	static final String OPERATOR = "operator";

	public static final String TAGS_ENABLED = "tags.enabled";
	public static final String TAGS = "globaltags";

	private final boolean tagsEnabled;

	private List<String> globalTags;
	private IMetricParser jmTmMetricParser;
	private IMetricParser taskOperatorMetricParser;


	public MetricParser(MetricConfig config) {
		tagsEnabled = config.getBoolean(TAGS_ENABLED, false);

		if(tagsEnabled) {
			globalTags = getGlobalTags(config.getString(TAGS, null));
			jmTmMetricParser = new JmTmMetricParser();
			taskOperatorMetricParser = new TaskAndOperatorMetricParser();
		}
	}

	public NameAndTags getNameAndTags(String fullName) {
		if(tagsEnabled) {
			NameAndTags nat = getNameAndTagsWhenEnabled(fullName);
			nat.getTags().addAll(globalTags);
			return nat;
		} else {
			return new NameAndTags(fullName);
		}
	}

	private NameAndTags getNameAndTagsWhenEnabled(String fullName) {
		if(fullName.contains(JOB_MANAGER)) {
			return jmTmMetricParser.getNameAndTags(fullName, JOB_MANAGER);
		} else if(fullName.contains(TASK_MANAGER)) {
			return jmTmMetricParser.getNameAndTags(fullName, TASK_MANAGER);
		} else if(fullName.contains(TASK)) {
			return taskOperatorMetricParser.getNameAndTags(fullName, TASK);
		} else if(fullName.contains(OPERATOR)) {
			return taskOperatorMetricParser.getNameAndTags(fullName, OPERATOR);
		} else {
			throw new IllegalArgumentException("Cannot find metric parser for " + fullName);
		}
	}

	private List<String> getGlobalTags(String str) {
		if(str != null) {
			return Lists.newArrayList(str.split(","));
		} else {
			return Lists.newArrayList();
		}
	}
}
