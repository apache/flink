package org.apache.flink.metrics.datadog.parser;


import java.util.ArrayList;
import java.util.List;

public class NameAndTags {
	private String name;
	private List<String> tags;

	public NameAndTags(String name) {
		this(name, new ArrayList<String>());
	}

	public NameAndTags(String name, List<String> tags) {
		this.name = name;
		this.tags = tags;
	}

	public String getName() {
		return name;
	}

	public List<String> getTags() {
		return tags;
	}
}
