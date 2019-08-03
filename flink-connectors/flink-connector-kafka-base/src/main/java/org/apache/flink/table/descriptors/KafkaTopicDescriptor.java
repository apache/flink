package org.apache.flink.table.descriptors;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Kafka Topic Descriptor.
 */
public class KafkaTopicDescriptor {

	/**
	 * SINGLE: Kafka topic is specified.
	 * LIST: Kafka topics is specified.
	 * PATTERN: Kafka subscription pattern is specified.
	 * UNKNOWN: None of above is specified.
	 */
	public enum TYPE {SINGLE, LIST, PATTERN, UNKNOWN}

	private String topic;
	private List<String> topics;
	private Pattern subscriptionPattern;
	private TYPE type = TYPE.UNKNOWN;

	public KafkaTopicDescriptor() {}

	public KafkaTopicDescriptor(String topic, List<String> topics, Pattern subscriptionPattern) {
		if (topic != null && !topic.equals("")) {
			this.setTopic(topic);
		}
		if (topics != null && topics.size() != 0) {
			this.setTopics(topics);
		}
		if (subscriptionPattern != null) {
			this.setSubscriptionPattern(subscriptionPattern);
		}
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
		this.type = TYPE.SINGLE;
	}

	public List<String> getTopics() {
		return topics;
	}

	public void setTopics(List<String> topics) {
		this.topics = topics;
		this.type = TYPE.LIST;
	}

	public Pattern getSubscriptionPattern() {
		return subscriptionPattern;
	}

	public void setSubscriptionPattern(Pattern subscriptionPattern) {
		this.subscriptionPattern = subscriptionPattern;
		this.type = TYPE.PATTERN;
	}

	public TYPE getType() {
		return type;
	}

	public void setType(TYPE type) {
		this.type = type;
	}

}
