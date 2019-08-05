/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.descriptors;

import java.util.List;
import java.util.Objects;
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

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		KafkaTopicDescriptor that = (KafkaTopicDescriptor) o;
		return Objects.equals(topic, that.topic) &&
			Objects.equals(topics, that.topics) &&
			Objects.equals(subscriptionPattern, that.subscriptionPattern) &&
			type == that.type;
	}

	@Override
	public int hashCode() {
		return Objects.hash(topic, topics, subscriptionPattern, type);
	}
}
