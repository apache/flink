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

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A Kafka Topics Descriptor describes how the consumer subscribes to Kafka topics -
 * either a fixed list of topics, or a topic pattern.
 */
@Internal
public class KafkaTopicsDescriptor implements Serializable {

	private static final long serialVersionUID = -3807227764764900975L;

	private final List<String> fixedTopics;
	private final Pattern topicPattern;

	public KafkaTopicsDescriptor(@Nullable List<String> fixedTopics, @Nullable Pattern topicPattern) {
		checkArgument((fixedTopics != null && topicPattern == null) || (fixedTopics == null && topicPattern != null),
			"Exactly one of either fixedTopics or topicPattern must be specified.");

		if (fixedTopics != null) {
			checkArgument(!fixedTopics.isEmpty(), "If subscribing to a fixed topics list, the supplied list cannot be empty.");
		}

		this.fixedTopics = fixedTopics;
		this.topicPattern = topicPattern;
	}

	public boolean isFixedTopics() {
		return fixedTopics != null;
	}

	public boolean isTopicPattern() {
		return topicPattern != null;
	}

	public List<String> getFixedTopics() {
		return fixedTopics;
	}

	public Pattern getTopicPattern() {
		return topicPattern;
	}

	@Override
	public String toString() {
		return (fixedTopics == null)
			? "Topic Regex Pattern (" + topicPattern.pattern() + ")"
			: "Fixed Topics (" + fixedTopics + ")";
	}
}
