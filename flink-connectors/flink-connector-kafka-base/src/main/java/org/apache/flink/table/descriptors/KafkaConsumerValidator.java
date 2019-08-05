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

import org.apache.flink.table.api.ValidationException;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Validator for Kafka producer setting.
 */
public class KafkaConsumerValidator extends KafkaValidator {

	/**
	 * For Kafka consumer:
	 * 1. Check only one of the topic, topics or subscriptionPattern is specified.
	 * 2. Check the regex pattern is valid if the subscriptionPattern is specified.
	 */
	@Override
	public void validateTopicSetting(DescriptorProperties properties) {
		int topicSettingCount = keyCount(
			properties,
			CONNECTOR_TOPIC,
			CONNECTOR_TOPICS,
			CONNECTOR_SUBSCRIPTION_PATTERN
		);
		if (topicSettingCount != 1) {
			throw new ValidationException("specify topic, topics or subscriptionPattern");
		}
		// Validate topic or topics or subscriptionPattern
		if (properties.containsKey(CONNECTOR_TOPIC)) {
			properties.validateString(CONNECTOR_TOPIC, false, 1, Integer.MAX_VALUE);
			return;
		}
		if (properties.containsKey(CONNECTOR_TOPICS)) {
			properties.validateString(CONNECTOR_TOPICS, false, 1, Integer.MAX_VALUE);
			return;
		}

		// First Valid subscriptionPattern string
		properties.validateString(CONNECTOR_SUBSCRIPTION_PATTERN, false, 1, Integer.MAX_VALUE);
		// Then valid regex pattern
		try {
			Pattern.compile(properties.getString(CONNECTOR_SUBSCRIPTION_PATTERN));
		} catch (PatternSyntaxException e) {
			throw new ValidationException("subscriptionPattern is not a valid java regex pattern");
		}

	}

	private int keyCount(DescriptorProperties properties, String... keys) {
		int count = 0;
		for (String key : keys) {
			if (properties.containsKey(key)) {
				count++;
			}
		}
		return count;
	}
}
