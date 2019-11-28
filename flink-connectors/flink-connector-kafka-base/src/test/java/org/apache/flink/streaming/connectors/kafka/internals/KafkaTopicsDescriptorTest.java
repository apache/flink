/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Tests for the {@link KafkaTopicsDescriptor}.
 */
@RunWith(Parameterized.class)
public class KafkaTopicsDescriptorTest {

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
			{ "topic1", null, Arrays.asList("topic1", "topic2", "topic3"), true },
			{ "topic1", null, Arrays.asList("topic2", "topic3"), false },
			{ "topic1", Pattern.compile("topic[0-9]"), null, true },
			{ "topicx", Pattern.compile("topic[0-9]"), null, false }
		});
	}

	private String topic;
	private Pattern topicPattern;
	private List<String> fixedTopics;
	boolean expected;

	public KafkaTopicsDescriptorTest(String topic, Pattern topicPattern, List<String> fixedTopics, boolean expected) {
		this.topic = topic;
		this.topicPattern = topicPattern;
		this.fixedTopics = fixedTopics;
		this.expected = expected;
	}

	@Test
	public void testIsMatchingTopic() {
		KafkaTopicsDescriptor topicsDescriptor = new KafkaTopicsDescriptor(fixedTopics, topicPattern);

		Assert.assertEquals(expected, topicsDescriptor.isMatchingTopic(topic));
	}
}
