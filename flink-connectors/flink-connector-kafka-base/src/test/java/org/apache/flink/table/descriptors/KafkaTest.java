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

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Tests for the {@link Kafka} descriptor.
 */
public class KafkaTest extends DescriptorTestBase {

	@Override
	public List<Descriptor> descriptors() {
		final Descriptor earliestDesc =
			new Kafka()
				.version("0.8")
				.startFromEarliest()
				.topic("WhateverTopic");

		final Descriptor specificOffsetsDesc =
			new Kafka()
				.version("0.11")
				.topic("MyTable")
				.startFromSpecificOffset(0, 42L)
				.startFromSpecificOffset(1, 300L)
				.property("zookeeper.stuff", "12")
				.property("kafka.stuff", "42");

		final Map<Integer, Long> offsets = new HashMap<>();
		offsets.put(0, 42L);
		offsets.put(1, 300L);

		final Properties properties = new Properties();
		properties.put("zookeeper.stuff", "12");
		properties.put("kafka.stuff", "42");

		final Descriptor specificOffsetsMapDesc =
			new Kafka()
				.version("0.11")
				.topic("MyTable")
				.startFromSpecificOffsets(offsets)
				.properties(properties)
				.sinkPartitionerCustom(FlinkFixedPartitioner.class);

		return Arrays.asList(earliestDesc, specificOffsetsDesc, specificOffsetsMapDesc);
	}

	@Override
	public List<Map<String, String>> properties() {
		final Map<String, String> props1 = new HashMap<>();
		props1.put("connector.property-version", "1");
		props1.put("connector.type", "kafka");
		props1.put("connector.version", "0.8");
		props1.put("connector.topic", "WhateverTopic");
		props1.put("connector.startup-mode", "earliest-offset");

		final Map<String, String> props2 = new HashMap<>();
		props2.put("connector.property-version", "1");
		props2.put("connector.type", "kafka");
		props2.put("connector.version", "0.11");
		props2.put("connector.topic", "MyTable");
		props2.put("connector.startup-mode", "specific-offsets");
		props2.put("connector.specific-offsets", "partition:0,offset:42;partition:1,offset:300");
		props2.put("connector.properties.zookeeper.stuff", "12");
		props2.put("connector.properties.kafka.stuff", "42");

		final Map<String, String> props3 = new HashMap<>();
		props3.put("connector.property-version", "1");
		props3.put("connector.type", "kafka");
		props3.put("connector.version", "0.11");
		props3.put("connector.topic", "MyTable");
		props3.put("connector.startup-mode", "specific-offsets");
		props3.put("connector.specific-offsets", "partition:0,offset:42;partition:1,offset:300");
		props3.put("connector.properties.zookeeper.stuff", "12");
		props3.put("connector.properties.kafka.stuff", "42");
		props3.put("connector.sink-partitioner", "custom");
		props3.put("connector.sink-partitioner-class", FlinkFixedPartitioner.class.getName());

		return Arrays.asList(props1, props2, props3);
	}

	@Override
	public DescriptorValidator validator() {
		return new KafkaValidator();
	}
}
