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

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES_KEY;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES_VALUE;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SPECIFIC_OFFSETS;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SPECIFIC_OFFSETS_OFFSET;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SPECIFIC_OFFSETS_PARTITION;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_STARTUP_MODE;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_TOPIC;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_TYPE_VALUE_KAFKA;

/**
 * Connector descriptor for the Apache Kafka message queue.
 */
public class Kafka extends ConnectorDescriptor {

	private String version;
	private String topic;
	private StartupMode startupMode;
	private Map<Integer, Long> specificOffsets;
	private Map<String, String> kafkaProperties;

	/**
	 * Connector descriptor for the Apache Kafka message queue.
	 */
	public Kafka() {
		super(CONNECTOR_TYPE_VALUE_KAFKA, 1, true);
	}

	/**
	 * Sets the Kafka version to be used.
	 *
	 * @param version Kafka version. E.g., "0.8", "0.11", etc.
	 */
	public Kafka version(String version) {
		Preconditions.checkNotNull(version);
		this.version = version;
		return this;
	}

	/**
	 * Sets the topic from which the table is read.
	 *
	 * @param topic The topic from which the table is read.
	 */
	public Kafka topic(String topic) {
		Preconditions.checkNotNull(topic);
		this.topic = topic;
		return this;
	}

	/**
	 * Sets the configuration properties for the Kafka consumer. Resets previously set properties.
	 *
	 * @param properties The configuration properties for the Kafka consumer.
	 */
	public Kafka properties(Properties properties) {
		Preconditions.checkNotNull(properties);
		if (this.kafkaProperties == null) {
			this.kafkaProperties = new HashMap<>();
		}
		this.kafkaProperties.clear();
		properties.forEach((k, v) -> this.kafkaProperties.put((String) k, (String) v));
		return this;
	}

	/**
	 * Adds a configuration properties for the Kafka consumer.
	 *
	 * @param key property key for the Kafka consumer
	 * @param value property value for the Kafka consumer
	 */
	public Kafka property(String key, String value) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(value);
		if (this.kafkaProperties == null) {
			this.kafkaProperties = new HashMap<>();
		}
		kafkaProperties.put(key, value);
		return this;
	}

	/**
	 * Configures to start reading from the earliest offset for all partitions.
	 *
	 * @see FlinkKafkaConsumerBase#setStartFromEarliest()
	 */
	public Kafka startFromEarliest() {
		this.startupMode = StartupMode.EARLIEST;
		this.specificOffsets = null;
		return this;
	}

	/**
	 * Configures to start reading from the latest offset for all partitions.
	 *
	 * @see FlinkKafkaConsumerBase#setStartFromLatest()
	 */
	public Kafka startFromLatest() {
		this.startupMode = StartupMode.LATEST;
		this.specificOffsets = null;
		return this;
	}

	/**
	 * Configures to start reading from any committed group offsets found in Zookeeper / Kafka brokers.
	 *
	 * @see FlinkKafkaConsumerBase#setStartFromGroupOffsets()
	 */
	public Kafka startFromGroupOffsets() {
		this.startupMode = StartupMode.GROUP_OFFSETS;
		this.specificOffsets = null;
		return this;
	}

	/**
	 * Configures to start reading partitions from specific offsets, set independently for each partition.
	 * Resets previously set offsets.
	 *
	 * @param specificOffsets the specified offsets for partitions
	 * @see FlinkKafkaConsumerBase#setStartFromSpecificOffsets(Map)
	 */
	public Kafka startFromSpecificOffsets(Map<Integer, Long> specificOffsets) {
		this.startupMode = StartupMode.SPECIFIC_OFFSETS;
		this.specificOffsets = Preconditions.checkNotNull(specificOffsets);
		return this;
	}

	/**
	 * Configures to start reading partitions from specific offsets and specifies the given offset for
	 * the given partition.
	 *
	 * @param partition partition index
	 * @param specificOffset partition offset to start reading from
	 * @see FlinkKafkaConsumerBase#setStartFromSpecificOffsets(Map)
	 */
	public Kafka startFromSpecificOffset(int partition, long specificOffset) {
		this.startupMode = StartupMode.SPECIFIC_OFFSETS;
		if (this.specificOffsets == null) {
			this.specificOffsets = new HashMap<>();
		}
		this.specificOffsets.put(partition, specificOffset);
		return this;
	}

	/**
	 * Internal method for connector properties conversion.
	 */
	@Override
	public void addConnectorProperties(DescriptorProperties properties) {
		if (version != null) {
			properties.putString(CONNECTOR_VERSION(), version);
		}

		if (topic != null) {
			properties.putString(CONNECTOR_TOPIC, topic);
		}

		if (startupMode != null) {
			properties.putString(CONNECTOR_STARTUP_MODE, KafkaValidator.normalizeStartupMode(startupMode));
		}

		if (specificOffsets != null) {
			final List<List<String>> values = new ArrayList<>();
			for (Map.Entry<Integer, Long> specificOffset : specificOffsets.entrySet()) {
				values.add(Arrays.asList(specificOffset.getKey().toString(), specificOffset.getValue().toString()));
			}
			properties.putIndexedFixedProperties(
				CONNECTOR_SPECIFIC_OFFSETS,
				Arrays.asList(CONNECTOR_SPECIFIC_OFFSETS_PARTITION, CONNECTOR_SPECIFIC_OFFSETS_OFFSET),
				values);
		}

		if (kafkaProperties != null) {
			properties.putIndexedFixedProperties(
				CONNECTOR_PROPERTIES,
				Arrays.asList(CONNECTOR_PROPERTIES_KEY, CONNECTOR_PROPERTIES_VALUE),
				this.kafkaProperties.entrySet().stream()
					.map(e -> Arrays.asList(e.getKey(), e.getValue()))
					.collect(Collectors.toList())
				);
		}
	}
}
