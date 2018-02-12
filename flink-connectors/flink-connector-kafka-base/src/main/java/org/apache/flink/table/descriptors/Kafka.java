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

import org.apache.flink.streaming.connectors.kafka.config.StartupMode;

import static org.apache.flink.table.descriptors.KafkaValidator.BOOTSTRAP_SERVERS;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_TYPE_VALUE;
import static org.apache.flink.table.descriptors.KafkaValidator.GROUP_ID;
import static org.apache.flink.table.descriptors.KafkaValidator.JSON_FIELD;
import static org.apache.flink.table.descriptors.KafkaValidator.KAFKA_VERSION;
import static org.apache.flink.table.descriptors.KafkaValidator.OFFSET;
import static org.apache.flink.table.descriptors.KafkaValidator.PARTITION;
import static org.apache.flink.table.descriptors.KafkaValidator.SPECIFIC_OFFSETS;
import static org.apache.flink.table.descriptors.KafkaValidator.TABLE_FIELD;
import static org.apache.flink.table.descriptors.KafkaValidator.TABLE_JSON_MAPPING;
import static org.apache.flink.table.descriptors.KafkaValidator.TOPIC;
import static org.apache.flink.table.descriptors.KafkaValidator.ZOOKEEPER_CONNECT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * Connector descriptor for the kafka message queue.
 */
public class Kafka extends ConnectorDescriptor {

	private Optional<String> version = Optional.empty();
	private Optional<String> bootstrapServers = Optional.empty();
	private Optional<String> groupId = Optional.empty();
	private Optional<String> topic = Optional.empty();
	private Optional<String> zookeeperConnect = Optional.empty();
	private Optional<Map<String, String>> tableJsonMapping = Optional.empty();

	private Optional<StartupMode> startupMode = Optional.empty();
	private Optional<Map<Integer, Long>> specificOffsets = Optional.empty();

	public Kafka() {
		super(CONNECTOR_TYPE_VALUE, 1);
	}

	/**
	 * Sets the kafka version.
	 *
	 * @param version
	 * Could be {@link KafkaValidator#KAFKA_VERSION_VALUE_011},
	 * {@link KafkaValidator#KAFKA_VERSION_VALUE_010},
	 * {@link KafkaValidator#KAFKA_VERSION_VALUE_09},
	 * or {@link KafkaValidator#KAFKA_VERSION_VALUE_08}.
	 */
	public Kafka version(String version) {
		this.version = Optional.of(version);
		return this;
	}

	/**
	 * Sets the bootstrap servers for kafka.
	 */
	public Kafka bootstrapServers(String bootstrapServers) {
		this.bootstrapServers = Optional.of(bootstrapServers);
		return this;
	}

	/**
	 * Sets the consumer group id.
	 */
	public Kafka groupId(String groupId) {
		this.groupId = Optional.of(groupId);
		return this;
	}

	/**
	 * Sets the topic to consume.
	 */
	public Kafka topic(String topic) {
		this.topic = Optional.of(topic);
		return this;
	}

	/**
	 * Sets the startup mode.
	 */
	public Kafka startupMode(StartupMode startupMode) {
		this.startupMode = Optional.of(startupMode);
		return this;
	}

	/**
	 * Sets the zookeeper hosts. Only required by kafka 0.8.
	 */
	public Kafka zookeeperConnect(String zookeeperConnect) {
		this.zookeeperConnect = Optional.of(zookeeperConnect);
		return this;
	}

	/**
	 * Sets the consume offsets for the topic set with {@link Kafka#topic(String)}.
	 * Only works in {@link StartupMode#SPECIFIC_OFFSETS} mode.
	 */
	public Kafka specificOffsets(Map<Integer, Long> specificOffsets) {
		this.specificOffsets = Optional.of(specificOffsets);
		return this;
	}

	/**
	 * Sets the mapping from logical table schema to json schema.
	 */
	public Kafka tableJsonMapping(Map<String, String> jsonTableMapping) {
		this.tableJsonMapping = Optional.of(jsonTableMapping);
		return this;
	}

	@Override
	public void addConnectorProperties(DescriptorProperties properties) {
		if (version.isPresent()) {
			properties.putString(KAFKA_VERSION, version.get());
		}
		if (bootstrapServers.isPresent()) {
			properties.putString(BOOTSTRAP_SERVERS, bootstrapServers.get());
		}
		if (groupId.isPresent()) {
			properties.putString(GROUP_ID, groupId.get());
		}
		if (topic.isPresent()) {
			properties.putString(TOPIC, topic.get());
		}
		if (zookeeperConnect.isPresent()) {
			properties.putString(ZOOKEEPER_CONNECT, zookeeperConnect.get());
		}
		if (startupMode.isPresent()) {
			Map<String, String> map = KafkaValidator.normalizeStartupMode(startupMode.get());
			for (Map.Entry<String, String> entry : map.entrySet()) {
				properties.putString(entry.getKey(), entry.getValue());
			}
		}
		if (specificOffsets.isPresent()) {
			List<String> propertyKeys = new ArrayList<>();
			propertyKeys.add(PARTITION);
			propertyKeys.add(OFFSET);

			List<Seq<String>> propertyValues = new ArrayList<>(specificOffsets.get().size());
			for (Map.Entry<Integer, Long> entry : specificOffsets.get().entrySet()) {
				List<String> partitionOffset = new ArrayList<>(2);
				partitionOffset.add(entry.getKey().toString());
				partitionOffset.add(entry.getValue().toString());
				propertyValues.add(JavaConversions.asScalaBuffer(partitionOffset).toSeq());
			}
			properties.putIndexedFixedProperties(
					SPECIFIC_OFFSETS,
					JavaConversions.asScalaBuffer(propertyKeys).toSeq(),
					JavaConversions.asScalaBuffer(propertyValues).toSeq()
			);
		}
		if (tableJsonMapping.isPresent()) {
			List<String> propertyKeys = new ArrayList<>();
			propertyKeys.add(TABLE_FIELD);
			propertyKeys.add(JSON_FIELD);

			List<Seq<String>> mappingFields = new ArrayList<>(tableJsonMapping.get().size());
			for (Map.Entry<String, String> entry : tableJsonMapping.get().entrySet()) {
				List<String> singleMapping = new ArrayList<>(2);
				singleMapping.add(entry.getKey());
				singleMapping.add(entry.getValue());
				mappingFields.add(JavaConversions.asScalaBuffer(singleMapping).toSeq());
			}
			properties.putIndexedFixedProperties(
					TABLE_JSON_MAPPING,
					JavaConversions.asScalaBuffer(propertyKeys).toSeq(),
					JavaConversions.asScalaBuffer(mappingFields).toSeq()
			);
		}
	}

	@Override
	public boolean needsFormat() {
		return true;
	}
}
