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

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.table.api.ValidationException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.table.descriptors.DescriptorProperties.noValidation;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;

/**
 * The validator for {@link Kafka}.
 */
@Internal
public class KafkaValidator extends ConnectorDescriptorValidator {

	public static final String CONNECTOR_TYPE_VALUE_KAFKA = "kafka";
	public static final String CONNECTOR_VERSION_VALUE_08 = "0.8";
	public static final String CONNECTOR_VERSION_VALUE_09 = "0.9";
	public static final String CONNECTOR_VERSION_VALUE_010 = "0.10";
	public static final String CONNECTOR_VERSION_VALUE_011 = "0.11";
	public static final String CONNECTOR_VERSION_VALUE_UNIVERSAL = "universal";
	public static final String CONNECTOR_TOPIC = "connector.topic";
	public static final String CONNECTOR_STARTUP_MODE = "connector.startup-mode";
	public static final String CONNECTOR_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
	public static final String CONNECTOR_STARTUP_MODE_VALUE_LATEST = "latest-offset";
	public static final String CONNECTOR_STARTUP_MODE_VALUE_GROUP_OFFSETS = "group-offsets";
	public static final String CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS = "specific-offsets";
	public static final String CONNECTOR_SPECIFIC_OFFSETS = "connector.specific-offsets";
	public static final String CONNECTOR_SPECIFIC_OFFSETS_PARTITION = "partition";
	public static final String CONNECTOR_SPECIFIC_OFFSETS_OFFSET = "offset";
	public static final String CONNECTOR_PROPERTIES = "connector.properties";
	public static final String CONNECTOR_PROPERTIES_ZOOKEEPER_CONNECT = "connector.properties.zookeeper.connect";
	public static final String CONNECTOR_PROPERTIES_BOOTSTRAP_SERVER = "connector.properties.bootstrap.servers";
	public static final String CONNECTOR_PROPERTIES_GROUP_ID = "connector.properties.group.id";
	public static final String CONNECTOR_PROPERTIES_KEY = "key";
	public static final String CONNECTOR_PROPERTIES_VALUE = "value";
	public static final String CONNECTOR_SINK_PARTITIONER = "connector.sink-partitioner";
	public static final String CONNECTOR_SINK_PARTITIONER_VALUE_FIXED = "fixed";
	public static final String CONNECTOR_SINK_PARTITIONER_VALUE_ROUND_ROBIN = "round-robin";
	public static final String CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM = "custom";
	public static final String CONNECTOR_SINK_PARTITIONER_CLASS = "connector.sink-partitioner-class";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateEnumValues(UPDATE_MODE, true, Collections.singletonList(UPDATE_MODE_VALUE_APPEND));

		properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_KAFKA, false);

		properties.validateString(CONNECTOR_TOPIC, false, 1, Integer.MAX_VALUE);

		validateStartupMode(properties);

		validateKafkaProperties(properties);

		validateSinkPartitioner(properties);
	}

	private void validateStartupMode(DescriptorProperties properties) {
		final Map<String, Consumer<String>> specificOffsetValidators = new HashMap<>();
		specificOffsetValidators.put(
			CONNECTOR_SPECIFIC_OFFSETS_PARTITION,
			(key) -> properties.validateInt(
				key,
				false,
				0,
				Integer.MAX_VALUE));
		specificOffsetValidators.put(
			CONNECTOR_SPECIFIC_OFFSETS_OFFSET,
			(key) -> properties.validateLong(
				key,
				false,
				0,
				Long.MAX_VALUE));

		final Map<String, Consumer<String>> startupModeValidation = new HashMap<>();
		startupModeValidation.put(CONNECTOR_STARTUP_MODE_VALUE_GROUP_OFFSETS, noValidation());
		startupModeValidation.put(CONNECTOR_STARTUP_MODE_VALUE_EARLIEST, noValidation());
		startupModeValidation.put(CONNECTOR_STARTUP_MODE_VALUE_LATEST, noValidation());

		if (properties.containsKey(CONNECTOR_SPECIFIC_OFFSETS)) {
			validateAndParseSpecificOffsetsString(properties);
			startupModeValidation.put(CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS, noValidation());
		} else {
			startupModeValidation.put(CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS,
				key -> properties.validateFixedIndexedProperties(CONNECTOR_SPECIFIC_OFFSETS, false, specificOffsetValidators));
		}

		properties.validateEnum(CONNECTOR_STARTUP_MODE, true, startupModeValidation);
	}

	private void validateKafkaProperties(DescriptorProperties properties) {
		if (properties.containsKey(CONNECTOR_PROPERTIES_ZOOKEEPER_CONNECT)
			|| properties.containsKey(CONNECTOR_PROPERTIES_BOOTSTRAP_SERVER)
			|| properties.containsKey(CONNECTOR_PROPERTIES_GROUP_ID)) {

			properties.validateString(CONNECTOR_PROPERTIES_ZOOKEEPER_CONNECT, false);
			properties.validateString(CONNECTOR_PROPERTIES_BOOTSTRAP_SERVER, false);
			properties.validateString(CONNECTOR_PROPERTIES_GROUP_ID, true);

		} else {
			final Map<String, Consumer<String>> propertyValidators = new HashMap<>();
			propertyValidators.put(
				CONNECTOR_PROPERTIES_KEY,
				key -> properties.validateString(key, false, 1));
			propertyValidators.put(
				CONNECTOR_PROPERTIES_VALUE,
				key -> properties.validateString(key, false, 0));
			properties.validateFixedIndexedProperties(CONNECTOR_PROPERTIES, true, propertyValidators);
		}
	}

	private void validateSinkPartitioner(DescriptorProperties properties) {
		final Map<String, Consumer<String>> sinkPartitionerValidators = new HashMap<>();
		sinkPartitionerValidators.put(CONNECTOR_SINK_PARTITIONER_VALUE_FIXED, noValidation());
		sinkPartitionerValidators.put(CONNECTOR_SINK_PARTITIONER_VALUE_ROUND_ROBIN, noValidation());
		sinkPartitionerValidators.put(
			CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM,
			key -> properties.validateString(CONNECTOR_SINK_PARTITIONER_CLASS, false, 1));
		properties.validateEnum(CONNECTOR_SINK_PARTITIONER, true, sinkPartitionerValidators);
	}

	// utilities

	public static String normalizeStartupMode(StartupMode startupMode) {
		switch (startupMode) {
			case EARLIEST:
				return CONNECTOR_STARTUP_MODE_VALUE_EARLIEST;
			case LATEST:
				return CONNECTOR_STARTUP_MODE_VALUE_LATEST;
			case GROUP_OFFSETS:
				return CONNECTOR_STARTUP_MODE_VALUE_GROUP_OFFSETS;
			case SPECIFIC_OFFSETS:
				return CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS;
		}
		throw new IllegalArgumentException("Invalid startup mode.");
	}

	/**
	 * Parse SpecificOffsets String to Map.
	 *
	 * <p>SpecificOffsets String format was given as following:
	 *
	 * <pre>
	 *     connector.specific-offsets = partition:0,offset:42;partition:1,offset:300
	 * </pre>
	 * @return SpecificOffsets with Map format, key is partition, and value is offset.
	 */
	public static Map<Integer, Long> validateAndParseSpecificOffsetsString(DescriptorProperties descriptorProperties) {
		final Map<Integer, Long> offsetMap = new HashMap<>();

		descriptorProperties.validateString(CONNECTOR_SPECIFIC_OFFSETS, false, 1);
		final String parseSpecificOffsetsStr = descriptorProperties.getString(CONNECTOR_SPECIFIC_OFFSETS);

		final String[] pairs = parseSpecificOffsetsStr.split(";");
		final String validationExceptionMessage = "Invalid properties '" + CONNECTOR_SPECIFIC_OFFSETS +
			"' should follow the format 'partition:0,offset:42;partition:1,offset:300', " +
			"but is '" + parseSpecificOffsetsStr + "'.";

		if (pairs.length == 0) {
			throw new ValidationException(validationExceptionMessage);
		}

		for (String pair : pairs) {
			if (null == pair || pair.length() == 0 || !pair.contains(",")) {
				throw new ValidationException(validationExceptionMessage);
			}

			final String[] kv = pair.split(",");
			if (kv.length != 2 ||
				!kv[0].startsWith(CONNECTOR_SPECIFIC_OFFSETS_PARTITION + ':') ||
				!kv[1].startsWith(CONNECTOR_SPECIFIC_OFFSETS_OFFSET + ':')) {
				throw new ValidationException(validationExceptionMessage);
			}

			String partitionValue = kv[0].substring(kv[0].indexOf(":") + 1);
			String offsetValue = kv[1].substring(kv[1].indexOf(":") + 1);
			try {
				final Integer partition = Integer.valueOf(partitionValue);
				final Long offset = Long.valueOf(offsetValue);
				offsetMap.put(partition, offset);
			} catch (NumberFormatException e) {
				throw new ValidationException(validationExceptionMessage, e);
			}
		}
		return offsetMap;
	}

	public static boolean hasConciseKafkaProperties(DescriptorProperties descriptorProperties) {
		return descriptorProperties.containsKey(CONNECTOR_PROPERTIES_ZOOKEEPER_CONNECT) ||
			descriptorProperties.containsKey(CONNECTOR_PROPERTIES_BOOTSTRAP_SERVER) ||
			descriptorProperties.containsKey(CONNECTOR_PROPERTIES_GROUP_ID);
	}
}
