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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The validator for {@link Kafka}.
 */
public class KafkaValidator extends ConnectorDescriptorValidator {

	public static final String CONNECTOR_TYPE_VALUE_KAFKA = "kafka";
	public static final String CONNECTOR_VERSION_VALUE_08 = "0.8";
	public static final String CONNECTOR_VERSION_VALUE_09 = "0.9";
	public static final String CONNECTOR_VERSION_VALUE_010 = "0.10";
	public static final String CONNECTOR_VERSION_VALUE_011 = "0.11";
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
		properties.validateValue(CONNECTOR_TYPE(), CONNECTOR_TYPE_VALUE_KAFKA, false);

		validateVersion(properties);

		validateStartupMode(properties);

		validateKafkaProperties(properties);

		validateSinkPartitioner(properties);
	}

	private void validateVersion(DescriptorProperties properties) {
		final List<String> versions = Arrays.asList(
			CONNECTOR_VERSION_VALUE_08,
			CONNECTOR_VERSION_VALUE_09,
			CONNECTOR_VERSION_VALUE_010,
			CONNECTOR_VERSION_VALUE_011);
		properties.validateEnumValues(CONNECTOR_VERSION(), false, versions);
		properties.validateString(CONNECTOR_TOPIC, false, 1, Integer.MAX_VALUE);
	}

	private void validateStartupMode(DescriptorProperties properties) {
		final Map<String, Consumer<String>> specificOffsetValidators = new HashMap<>();
		specificOffsetValidators.put(
			CONNECTOR_SPECIFIC_OFFSETS_PARTITION,
			(prefix) -> properties.validateInt(
				prefix + CONNECTOR_SPECIFIC_OFFSETS_PARTITION,
				false,
				0,
				Integer.MAX_VALUE));
		specificOffsetValidators.put(
			CONNECTOR_SPECIFIC_OFFSETS_OFFSET,
			(prefix) -> properties.validateLong(
				prefix + CONNECTOR_SPECIFIC_OFFSETS_OFFSET,
				false,
				0,
				Long.MAX_VALUE));

		final Map<String, Consumer<String>> startupModeValidation = new HashMap<>();
		startupModeValidation.put(CONNECTOR_STARTUP_MODE_VALUE_GROUP_OFFSETS, properties.noValidation());
		startupModeValidation.put(CONNECTOR_STARTUP_MODE_VALUE_EARLIEST, properties.noValidation());
		startupModeValidation.put(CONNECTOR_STARTUP_MODE_VALUE_LATEST, properties.noValidation());
		startupModeValidation.put(
			CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS,
			prefix -> properties.validateFixedIndexedProperties(CONNECTOR_SPECIFIC_OFFSETS, false, specificOffsetValidators));
		properties.validateEnum(CONNECTOR_STARTUP_MODE, true, startupModeValidation);
	}

	private void validateKafkaProperties(DescriptorProperties properties) {
		final Map<String, Consumer<String>> propertyValidators = new HashMap<>();
		propertyValidators.put(
			CONNECTOR_PROPERTIES_KEY,
			prefix -> properties.validateString(prefix + CONNECTOR_PROPERTIES_KEY, false, 1));
		propertyValidators.put(
			CONNECTOR_PROPERTIES_VALUE,
			prefix -> properties.validateString(prefix + CONNECTOR_PROPERTIES_VALUE, false, 0));
		properties.validateFixedIndexedProperties(CONNECTOR_PROPERTIES, true, propertyValidators);
	}

	private void validateSinkPartitioner(DescriptorProperties properties) {
		final Map<String, Consumer<String>> sinkPartitionerValidators = new HashMap<>();
		sinkPartitionerValidators.put(CONNECTOR_SINK_PARTITIONER_VALUE_FIXED, properties.noValidation());
		sinkPartitionerValidators.put(CONNECTOR_SINK_PARTITIONER_VALUE_ROUND_ROBIN, properties.noValidation());
		sinkPartitionerValidators.put(
			CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM,
			prefix -> properties.validateString(CONNECTOR_SINK_PARTITIONER_CLASS, false, 1));
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
}
