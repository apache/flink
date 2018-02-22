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
import org.apache.flink.table.api.ValidationException;

import java.util.HashMap;
import java.util.Map;

import scala.Function0;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;


/**
 * The validator for {@link Kafka}.
 */
public class KafkaValidator extends ConnectorDescriptorValidator {
	// fields
	public static final String CONNECTOR_TYPE_VALUE = "kafka";
	public static final String KAFKA_VERSION = "kafka.version";
	public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
	public static final String GROUP_ID = "group.id";
	public static final String TOPIC = "topic";
	public static final String STARTUP_MODE = "startup.mode";
	public static final String SPECIFIC_OFFSETS = "specific.offsets";
	public static final String TABLE_JSON_MAPPING = "table.json.mapping";

	public static final String PARTITION = "partition";
	public static final String OFFSET = "offset";

	public static final String TABLE_FIELD = "table.field";
	public static final String JSON_FIELD = "json.field";

	public static final String ZOOKEEPER_CONNECT = "zookeeper.connect"; // only required for 0.8

	// values
	public static final String KAFKA_VERSION_VALUE_08 = "0.8";
	public static final String KAFKA_VERSION_VALUE_09 = "0.9";
	public static final String KAFKA_VERSION_VALUE_010 = "0.10";
	public static final String KAFKA_VERSION_VALUE_011 = "0.11";

	public static final String STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
	public static final String STARTUP_MODE_VALUE_LATEST = "latest-offset";
	public static final String STARTUP_MODE_VALUE_GROUP_OFFSETS = "group-offsets";
	public static final String STARTUP_MODE_VALUE_SPECIFIC_OFFSETS = "specific-offsets";

	// utils
	public static Map<String, String> normalizeStartupMode(StartupMode startupMode) {
		Map<String, String> mapPair = new HashMap<>();
		switch (startupMode) {
			case EARLIEST:
				mapPair.put(STARTUP_MODE, STARTUP_MODE_VALUE_EARLIEST);
				break;
			case LATEST:
				mapPair.put(STARTUP_MODE, STARTUP_MODE_VALUE_LATEST);
				break;
			case GROUP_OFFSETS:
				mapPair.put(STARTUP_MODE, STARTUP_MODE_VALUE_GROUP_OFFSETS);
				break;
			case SPECIFIC_OFFSETS:
				mapPair.put(STARTUP_MODE, STARTUP_MODE_VALUE_SPECIFIC_OFFSETS);
				break;
		}
		return mapPair;
	}

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);

		AbstractFunction0<BoxedUnit> emptyValidator = new AbstractFunction0<BoxedUnit>() {
			@Override
			public BoxedUnit apply() {
				return BoxedUnit.UNIT;
			}
		};

		properties.validateValue(CONNECTOR_TYPE(), CONNECTOR_TYPE_VALUE, false);

		AbstractFunction0<BoxedUnit> version08Validator = new AbstractFunction0<BoxedUnit>() {
			@Override
			public BoxedUnit apply() {
				properties.validateString(ZOOKEEPER_CONNECT, false, 0, Integer.MAX_VALUE);
				return BoxedUnit.UNIT;
			}
		};

		Map<String, Function0<BoxedUnit>> versionValidatorMap = new HashMap<>();
		versionValidatorMap.put(KAFKA_VERSION_VALUE_08, version08Validator);
		versionValidatorMap.put(KAFKA_VERSION_VALUE_09, emptyValidator);
		versionValidatorMap.put(KAFKA_VERSION_VALUE_010, emptyValidator);
		versionValidatorMap.put(KAFKA_VERSION_VALUE_011, emptyValidator);
		properties.validateEnum(
				KAFKA_VERSION,
				false,
				toScalaImmutableMap(versionValidatorMap)
		);

		properties.validateString(BOOTSTRAP_SERVERS, false, 1, Integer.MAX_VALUE);
		properties.validateString(GROUP_ID, false, 1, Integer.MAX_VALUE);
		properties.validateString(TOPIC, false, 1, Integer.MAX_VALUE);

		AbstractFunction0<BoxedUnit> specificOffsetsValidator = new AbstractFunction0<BoxedUnit>() {
			@Override
			public BoxedUnit apply() {
				Map<String, String> partitions = JavaConversions.mapAsJavaMap(
						properties.getIndexedProperty(SPECIFIC_OFFSETS, PARTITION));

				Map<String, String> offsets = JavaConversions.mapAsJavaMap(
						properties.getIndexedProperty(SPECIFIC_OFFSETS, OFFSET));
				if (partitions.isEmpty() || offsets.isEmpty()) {
					throw new ValidationException("Offsets must be set for SPECIFIC_OFFSETS mode.");
				}
				for (int i = 0; i < partitions.size(); ++i) {
					properties.validateInt(
							SPECIFIC_OFFSETS + "." + i + "." + PARTITION,
							false,
							0,
							Integer.MAX_VALUE);
					properties.validateLong(
							SPECIFIC_OFFSETS + "." + i + "." + OFFSET,
							false,
							0,
							Long.MAX_VALUE);
				}
				return BoxedUnit.UNIT;
			}
		};
		Map<String, Function0<BoxedUnit>> startupModeValidatorMap = new HashMap<>();
		startupModeValidatorMap.put(STARTUP_MODE_VALUE_GROUP_OFFSETS, emptyValidator);
		startupModeValidatorMap.put(STARTUP_MODE_VALUE_EARLIEST, emptyValidator);
		startupModeValidatorMap.put(STARTUP_MODE_VALUE_LATEST, emptyValidator);
		startupModeValidatorMap.put(STARTUP_MODE_VALUE_SPECIFIC_OFFSETS, specificOffsetsValidator);

		properties.validateEnum(STARTUP_MODE, true, toScalaImmutableMap(startupModeValidatorMap));
		validateTableJsonMapping(properties);
	}

	private void validateTableJsonMapping(DescriptorProperties properties) {
		Map<String, String> mappingTableField = JavaConversions.mapAsJavaMap(
				properties.getIndexedProperty(TABLE_JSON_MAPPING, TABLE_FIELD));
		Map<String, String> mappingJsonField = JavaConversions.mapAsJavaMap(
				properties.getIndexedProperty(TABLE_JSON_MAPPING, JSON_FIELD));

		if (mappingJsonField.size() != mappingJsonField.size()) {
			throw new ValidationException("Table JSON mapping must be one to one.");
		}

		for (int i = 0; i < mappingTableField.size(); i++) {
			properties.validateString(
					TABLE_JSON_MAPPING + "." + i + "." + TABLE_FIELD,
					false,
					1,
					Integer.MAX_VALUE);
			properties.validateString(
					TABLE_JSON_MAPPING + "." + i + "." + JSON_FIELD,
					false,
					1,
					Integer.MAX_VALUE);
		}
	}

	@SuppressWarnings("unchecked")
	private <K, V> scala.collection.immutable.Map<K, V> toScalaImmutableMap(Map<K, V> javaMap) {
		final java.util.List<scala.Tuple2<K, V>> list = new java.util.ArrayList<>(javaMap.size());
		for (final java.util.Map.Entry<K, V> entry : javaMap.entrySet()) {
			list.add(scala.Tuple2.apply(entry.getKey(), entry.getValue()));
		}
		final scala.collection.Seq<Tuple2<K, V>> seq =
				scala.collection.JavaConverters.asScalaBufferConverter(list).asScala().toSeq();
		return (scala.collection.immutable.Map<K, V>) scala.collection.immutable.Map$.MODULE$.apply(seq);
	}
}
