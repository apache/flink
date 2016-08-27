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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.sources.StreamTableSource;
import org.apache.flink.api.table.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.util.Preconditions;

import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.internals.TypeUtil.toTypeInfo;

/**
 * A version-agnostic Kafka {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #getKafkaConsumer(String, Properties, DeserializationSchema)}}.
 */
abstract class KafkaTableSource implements StreamTableSource<Row> {

	/** The Kafka topic to consume. */
	private final String topic;

	/** Properties for the Kafka consumer. */
	private final Properties properties;

	/** Deserialization schema to use for Kafka records. */
	private final DeserializationSchema<Row> deserializationSchema;

	/** Row field names. */
	private final String[] fieldNames;

	/** Row field types. */
	private final TypeInformation<?>[] fieldTypes;

	/**
	 * Creates a generic Kafka {@link StreamTableSource}.
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @param fieldNames            Row field names.
	 * @param fieldTypes            Row field types.
	 */
	KafkaTableSource(
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema,
			String[] fieldNames,
			Class<?>[] fieldTypes) {

		this(topic, properties, deserializationSchema, fieldNames, toTypeInfo(fieldTypes));
	}

	/**
	 * Creates a generic Kafka {@link StreamTableSource}.
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @param fieldNames            Row field names.
	 * @param fieldTypes            Row field types.
	 */
	KafkaTableSource(
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema,
			String[] fieldNames,
			TypeInformation<?>[] fieldTypes) {

		this.topic = Preconditions.checkNotNull(topic, "Topic");
		this.properties = Preconditions.checkNotNull(properties, "Properties");
		this.deserializationSchema = Preconditions.checkNotNull(deserializationSchema, "Deserialization schema");
		this.fieldNames = Preconditions.checkNotNull(fieldNames, "Field names");
		this.fieldTypes = Preconditions.checkNotNull(fieldTypes, "Field types");

		Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
				"Number of provided field names and types does not match.");
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
		// Version-specific Kafka consumer
		FlinkKafkaConsumerBase<Row> kafkaConsumer = getKafkaConsumer(topic, properties, deserializationSchema);
		DataStream<Row> kafkaSource = env.addSource(kafkaConsumer);
		return kafkaSource;
	}

	@Override
	public int getNumberOfFields() {
		return fieldNames.length;
	}

	@Override
	public String[] getFieldsNames() {
		return fieldNames;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return new RowTypeInfo(fieldTypes);
	}

	/**
	 * Returns the version-specific Kafka consumer.
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @return The version-specific Kafka consumer
	 */
	abstract FlinkKafkaConsumerBase<Row> getKafkaConsumer(
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema);

	/**
	 * Returns the deserialization schema.
	 *
	 * @return The deserialization schema
	 */
	protected DeserializationSchema<Row> getDeserializationSchema() {
		return deserializationSchema;
	}
}
