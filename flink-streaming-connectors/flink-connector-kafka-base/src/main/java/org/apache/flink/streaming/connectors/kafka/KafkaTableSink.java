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
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.sinks.StreamTableSink;
import org.apache.flink.api.table.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.util.Preconditions;

import java.util.Properties;

/**
 * A version-agnostic Kafka {@link StreamTableSink}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #createKafkaProducer(String, Properties, SerializationSchema, KafkaPartitioner)}}.
 */
public abstract class KafkaTableSink implements StreamTableSink<Row> {

	private final String topic;
	private final Properties properties;
	private final SerializationSchema<Row> serializationSchema;
	private final KafkaPartitioner<Row> partitioner;
	private final String[] fieldNames;
	private final TypeInformation[] fieldTypes;

	/**
	 * Creates KafkaTableSink
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param serializationSchema   Serialization schema for emitted items
	 * @param partitioner           Partitioner to select Kafka partition for each item
	 * @param fieldNames            Row field names.
	 * @param fieldTypes            Row field types.
	 */
	public KafkaTableSink(
			String topic,
			Properties properties,
			SerializationSchema<Row> serializationSchema,
			KafkaPartitioner<Row> partitioner,
			String[] fieldNames,
			Class<?>[] fieldTypes) {
		this(topic, properties, serializationSchema, partitioner, fieldNames, toTypeInfo(fieldTypes));
	}

	/**
	 * Creates KafkaTableSink
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param serializationSchema   Serialization schema for emitted items
	 * @param partitioner           Partitioner to select Kafka partition for each item
	 * @param fieldNames            Row field names.
	 * @param fieldTypes            Row field types.
	 */
	public KafkaTableSink(
			String topic,
			Properties properties,
			SerializationSchema<Row> serializationSchema,
			KafkaPartitioner<Row> partitioner,
			String[] fieldNames,
			TypeInformation<?>[] fieldTypes) {

		this.topic = Preconditions.checkNotNull(topic, "topic");
		this.properties = Preconditions.checkNotNull(properties, "properties");
		this.serializationSchema = Preconditions.checkNotNull(serializationSchema, "serializationSchema");
		this.partitioner = Preconditions.checkNotNull(partitioner, "partitioner");
		this.fieldNames = Preconditions.checkNotNull(fieldNames, "fieldNames");
		this.fieldTypes = Preconditions.checkNotNull(fieldTypes);
		Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
			"Number of provided field names and types does not match.");
	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		FlinkKafkaProducerBase<Row> kafkaProducer = createKafkaProducer(topic, properties, serializationSchema, partitioner);
		dataStream.addSink(kafkaProducer);
	}

	abstract protected FlinkKafkaProducerBase<Row> createKafkaProducer(
		String topic, Properties properties,
		SerializationSchema<Row> serializationSchema,
		KafkaPartitioner<Row> partitioner);

	@Override
	public TypeInformation<Row> getOutputType() {
		return new RowTypeInfo(getFieldTypes(), getFieldNames());
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}

	/**
	 * Creates TypeInformation array for an array of Classes.
	 */
	private static TypeInformation<?>[] toTypeInfo(Class<?>[] fieldTypes) {
		TypeInformation<?>[] typeInfos = new TypeInformation[fieldTypes.length];
		for (int i = 0; i < fieldTypes.length; i++) {
			typeInfos[i] = TypeExtractor.getForClass(fieldTypes[i]);
		}
		return typeInfos;
	}
}
