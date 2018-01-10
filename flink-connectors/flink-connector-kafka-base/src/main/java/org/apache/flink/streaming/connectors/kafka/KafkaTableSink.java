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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Properties;

/**
 * A version-agnostic Kafka {@link AppendStreamTableSink}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #createKafkaProducer(String, Properties, SerializationSchema, FlinkKafkaPartitioner)}}.
 */
@Internal
public abstract class KafkaTableSink implements AppendStreamTableSink<Row> {

	protected final String topic;
	protected final Properties properties;
	protected SerializationSchema<Row> serializationSchema;
	protected final FlinkKafkaPartitioner<Row> partitioner;
	protected String[] fieldNames;
	protected TypeInformation[] fieldTypes;

	/**
	 * Creates KafkaTableSink.
	 *
	 * @param topic                 Kafka topic to write to.
	 * @param properties            Properties for the Kafka consumer.
	 * @param partitioner           Partitioner to select Kafka partition for each item
	 */
	public KafkaTableSink(
			String topic,
			Properties properties,
			FlinkKafkaPartitioner<Row> partitioner) {
		this.topic = Preconditions.checkNotNull(topic, "topic");
		this.properties = Preconditions.checkNotNull(properties, "properties");
		this.partitioner = Preconditions.checkNotNull(partitioner, "partitioner");
	}

	/**
	 * Returns the version-specific Kafka producer.
	 *
	 * @param topic               Kafka topic to produce to.
	 * @param properties          Properties for the Kafka producer.
	 * @param serializationSchema Serialization schema to use to create Kafka records.
	 * @param partitioner         Partitioner to select Kafka partition.
	 * @return The version-specific Kafka producer
	 */
	protected abstract FlinkKafkaProducerBase<Row> createKafkaProducer(
		String topic, Properties properties,
		SerializationSchema<Row> serializationSchema,
		FlinkKafkaPartitioner<Row> partitioner);

	/**
	 * Create serialization schema for converting table rows into bytes.
	 *
	 * @param rowSchema the schema of the row to serialize.
	 * @return Instance of serialization schema
	 */
	protected abstract SerializationSchema<Row> createSerializationSchema(RowTypeInfo rowSchema);

	/**
	 * Create a deep copy of this sink.
	 *
	 * @return Deep copy of this sink
	 */
	protected abstract KafkaTableSink createCopy();

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		FlinkKafkaProducerBase<Row> kafkaProducer = createKafkaProducer(topic, properties, serializationSchema, partitioner);
		// always enable flush on checkpoint to achieve at-least-once if query runs with checkpointing enabled.
		kafkaProducer.setFlushOnCheckpoint(true);
		dataStream.addSink(kafkaProducer).name(TableConnectorUtil.generateRuntimeName(this.getClass(), fieldNames));
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return new RowTypeInfo(getFieldTypes());
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}

	@Override
	public KafkaTableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		KafkaTableSink copy = createCopy();
		copy.fieldNames = Preconditions.checkNotNull(fieldNames, "fieldNames");
		copy.fieldTypes = Preconditions.checkNotNull(fieldTypes, "fieldTypes");
		Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
			"Number of provided field names and types does not match.");

		RowTypeInfo rowSchema = new RowTypeInfo(fieldTypes, fieldNames);
		copy.serializationSchema = createSerializationSchema(rowSchema);

		return copy;
	}
}
