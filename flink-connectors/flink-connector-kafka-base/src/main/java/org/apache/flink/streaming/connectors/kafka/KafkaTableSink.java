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
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * A version-agnostic Kafka {@link AppendStreamTableSink}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #createKafkaProducer(String, Properties, SerializationSchema, Optional)}}.
 */
@Internal
public abstract class KafkaTableSink implements AppendStreamTableSink<Row> {

	// TODO make all attributes final and mandatory once we drop support for format-specific table sinks

	/** The schema of the table. */
	private final Optional<TableSchema> schema;

	/** The Kafka topic to write to. */
	protected final String topic;

	/** Properties for the Kafka producer. */
	protected final Properties properties;

	/** Serialization schema for encoding records to Kafka. */
	protected Optional<SerializationSchema<Row>> serializationSchema;

	/** Partitioner to select Kafka partition for each item. */
	protected final Optional<FlinkKafkaPartitioner<Row>> partitioner;

	// legacy variables
	protected String[] fieldNames;
	protected TypeInformation[] fieldTypes;

	protected KafkaTableSink(
			TableSchema schema,
			String topic,
			Properties properties,
			Optional<FlinkKafkaPartitioner<Row>> partitioner,
			SerializationSchema<Row> serializationSchema) {
		this.schema = Optional.of(Preconditions.checkNotNull(schema, "Schema must not be null."));
		this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
		this.partitioner = Preconditions.checkNotNull(partitioner, "Partitioner must not be null.");
		this.serializationSchema = Optional.of(Preconditions.checkNotNull(
			serializationSchema, "Serialization schema must not be null."));
	}

	/**
	 * Creates KafkaTableSink.
	 *
	 * @param topic                 Kafka topic to write to.
	 * @param properties            Properties for the Kafka producer.
	 * @param partitioner           Partitioner to select Kafka partition for each item
	 * @deprecated Use table descriptors instead of implementation-specific classes.
	 */
	@Deprecated
	public KafkaTableSink(
			String topic,
			Properties properties,
			FlinkKafkaPartitioner<Row> partitioner) {
		this.schema = Optional.empty();
		this.topic = Preconditions.checkNotNull(topic, "topic");
		this.properties = Preconditions.checkNotNull(properties, "properties");
		this.partitioner = Optional.of(Preconditions.checkNotNull(partitioner, "partitioner"));
		this.serializationSchema = Optional.empty();
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
	protected abstract SinkFunction<Row> createKafkaProducer(
		String topic,
		Properties properties,
		SerializationSchema<Row> serializationSchema,
		Optional<FlinkKafkaPartitioner<Row>> partitioner);

	/**
	 * Create serialization schema for converting table rows into bytes.
	 *
	 * @param rowSchema the schema of the row to serialize.
	 * @return Instance of serialization schema
	 * @deprecated Use the constructor to pass a serialization schema instead.
	 */
	@Deprecated
	protected SerializationSchema<Row> createSerializationSchema(RowTypeInfo rowSchema) {
		throw new UnsupportedOperationException("This method only exists for backwards compatibility.");
	}

	/**
	 * Create a deep copy of this sink.
	 *
	 * @return Deep copy of this sink
	 */
	@Deprecated
	protected KafkaTableSink createCopy() {
		throw new UnsupportedOperationException("This method only exists for backwards compatibility.");
	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		SinkFunction<Row> kafkaProducer = createKafkaProducer(
			topic,
			properties,
			serializationSchema.orElseThrow(() -> new IllegalStateException("No serialization schema defined.")),
			partitioner);
		dataStream.addSink(kafkaProducer).name(TableConnectorUtil.generateRuntimeName(this.getClass(), fieldNames));
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return schema
			.map(TableSchema::toRowType)
			.orElseGet(() -> new RowTypeInfo(getFieldTypes()));
	}

	public String[] getFieldNames() {
		return schema.map(TableSchema::getColumnNames).orElse(fieldNames);
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return schema.map(TableSchema::getTypes).orElse(fieldTypes);
	}

	@Override
	public KafkaTableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		if (schema.isPresent()) {
			// a fixed schema is defined so reconfiguration is not supported
			throw new UnsupportedOperationException("Reconfiguration of this sink is not supported.");
		}

		// legacy code
		KafkaTableSink copy = createCopy();
		copy.fieldNames = Preconditions.checkNotNull(fieldNames, "fieldNames");
		copy.fieldTypes = Preconditions.checkNotNull(fieldTypes, "fieldTypes");
		Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
			"Number of provided field names and types does not match.");

		RowTypeInfo rowSchema = new RowTypeInfo(fieldTypes, fieldNames);
		copy.serializationSchema = Optional.of(createSerializationSchema(rowSchema));

		return copy;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		KafkaTableSink that = (KafkaTableSink) o;
		return Objects.equals(schema, that.schema) &&
			Objects.equals(topic, that.topic) &&
			Objects.equals(properties, that.properties) &&
			Objects.equals(serializationSchema, that.serializationSchema) &&
			Objects.equals(partitioner, that.partitioner) &&
			Arrays.equals(fieldNames, that.fieldNames) &&
			Arrays.equals(fieldTypes, that.fieldTypes);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(
			schema,
			topic,
			properties,
			serializationSchema,
			partitioner);
		result = 31 * result + Arrays.hashCode(fieldNames);
		result = 31 * result + Arrays.hashCode(fieldTypes);
		return result;
	}
}
