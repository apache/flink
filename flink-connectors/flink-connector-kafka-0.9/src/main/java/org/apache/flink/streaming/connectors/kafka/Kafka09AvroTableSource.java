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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka {@link StreamTableSource} for Kafka 0.9.
 *
 * @deprecated Use the {@link org.apache.flink.table.descriptors.Kafka} descriptor together
 *             with descriptors for schema and format instead. Descriptors allow for
 *             implementation-agnostic definition of tables. See also
 *             {@link org.apache.flink.table.api.TableEnvironment#connect(ConnectorDescriptor)}.
 */
@Deprecated
public class Kafka09AvroTableSource extends KafkaAvroTableSource {

	/**
	 * Creates a Kafka 0.9 Avro {@link StreamTableSource} using a given {@link SpecificRecord}.
	 *
	 * @param topic      Kafka topic to consume.
	 * @param properties Properties for the Kafka consumer.
	 * @param schema	 Schema of the produced table.
	 * @param record     Avro specific record.
	 */
	public Kafka09AvroTableSource(
		String topic,
		Properties properties,
		TableSchema schema,
		Class<? extends SpecificRecordBase> record) {

		super(
			topic,
			properties,
			schema,
			record);
	}

	/**
	 * Sets a mapping from schema fields to fields of the produced Avro record.
	 *
	 * <p>A field mapping is required if the fields of produced tables should be named different than
	 * the fields of the Avro record.
	 * The key of the provided Map refers to the field of the table schema,
	 * the value to the field of the Avro record.</p>
	 *
	 * @param fieldMapping A mapping from schema fields to Avro fields.
	 * @deprecated Use table descriptors instead of implementation-specific builders.
	 */
	@Deprecated
	@Override
	public void setFieldMapping(Map<String, String> fieldMapping) {
		super.setFieldMapping(fieldMapping);
	}

	/**
	 * Declares a field of the schema to be a processing time attribute.
	 *
	 * @param proctimeAttribute The name of the field that becomes the processing time field.
	 * @deprecated Use table descriptors instead of implementation-specific builders.
	 */
	@Deprecated
	@Override
	public void setProctimeAttribute(String proctimeAttribute) {
		super.setProctimeAttribute(proctimeAttribute);
	}

	/**
	 * Declares a field of the schema to be a rowtime attribute.
	 *
	 * @param rowtimeAttributeDescriptor The descriptor of the rowtime attribute.
	 * @deprecated Use table descriptors instead of implementation-specific builders.
	 */
	@Deprecated
	public void setRowtimeAttributeDescriptor(RowtimeAttributeDescriptor rowtimeAttributeDescriptor) {
		Preconditions.checkNotNull(rowtimeAttributeDescriptor, "Rowtime attribute descriptor must not be null.");
		super.setRowtimeAttributeDescriptors(Collections.singletonList(rowtimeAttributeDescriptor));
	}

	@Override
	protected FlinkKafkaConsumerBase<Row> createKafkaConsumer(String topic, Properties properties, DeserializationSchema<Row> deserializationSchema) {
		return new FlinkKafkaConsumer09<>(topic, deserializationSchema, properties);
	}

	/**
	 * Returns a builder to configure and create a {@link Kafka09AvroTableSource}.
	 *
	 * @return A builder to configure and create a {@link Kafka09AvroTableSource}.
	 * @deprecated Use the {@link org.apache.flink.table.descriptors.Kafka} descriptor together
	 *             with descriptors for schema and format instead. Descriptors allow for
	 *             implementation-agnostic definition of tables. See also
	 *             {@link org.apache.flink.table.api.TableEnvironment#connect(ConnectorDescriptor)}.
	 */
	@Deprecated
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * A builder to configure and create a {@link Kafka09AvroTableSource}.
	 *
	 * @deprecated Use the {@link org.apache.flink.table.descriptors.Kafka} descriptor together
	 *             with descriptors for schema and format instead. Descriptors allow for
	 *             implementation-agnostic definition of tables. See also
	 *             {@link org.apache.flink.table.api.TableEnvironment#connect(ConnectorDescriptor)}.
	 */
	@Deprecated
	public static class Builder extends KafkaAvroTableSource.Builder<Kafka09AvroTableSource, Kafka09AvroTableSource.Builder> {

		@Override
		protected boolean supportsKafkaTimestamps() {
			return false;
		}

		@Override
		protected Kafka09AvroTableSource.Builder builder() {
			return this;
		}

		/**
		 * Builds and configures a {@link Kafka09AvroTableSource}.
		 *
		 * @return A configured {@link Kafka09AvroTableSource}.
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		@Override
		public Kafka09AvroTableSource build() {
			Kafka09AvroTableSource tableSource = new Kafka09AvroTableSource(
				getTopic(),
				getKafkaProps(),
				getTableSchema(),
				getAvroRecordClass());
			super.configureTableSource(tableSource);
			return tableSource;
		}
	}
}

