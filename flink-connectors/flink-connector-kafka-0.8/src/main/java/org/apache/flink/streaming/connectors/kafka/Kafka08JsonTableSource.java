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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka {@link StreamTableSource} for Kafka 0.8.
 */
@PublicEvolving
public class Kafka08JsonTableSource extends KafkaJsonTableSource {

	/**
	 * Creates a Kafka 0.8 JSON {@link StreamTableSource}.
	 *
	 * @param topic       Kafka topic to consume.
	 * @param properties  Properties for the Kafka consumer.
	 * @param tableSchema The schema of the table.
	 * @param jsonSchema  The schema of the JSON messages to decode from Kafka.
	 */
	public Kafka08JsonTableSource(
		String topic,
		Properties properties,
		TableSchema tableSchema,
		TableSchema jsonSchema) {

		super(topic, properties, tableSchema, jsonSchema);
	}

	/**
	 * Sets the flag that specifies the behavior in case of missing fields.
	 * TableSource will fail for missing fields if set to true. If set to false, the missing field is set to null.
	 *
	 * @param failOnMissingField Flag that specifies the TableSource behavior in case of missing fields.
	 */
	@Override
	public void setFailOnMissingField(boolean failOnMissingField) {
		super.setFailOnMissingField(failOnMissingField);
	}

	/**
	 * Sets the mapping from table schema fields to JSON schema fields.
	 *
	 * @param fieldMapping The mapping from table schema fields to JSON schema fields.
	 */
	@Override
	public void setFieldMapping(Map<String, String> fieldMapping) {
		super.setFieldMapping(fieldMapping);
	}

	/**
	 * Declares a field of the schema to be a processing time attribute.
	 *
	 * @param proctimeAttribute The name of the field that becomes the processing time field.
	 */
	@Override
	public void setProctimeAttribute(String proctimeAttribute) {
		super.setProctimeAttribute(proctimeAttribute);
	}

	/**
	 * Declares a field of the schema to be a rowtime attribute.
	 *
	 * @param rowtimeAttributeDescriptor The descriptor of the rowtime attribute.
	 */
	public void setRowtimeAttributeDescriptor(RowtimeAttributeDescriptor rowtimeAttributeDescriptor) {
		Preconditions.checkNotNull(rowtimeAttributeDescriptor, "Rowtime attribute descriptor must not be null.");
		super.setRowtimeAttributeDescriptors(Collections.singletonList(rowtimeAttributeDescriptor));
	}

	@Override
	protected FlinkKafkaConsumerBase<Row> createKafkaConsumer(String topic, Properties properties, DeserializationSchema<Row> deserializationSchema) {
		return new FlinkKafkaConsumer08<>(topic, deserializationSchema, properties);
	}

	/**
	 * Returns a builder to configure and create a {@link Kafka08JsonTableSource}.
	 * @return A builder to configure and create a {@link Kafka08JsonTableSource}.
	 */
	public static Kafka08JsonTableSource.Builder builder() {
		return new Kafka08JsonTableSource.Builder();
	}

	/**
	 * A builder to configure and create a {@link Kafka08JsonTableSource}.
	 */
	public static class Builder extends KafkaJsonTableSource.Builder<Kafka08JsonTableSource, Kafka08JsonTableSource.Builder> {

		@Override
		protected boolean supportsKafkaTimestamps() {
			return false;
		}

		@Override
		protected Kafka08JsonTableSource.Builder builder() {
			return this;
		}

		/**
		 * Builds and configures a {@link Kafka08JsonTableSource}.
		 *
		 * @return A configured {@link Kafka08JsonTableSource}.
		 */
		@Override
		public Kafka08JsonTableSource build() {
			Kafka08JsonTableSource tableSource = new Kafka08JsonTableSource(
				getTopic(),
				getKafkaProps(),
				getTableSchema(),
				getJsonSchema());
			super.configureTableSource(tableSource);
			return tableSource;
		}
	}
}
