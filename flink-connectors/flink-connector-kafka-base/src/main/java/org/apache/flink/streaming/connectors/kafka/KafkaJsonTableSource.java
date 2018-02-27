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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.StreamTableSource;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * A version-agnostic Kafka JSON {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #createKafkaConsumer(String, Properties, DeserializationSchema)}}.
 *
 * <p>The field names are used to parse the JSON file and so are the types.
 */
@Internal
public abstract class KafkaJsonTableSource extends KafkaTableSource implements DefinedFieldMapping {

	private TableSchema jsonSchema;

	private Map<String, String> fieldMapping;

	private boolean failOnMissingField;

	/**
	 * Creates a generic Kafka JSON {@link StreamTableSource}.
	 *
	 * @param topic       Kafka topic to consume.
	 * @param properties  Properties for the Kafka consumer.
	 * @param tableSchema The schema of the table.
	 * @param jsonSchema  The schema of the JSON messages to decode from Kafka.
	 */
	protected KafkaJsonTableSource(
		String topic,
		Properties properties,
		TableSchema tableSchema,
		TableSchema jsonSchema) {

		super(
			topic,
			properties,
			tableSchema,
			jsonSchemaToReturnType(jsonSchema));

		this.jsonSchema = jsonSchema;
	}

	@Override
	public Map<String, String> getFieldMapping() {
		return fieldMapping;
	}

	@Override
	protected JsonRowDeserializationSchema getDeserializationSchema() {
		JsonRowDeserializationSchema deserSchema = new JsonRowDeserializationSchema(jsonSchemaToReturnType(jsonSchema));
		deserSchema.setFailOnMissingField(failOnMissingField);
		return deserSchema;
	}

	@Override
	public String explainSource() {
		return "KafkaJsonTableSource";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof KafkaJsonTableSource)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		KafkaJsonTableSource that = (KafkaJsonTableSource) o;
		return failOnMissingField == that.failOnMissingField &&
			Objects.equals(jsonSchema, that.jsonSchema) &&
			Objects.equals(fieldMapping, that.fieldMapping);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), jsonSchema, fieldMapping, failOnMissingField);
	}

	//////// SETTERS FOR OPTIONAL PARAMETERS

	/**
	 * Sets the flag that specifies the behavior in case of missing fields.
	 * TableSource will fail for missing fields if set to true. If set to false, the missing field is set to null.
	 *
	 * @param failOnMissingField Flag that specifies the TableSource behavior in case of missing fields.
	 */
	protected void setFailOnMissingField(boolean failOnMissingField) {
		this.failOnMissingField = failOnMissingField;
	}

	/**
	 * Sets the mapping from table schema fields to JSON schema fields.
	 *
	 * @param fieldMapping The mapping from table schema fields to JSON schema fields.
	 */
	protected void setFieldMapping(Map<String, String> fieldMapping) {
		this.fieldMapping = fieldMapping;
	}

	//////// HELPER METHODS

	/** Converts the JSON schema into into the return type. */
	private static RowTypeInfo jsonSchemaToReturnType(TableSchema jsonSchema) {
		return new RowTypeInfo(jsonSchema.getTypes(), jsonSchema.getColumnNames());
	}

	/**
	 * Abstract builder for a {@link KafkaJsonTableSource} to be extended by builders of subclasses of
	 * KafkaJsonTableSource.
	 *
	 * @param <T> Type of the KafkaJsonTableSource produced by the builder.
	 * @param <B> Type of the KafkaJsonTableSource.Builder subclass.
	 */
	protected abstract static class Builder<T extends KafkaJsonTableSource, B extends KafkaJsonTableSource.Builder>
		extends KafkaTableSource.Builder<T, B> {

		private TableSchema jsonSchema;

		private Map<String, String> fieldMapping;

		private boolean failOnMissingField = false;

		/**
		 * Sets the schema of the JSON-encoded Kafka messages.
		 * If not set, the JSON messages are decoded with the table schema.
		 *
		 * @param jsonSchema The schema of the JSON-encoded Kafka messages.
		 * @return The builder.
		 */
		public B forJsonSchema(TableSchema jsonSchema) {
			this.jsonSchema = jsonSchema;
			return builder();
		}

		/**
		 * Sets a mapping from schema fields to fields of the JSON schema.
		 *
		 * <p>A field mapping is required if the fields of produced tables should be named different than
		 * the fields of the JSON records.
		 * The key of the provided Map refers to the field of the table schema,
		 * the value to the field in the JSON schema.</p>
		 *
		 * @param tableToJsonMapping A mapping from table schema fields to JSON schema fields.
		 * @return The builder.
		 */
		public B withTableToJsonMapping(Map<String, String> tableToJsonMapping) {
			this.fieldMapping = tableToJsonMapping;
			return builder();
		}

		/**
		 * Sets flag whether to fail if a field is missing or not.
		 *
		 * @param failOnMissingField If set to true, the TableSource fails if there is a missing
		 *                           field.
		 *                           If set to false, a missing field is set to null.
		 * @return The builder.
		 */
		public B failOnMissingField(boolean failOnMissingField) {
			this.failOnMissingField = failOnMissingField;
			return builder();
		}

		/**
		 * Returns the configured JSON schema. If no JSON schema was configured, the table schema
		 * is returned.
		 *
		 * @return The JSON schema for the TableSource.
		 */
		protected TableSchema getJsonSchema() {
			if (jsonSchema != null) {
				return this.jsonSchema;
			} else {
				return getTableSchema();
			}
		}

		@Override
		protected void configureTableSource(T source) {
			super.configureTableSource(source);
			// configure field mapping
			source.setFieldMapping(this.fieldMapping);
			// configure missing field behavior
			source.setFailOnMissingField(this.failOnMissingField);
		}
	}
}
