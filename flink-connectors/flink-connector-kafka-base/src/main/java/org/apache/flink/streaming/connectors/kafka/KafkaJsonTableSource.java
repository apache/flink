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
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.sources.StreamTableSource;

import java.util.Map;
import java.util.Properties;

/**
 * A version-agnostic Kafka JSON {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #createKafkaConsumer(String, Properties, DeserializationSchema)}}.
 *
 * <p>The field names are used to parse the JSON file and so are the types.
 *
 * @deprecated Use the {@link org.apache.flink.table.descriptors.Kafka} descriptor together
 *             with descriptors for schema and format instead. Descriptors allow for
 *             implementation-agnostic definition of tables. See also
 *             {@link org.apache.flink.table.api.TableEnvironment#connect(ConnectorDescriptor)}.
 */
@Deprecated
@Internal
public abstract class KafkaJsonTableSource extends KafkaTableSource {

	/**
	 * Creates a generic Kafka JSON {@link StreamTableSource}.
	 *
	 * @param topic       Kafka topic to consume.
	 * @param properties  Properties for the Kafka consumer.
	 * @param tableSchema The schema of the table.
	 * @param jsonSchema  The schema of the JSON messages to decode from Kafka.
	 * @deprecated Use table descriptors instead of implementation-specific builders.
	 */
	@Deprecated
	protected KafkaJsonTableSource(
		String topic,
		Properties properties,
		TableSchema tableSchema,
		TableSchema jsonSchema) {

		super(
			tableSchema,
			topic,
			properties,
			new JsonRowDeserializationSchema(jsonSchema.toRowType()));
	}

	@Override
	public String explainSource() {
		return "KafkaJsonTableSource";
	}

	//////// SETTERS FOR OPTIONAL PARAMETERS

	/**
	 * Sets the flag that specifies the behavior in case of missing fields.
	 * TableSource will fail for missing fields if set to true. If set to false, the missing field is set to null.
	 *
	 * @param failOnMissingField Flag that specifies the TableSource behavior in case of missing fields.
	 * @deprecated Use table descriptors instead of implementation-specific builders.
	 */
	@Deprecated
	protected void setFailOnMissingField(boolean failOnMissingField) {
		((JsonRowDeserializationSchema) getDeserializationSchema()).setFailOnMissingField(failOnMissingField);
	}

	//////// HELPER METHODS

	/**
	 * Abstract builder for a {@link KafkaJsonTableSource} to be extended by builders of subclasses of
	 * KafkaJsonTableSource.
	 *
	 * @param <T> Type of the KafkaJsonTableSource produced by the builder.
	 * @param <B> Type of the KafkaJsonTableSource.Builder subclass.
	 * @deprecated Use the {@link org.apache.flink.table.descriptors.Kafka} descriptor together
	 *             with descriptors for schema and format instead. Descriptors allow for
	 *             implementation-agnostic definition of tables. See also
	 *             {@link org.apache.flink.table.api.TableEnvironment#connect(ConnectorDescriptor)}.
	 */
	@Deprecated
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
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
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
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
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
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		public B failOnMissingField(boolean failOnMissingField) {
			this.failOnMissingField = failOnMissingField;
			return builder();
		}

		/**
		 * Returns the configured JSON schema. If no JSON schema was configured, the table schema
		 * is returned.
		 *
		 * @return The JSON schema for the TableSource.
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		protected TableSchema getJsonSchema() {
			if (jsonSchema != null) {
				return this.jsonSchema;
			} else {
				return getTableSchema();
			}
		}

		/**
		 * Configures a TableSource with optional parameters.
		 *
		 * @param source The TableSource to configure.
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		protected void configureTableSource(T source) {
			super.configureTableSource(source);
			// configure field mapping
			source.setFieldMapping(this.fieldMapping);
			// configure missing field behavior
			source.setFailOnMissingField(this.failOnMissingField);
		}
	}
}
