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
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.JsonRowDeserializationSchema;

import java.util.Properties;
import org.apache.flink.types.Row;

/**
 * A version-agnostic Kafka JSON {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #getKafkaConsumer(String, Properties, DeserializationSchema)}}.
 *
 * <p>The field names are used to parse the JSON file and so are the types.
 */
public abstract class KafkaJsonTableSource extends KafkaTableSource {

	/**
	 * Creates a generic Kafka JSON {@link StreamTableSource}.
	 *
	 * @param topic      Kafka topic to consume.
	 * @param properties Properties for the Kafka consumer.
	 * @param typeInfo   Type information describing the result type. The field names are used
	 *                   to parse the JSON file and so are the types.
	 */
	KafkaJsonTableSource(
			String topic,
			Properties properties,
			TypeInformation<Row> typeInfo) {

		super(topic, properties, createDeserializationSchema(typeInfo), typeInfo);
	}

	/**
	 * Configures the failure behaviour if a JSON field is missing.
	 *
	 * <p>By default, a missing field is ignored and the field is set to null.
	 *
	 * @param failOnMissingField Flag indicating whether to fail or not on a missing field.
	 */
	public void setFailOnMissingField(boolean failOnMissingField) {
		JsonRowDeserializationSchema deserializationSchema = (JsonRowDeserializationSchema) getDeserializationSchema();
		deserializationSchema.setFailOnMissingField(failOnMissingField);
	}

	private static JsonRowDeserializationSchema createDeserializationSchema(TypeInformation<Row> typeInfo) {

		return new JsonRowDeserializationSchema(typeInfo);
	}
}
