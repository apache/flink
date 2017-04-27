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

import java.util.List;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.AvroTypeInfo;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.util.serialization.AvroRowDeserializationSchema;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

/**
 * A version-agnostic Kafka Avro {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #getKafkaConsumer(String, Properties, DeserializationSchema)}}.
 */
public abstract class KafkaAvroTableSource extends KafkaTableSource {

	/**
	 * Creates a generic Kafka Avro {@link StreamTableSource} using a given {@link SpecificRecord}.
	 *
	 * @param topic      Kafka topic to consume.
	 * @param properties Properties for the Kafka consumer.
	 * @param avroClass  Avro specific record.
	 */
	KafkaAvroTableSource(
		String topic,
		Properties properties,
		Class<? extends SpecificRecordBase> avroClass) {

		super(
			topic,
			properties,
			createDeserializationSchema(avroClass),
			convertToRowTypeInformation(avroClass));
	}

	private static AvroRowDeserializationSchema createDeserializationSchema(Class<? extends SpecificRecordBase> record) {
		return new AvroRowDeserializationSchema(record);
	}

	/**
	 * Converts the extracted AvroTypeInfo into a RowTypeInfo nested structure with deterministic field order.
	 * Replaces generic Utf8 with basic String type information.
	 */
	@SuppressWarnings("unchecked")
	private static <T extends SpecificRecordBase> TypeInformation<Row> convertToRowTypeInformation(Class<T> avroClass) {
		final AvroTypeInfo<T> avroTypeInfo = new AvroTypeInfo<>(avroClass);
		// determine schema to retrieve deterministic field order
		final Schema schema = SpecificData.get().getSchema(avroClass);
		return (TypeInformation<Row>) convertToTypeInformation(avroTypeInfo, schema);
	}

	/**
	 * Recursively converts extracted AvroTypeInfo into a RowTypeInfo nested structure with deterministic field order.
	 * Replaces generic Utf8 with basic String type information.
	 */
	private static TypeInformation<?> convertToTypeInformation(TypeInformation<?> extracted, Schema schema) {
		if (schema.getType() == Schema.Type.RECORD) {
			final List<Schema.Field> fields = schema.getFields();
			final AvroTypeInfo<?> avroTypeInfo = (AvroTypeInfo<?>) extracted;

			final TypeInformation<?>[] types = new TypeInformation<?>[fields.size()];
			final String[] names = new String[fields.size()];
			for (int i = 0; i < fields.size(); i++) {
				final Schema.Field field = fields.get(i);
				types[i] = convertToTypeInformation(avroTypeInfo.getTypeAt(field.name()), field.schema());
				names[i] = field.name();
			}
			return new RowTypeInfo(types, names);
		} else if (extracted instanceof GenericTypeInfo<?>) {
			final GenericTypeInfo<?> genericTypeInfo = (GenericTypeInfo<?>) extracted;
			if (genericTypeInfo.getTypeClass() == Utf8.class) {
				return BasicTypeInfo.STRING_TYPE_INFO;
			}
		}
		return extracted;
	}
}
