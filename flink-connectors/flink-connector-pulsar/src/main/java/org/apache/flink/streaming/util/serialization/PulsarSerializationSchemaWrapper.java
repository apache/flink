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

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.pulsar.SerializableFunction;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.streaming.connectors.pulsar.internal.IncompatibleSchemaException;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaTranslator;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaUtils;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A wrapper that warp flink {@link SerializationSchema} to {@link PulsarSerializationSchema}.
 */
@PublicEvolving
public class PulsarSerializationSchemaWrapper<T> implements PulsarSerializationSchema<T>, PulsarContextAware<T> {
	private final SerializationSchema<T> serializationSchema;
	private final RecordSchemaType recordSchemaType;
	private final Schema<?> schema;
	private final Class<?> clazz;
	private final DataType dataType;
	private final SchemaMode schemaMode;
	private final SerializableFunction<T, String> topicExtractor;
	private final SerializableFunction<T, byte[]> keyExtractor;

	private PulsarSerializationSchemaWrapper(
		SerializationSchema<T> serializationSchema,
		RecordSchemaType recordSchemaType,
		Class<?> clazz,
		Schema<?> schema,
		DataType dataType,
		SchemaMode schemaMode,
		SerializableFunction<T, String> topicExtractor,
		SerializableFunction<T, byte[]> keyExtractor) {
		this.serializationSchema = serializationSchema;
		this.recordSchemaType = recordSchemaType;
		this.schema = schema;
		this.clazz = clazz;
		this.dataType = dataType;
		this.schemaMode = checkNotNull(schemaMode);
		this.topicExtractor = topicExtractor;
		this.keyExtractor = keyExtractor;
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return null;
	}

	@Override
	public Optional<String> getTargetTopic(T element) {
		return Optional.ofNullable(topicExtractor.apply(element));
	}

	@Override
	public void serialize(T element, TypedMessageBuilder<T> messageBuilder) {
		messageBuilder.value(element);
	}

	@Override
	public void open(SerializationSchema.InitializationContext context) throws Exception {
		this.serializationSchema.open(context);
	}

	@Override
	public Schema<T> getSchema() {
		try {
			switch (schemaMode) {
				case SPECIAL:
					checkNotNull(schema, "The schema cannot be null in SPECIAL mode");
					return new FlinkSchema<>(schema.getSchemaInfo(), serializationSchema, null);
				case ATOMIC:
					return new FlinkSchema<>(SchemaTranslator
						.atomicType2PulsarSchema(dataType)
						.getSchemaInfo(),
						serializationSchema, null);
				case POJO:
					return new FlinkSchema<>(
						SchemaUtils
							.buildSchemaForRecordClazz(clazz, recordSchemaType)
							.getSchemaInfo(),
						serializationSchema, null);
				case ROW:
					return new FlinkSchema<>(SchemaUtils.buildRowSchema(dataType, recordSchemaType),
						serializationSchema, null);
			}
		} catch (IncompatibleSchemaException e) {
			throw new IllegalStateException(e);
		}
		if (schema != null) {
			return new FlinkSchema<>(schema.getSchemaInfo(), serializationSchema, null);
		}
		try {
			if (dataType instanceof AtomicDataType) {
				return new FlinkSchema<>(SchemaTranslator
					.atomicType2PulsarSchema(dataType)
					.getSchemaInfo(),
					serializationSchema, null);
			} else {
				// for pojo type, use avro or json
				checkNotNull(clazz, "for non-atomic type, you must set clazz");
				checkNotNull(
					recordSchemaType,
					"for non-atomic type, you must set recordSchemaType");
				return new FlinkSchema<>(SchemaUtils.buildRowSchema(dataType, recordSchemaType),
					serializationSchema, null);
			}
		} catch (IncompatibleSchemaException e) {
			throw new RuntimeException(e);
		}
	}

	enum SchemaMode {
		ATOMIC,
		POJO,
		SPECIAL,
		ROW
	}

	/**
	 * Builder for {@link PulsarSerializationSchemaWrapper}.
	 */
	@PublicEvolving
	public static class Builder<T> {
		private final SerializationSchema<T> serializationSchema;
		private RecordSchemaType recordSchemaType;
		private Schema<?> schema;
		private Class<?> clazz;
		private DataType dataType;
		private SchemaMode mode;
		private SerializableFunction<T, String> topicExtractor = (T) -> null;
		private SerializableFunction<T, byte[]> keyExtractor;

		public Builder(SerializationSchema<T> serializationSchema) {
			this.serializationSchema = serializationSchema;
		}

		public PulsarSerializationSchemaWrapper.Builder<T> useSpecialMode(Schema<?> schema) {
			checkArgument(mode == null, "you can only set one schemaMode");
			this.mode = SchemaMode.SPECIAL;
			this.schema = schema;
			return this;
		}

		public PulsarSerializationSchemaWrapper.Builder<T> useAtomicMode(DataType dataType) {
			checkArgument(mode == null, "you can only set one schemaMode");
			this.mode = SchemaMode.ATOMIC;
			checkArgument(dataType instanceof AtomicDataType, "you must set an atomic dataType");
			this.dataType = dataType;
			return this;
		}

		public PulsarSerializationSchemaWrapper.Builder<T> usePojoMode(
			Class<?> clazz,
			RecordSchemaType recordSchemaType) {
			checkArgument(mode == null, "you can only set one schemaMode");
			this.mode = SchemaMode.POJO;
			checkArgument(
				recordSchemaType != RecordSchemaType.ATOMIC,
				"cant ues RecordSchemaType.ATOMIC to build pojo type schema");
			this.clazz = clazz;
			this.recordSchemaType = recordSchemaType;
			return this;
		}

		public PulsarSerializationSchemaWrapper.Builder<T> useRowMode(
			DataType dataType,
			RecordSchemaType recordSchemaType) {
			checkArgument(mode == null, "you can only set one schemaMode");
			this.mode = SchemaMode.ROW;
			this.dataType = dataType;
			this.recordSchemaType = recordSchemaType;
			return this;
		}

		public PulsarSerializationSchemaWrapper.Builder<T> setTopicExtractor(
			SerializableFunction<T, String> topicExtractor) {
			this.topicExtractor = topicExtractor;
			return this;
		}

		public PulsarSerializationSchemaWrapper.Builder<T> setKeyExtractor(
			SerializableFunction<T, byte[]> keyExtractor) {
			this.keyExtractor = keyExtractor;
			return this;
		}

		public PulsarSerializationSchemaWrapper<T> build() {
			return new PulsarSerializationSchemaWrapper<>(
				serializationSchema,
				recordSchemaType,
				clazz,
				schema,
				dataType,
				mode,
				topicExtractor,
				keyExtractor);
		}
	}
}
