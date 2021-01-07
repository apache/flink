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
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.schema.BooleanSchema;
import org.apache.pulsar.client.impl.schema.ByteSchema;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.client.impl.schema.DateSchema;
import org.apache.pulsar.client.impl.schema.DoubleSchema;
import org.apache.pulsar.client.impl.schema.FloatSchema;
import org.apache.pulsar.client.impl.schema.InstantSchema;
import org.apache.pulsar.client.impl.schema.IntSchema;
import org.apache.pulsar.client.impl.schema.LocalDateSchema;
import org.apache.pulsar.client.impl.schema.LocalDateTimeSchema;
import org.apache.pulsar.client.impl.schema.LocalTimeSchema;
import org.apache.pulsar.client.impl.schema.LongSchema;
import org.apache.pulsar.client.impl.schema.ShortSchema;
import org.apache.pulsar.client.impl.schema.TimeSchema;
import org.apache.pulsar.client.impl.schema.TimestampSchema;

import java.io.IOException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkArgument;

/**
 * pulsar primitive deserialization.
 */
@PublicEvolving
public class PulsarPrimitiveSchema<T> implements PulsarSerializationSchema<T>,
	PulsarDeserializationSchema<T>, PulsarContextAware<T> {

	private static final Map<Class<?>, Schema<?>> pulsarPrimitives = new HashMap<>();

	static {
		pulsarPrimitives.put(Boolean.class, BooleanSchema.of());
		pulsarPrimitives.put(Boolean.TYPE, BooleanSchema.of());
		pulsarPrimitives.put(Byte.class, ByteSchema.of());
		pulsarPrimitives.put(Byte.TYPE, ByteSchema.of());
		pulsarPrimitives.put(Short.class, ShortSchema.of());
		pulsarPrimitives.put(Short.TYPE, ShortSchema.of());
		pulsarPrimitives.put(Integer.class, IntSchema.of());
		pulsarPrimitives.put(Integer.TYPE, IntSchema.of());
		pulsarPrimitives.put(Long.class, LongSchema.of());
		pulsarPrimitives.put(Long.TYPE, LongSchema.of());
		pulsarPrimitives.put(String.class, org.apache.pulsar.client.api.Schema.STRING);
		pulsarPrimitives.put(Float.class, FloatSchema.of());
		pulsarPrimitives.put(Float.TYPE, FloatSchema.of());
		pulsarPrimitives.put(Double.class, DoubleSchema.of());
		pulsarPrimitives.put(Double.TYPE, DoubleSchema.of());
		pulsarPrimitives.put(Byte[].class, BytesSchema.of());
		pulsarPrimitives.put(Date.class, DateSchema.of());
		pulsarPrimitives.put(Time.class, TimeSchema.of());
		pulsarPrimitives.put(Timestamp.class, TimestampSchema.of());
		pulsarPrimitives.put(LocalDate.class, LocalDateSchema.of());
		pulsarPrimitives.put(LocalTime.class, LocalTimeSchema.of());
		pulsarPrimitives.put(LocalDateTime.class, LocalDateTimeSchema.of());
		pulsarPrimitives.put(Instant.class, InstantSchema.of());
	}

	private final Class<T> recordClazz;

	@SuppressWarnings("unchecked")
	public PulsarPrimitiveSchema(Class<T> recordClazz) {
		checkArgument(
			pulsarPrimitives.containsKey(recordClazz),
			"Must be of Pulsar primitive types");
		this.recordClazz = recordClazz;
	}

	public static boolean isPulsarPrimitive(Class<?> key) {
		return pulsarPrimitives.containsKey(key);
	}

	@Override
	public void serialize(T element, TypedMessageBuilder<T> messageBuilder) {
		messageBuilder.value(element);
	}

	@Override
	public T deserialize(Message<T> message) throws IOException {
		return message.getValue();
	}

	@Override
	public Optional<String> getTargetTopic(T element) {
		return Optional.empty();
	}

	@Override
	@SuppressWarnings("unchecked")
	public Schema<T> getSchema() {
		return (Schema<T>) pulsarPrimitives.get(recordClazz);
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return TypeInformation.of(recordClazz);
	}

	@Override
	public boolean isEndOfStream(T nextElement) {
		return false;
	}
}
