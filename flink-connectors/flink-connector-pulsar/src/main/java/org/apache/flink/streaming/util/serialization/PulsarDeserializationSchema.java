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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.io.Serializable;

/**
 * The deserialization schema describes how to turn the pulsar messages
 * into data types (Java/Scala objects) that are processed by Flink.
 *
 * @param <T> The type created by the keyed deserialization schema.
 */
@PublicEvolving
public interface PulsarDeserializationSchema<T> extends PulsarContextAware<T>, Serializable {

	static <V> PulsarDeserializationSchemaBuilder<V> builder() {
		return new PulsarDeserializationSchemaBuilder<>();
	}

	;

	/**
	 * Wraps a Flink {@link DeserializationSchema} to a {@link PulsarDeserializationSchema}.
	 *
	 * @param valueDeserializer the deserializer class used to deserialize the value.
	 * @param <V> the value type.
	 *
	 * @return A {@link PulsarDeserializationSchema} that deserialize the value with the given deserializer.
	 */
	static <V> PulsarDeserializationSchema<V> valueOnly(DeserializationSchema<V> valueDeserializer) {
		return new PulsarDeserializationSchema<V>() {
			@Override
			public Schema<V> getSchema() {
				return new FlinkSchema<>(Schema.BYTES.getSchemaInfo(), null, valueDeserializer);
			}

			@Override
			public V deserialize(Message<V> message) throws IOException {
				return valueDeserializer.deserialize(message.getData());
			}

			@Override
			public void deserialize(Message<V> message, Collector<V> collector) throws IOException {
				valueDeserializer.deserialize(message.getData(), collector);
			}

			@Override
			public boolean isEndOfStream(V nextElement) {
				return valueDeserializer.isEndOfStream(nextElement);
			}

			@Override
			public TypeInformation<V> getProducedType() {
				return valueDeserializer.getProducedType();
			}
		};
	}

	default void open(DeserializationSchema.InitializationContext context) throws Exception {
	}

	;

	/**
	 * Method to decide whether the element signals the end of the stream. If
	 * true is returned the element won't be emitted.
	 *
	 * @param nextElement The element to test for the end-of-stream signal.
	 *
	 * @return True, if the element signals end of stream, false otherwise.
	 */
	boolean isEndOfStream(T nextElement);

	;

	/**
	 * Deserializes the Pulsar message.
	 *
	 * <p>Can output multiple records through the {@link Collector}. Note that number and size of the
	 * produced records should be relatively small. Depending on the source implementation records
	 * can be buffered in memory or collecting records might delay emitting checkpoint barrier.
	 *
	 * @param message The message, as a byte array.
	 */
	T deserialize(Message<T> message) throws IOException;

	/**
	 * Deserializes the Pulsar message.
	 *
	 * <p>Can output multiple records through the {@link Collector}. Note that number and size of the
	 * produced records should be relatively small. Depending on the source implementation records
	 * can be buffered in memory or collecting records might delay emitting checkpoint barrier.
	 *
	 * @param message The message, as a byte array.
	 * @param out The collector to put the resulting messages.
	 */
	default void deserialize(Message<T> message, Collector<T> out) throws IOException {
		out.collect(deserialize(message));
	}
}
