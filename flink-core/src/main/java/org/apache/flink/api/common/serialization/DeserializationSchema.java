/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.serialization;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;

/**
 * The deserialization schema describes how to turn the byte messages delivered by certain
 * data sources (for example Apache Kafka) into data types (Java/Scala objects) that are
 * processed by Flink.
 *
 * <p>In addition, the DeserializationSchema describes the produced type ({@link #getProducedType()}),
 * which lets Flink create internal serializers and structures to handle the type.
 *
 * <p><b>Note:</b> In most cases, one should start from {@link AbstractDeserializationSchema}, which
 * takes care of producing the return type information automatically.
 *
 * <p>A DeserializationSchema must be {@link Serializable} because its instances are often part of
 * an operator or transformation function.
 *
 * @param <T> The type created by the deserialization schema.
 */
@Public
public interface DeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {
	/**
	 * Initialization method for the schema. It is called before the actual working methods
	 * {@link #deserialize} and thus suitable for one time setup work.
	 *
	 * <p>The provided {@link InitializationContext} can be used to access additional features such as e.g.
	 * registering user metrics.
	 *
	 * @param context Contextual information that can be used during initialization.
	 */
	@PublicEvolving
	default void open(InitializationContext context) throws Exception {
	}

	/**
	 * Deserializes the byte message.
	 *
	 * @param message The message, as a byte array.
	 *
	 * @return The deserialized message as an object (null if the message cannot be deserialized).
	 */
	T deserialize(byte[] message) throws IOException;

	/**
	 * Deserializes the byte message.
	 *
	 * <p>Can output multiple records through the {@link Collector}. Note that number and size of the
	 * produced records should be relatively small. Depending on the source implementation records
	 * can be buffered in memory or collecting records might delay emitting checkpoint barrier.
	 *
	 * @param message The message, as a byte array.
	 * @param out The collector to put the resulting messages.
	 */
	@PublicEvolving
	default void deserialize(byte[] message, Collector<T> out) throws IOException {
		T deserialize = deserialize(message);
		if (deserialize != null) {
			out.collect(deserialize);
		}
	}

	/**
	 * Method to decide whether the element signals the end of the stream. If
	 * true is returned the element won't be emitted.
	 *
	 * @param nextElement The element to test for the end-of-stream signal.
	 * @return True, if the element signals end of stream, false otherwise.
	 */
	boolean isEndOfStream(T nextElement);

	/**
	 * A contextual information provided for {@link #open(InitializationContext)} method. It can be used to:
	 * <ul>
	 *     <li>Register user metrics via {@link InitializationContext#getMetricGroup()}</li>
	 * </ul>
	 */
	@PublicEvolving
	interface InitializationContext {
		/**
		 * Returns the metric group for the parallel subtask of the source that runs
		 * this {@link DeserializationSchema}.
		 *
		 * <p>Instances of this class can be used to register new metrics with Flink and to create a nested
		 * hierarchy based on the group names. See {@link MetricGroup} for more information for the metrics
		 * system.
		 *
		 * @see MetricGroup
		 */
		MetricGroup getMetricGroup();
	}
}
