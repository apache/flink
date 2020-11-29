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

package org.apache.flink.api.common.serialization;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * The deserialization schema interface that deserialize a generic record
 * read from the source. Unlike the {@link DeserializationSchema}, this class
 * allows the record to be deserialized in a generic type instead of fixed
 * to byte[].
 *
 * @param <M> the type of the messages to be deserialized.
 * @param <T> the type of the deserialized messages.
 */
public interface GenericDeserializationSchema<M, T> {

	/**
	 * Initialization method for the schema. It is called before the actual working methods
	 * {@link #deserialize} and thus suitable for one time setup work.
	 *
	 * <p>The provided {@link DeserializationSchema.InitializationContext} can be used to access additional features such as e.g.
	 * registering user metrics.
	 *
	 * @param context Contextual information that can be used during initialization.
	 */
	@PublicEvolving
	default void open(DeserializationSchema.InitializationContext context) throws Exception {}

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
	void deserialize(M message, Collector<T> out) throws IOException;
}
