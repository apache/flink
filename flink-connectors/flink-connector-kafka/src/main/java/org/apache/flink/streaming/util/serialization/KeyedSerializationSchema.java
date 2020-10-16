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

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import java.io.Serializable;

/**
 * The serialization schema describes how to turn a data object into a different serialized
 * representation. Most data sinks (for example Apache Kafka) require the data to be handed
 * to them in a specific format (for example as byte strings).
 *
 * @param <T> The type to be serialized.
 *
 * @deprecated Use {@link KafkaSerializationSchema}.
 */
@Deprecated
@PublicEvolving
public interface KeyedSerializationSchema<T> extends Serializable {

	/**
	 * Serializes the key of the incoming element to a byte array
	 * This method might return null if no key is available.
	 *
	 * @param element The incoming element to be serialized
	 * @return the key of the element as a byte array
	 */
	byte[] serializeKey(T element);

	/**
	 * Serializes the value of the incoming element to a byte array.
	 *
	 * @param element The incoming element to be serialized
	 * @return the value of the element as a byte array
	 */
	byte[] serializeValue(T element);

	/**
	 * Optional method to determine the target topic for the element.
	 *
	 * @param element Incoming element to determine the target topic from
	 * @return null or the target topic
	 */
	String getTargetTopic(T element);
}
