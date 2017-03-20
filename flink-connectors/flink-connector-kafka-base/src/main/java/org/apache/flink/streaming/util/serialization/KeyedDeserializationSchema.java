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

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.IOException;
import java.io.Serializable;

/**
 * The deserialization schema describes how to turn the byte key / value messages delivered by certain
 * data sources (for example Apache Kafka) into data types (Java/Scala objects) that are
 * processed by Flink.
 * 
 * @param <T> The type created by the keyed deserialization schema.
 */
public interface KeyedDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

	/**
	 * Deserializes the byte message.
	 *
	 * @param messageKey the key as a byte array (null if no key has been set).
	 * @param message The message, as a byte array (null if the message was empty or deleted).
	 * @param partition The partition the message has originated from.
	 * @param offset the offset of the message in the original source (for example the Kafka offset).
	 *
	 * @return The deserialized message as an object (null if the message cannot be deserialized).
	 */
	T deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException;

	/**
	 * Method to decide whether the element signals the end of the stream. If
	 * true is returned the element won't be emitted.
	 * 
	 * @param nextElement The element to test for the end-of-stream signal.
	 *
	 * @return True, if the element signals end of stream, false otherwise.
	 */
	boolean isEndOfStream(T nextElement);
}
