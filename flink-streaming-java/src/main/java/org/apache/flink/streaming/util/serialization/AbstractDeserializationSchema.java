/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

/**
 * The deserialization schema describes how to turn the byte messages delivered by certain
 * data sources (for example Apache Kafka) into data types (Java/Scala objects) that are
 * processed by Flink.
 * 
 * <p>This base variant of the deserialization schema produces the type information
 * automatically by extracting it from the generic class arguments.
 * 
 * @param <T> The type created by the deserialization schema.
 */
public abstract class AbstractDeserializationSchema<T> implements DeserializationSchema<T> {

	private static final long serialVersionUID = 1L;

	/**
	 * De-serializes the byte message.
	 *
	 * @param message The message, as a byte array.
	 * @return The de-serialized message as an object.
	 */
	@Override
	public abstract T deserialize(byte[] message) throws IOException;

	/**
	 * Method to decide whether the element signals the end of the stream. If
	 * true is returned the element won't be emitted.
	 * 
	 * <p>This default implementation returns always false, meaning the stream is interpreted
	 * to be unbounded.
	 *
	 * @param nextElement The element to test for the end-of-stream signal.
	 * @return True, if the element signals end of stream, false otherwise.
	 */
	@Override
	public boolean isEndOfStream(T nextElement) {
		return false;
	}
	
	@Override
	public TypeInformation<T> getProducedType() {
		return TypeExtractor.createTypeInfo(AbstractDeserializationSchema.class, getClass(), 0, null, null);
	}
}
