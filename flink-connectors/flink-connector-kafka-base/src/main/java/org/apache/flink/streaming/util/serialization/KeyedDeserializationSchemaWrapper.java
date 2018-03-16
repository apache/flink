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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * A simple wrapper for using the DeserializationSchema with the KeyedDeserializationSchema
 * interface.
 * @param <T> The type created by the deserialization schema.
 */
@Internal
public class KeyedDeserializationSchemaWrapper<T> implements KeyedDeserializationSchema<T> {

	private static final long serialVersionUID = 2651665280744549932L;

	private final DeserializationSchema<T> deserializationSchema;

	public KeyedDeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public T deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
		return deserializationSchema.deserialize(message);
	}

	@Override
	public boolean isEndOfStream(T nextElement) {
		return deserializationSchema.isEndOfStream(nextElement);
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return deserializationSchema.getProducedType();
	}
}
